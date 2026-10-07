import asyncio
import copy
import json
import os
import time
from collections import OrderedDict, defaultdict, deque
from datetime import datetime, timezone
from typing import Optional, Set

from fastapi import FastAPI, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from config import BASE_DIR, CHANNEL_SLUG, DB_PATH
from db import (
    connect,
    get_active_stream,
    get_stream_summary_cache,
    init_db,
    set_stream_summary_cache,
)
from recorder import KickRecorder
from stream_manager import get_channel_meta

app = FastAPI(title='Kick Live Analytics', version='3.0.0')
app.add_middleware(GZipMiddleware, minimum_size=1200)

connected_clients: Set[WebSocket] = set()
recent_messages: deque[dict] = deque(maxlen=250)
recorder: KickRecorder | None = None
recorder_task: asyncio.Task | None = None
broadcast_task: asyncio.Task | None = None

# Cache settings are intentionally conservative: live data can be up to ~30s old,
# while finished broadcasts are effectively immutable and may stay cached for 10 min.
LIVE_CACHE_TTL = int(os.getenv('LIVE_CACHE_TTL', '30'))
HISTORICAL_CACHE_TTL = int(os.getenv('HISTORICAL_CACHE_TTL', '600'))
STREAMS_CACHE_TTL = int(os.getenv('STREAMS_CACHE_TTL', '30'))
CACHE_MAX_ENTRIES = int(os.getenv('CACHE_MAX_ENTRIES', '32'))
MAX_WS_CLIENTS = int(os.getenv('MAX_WS_CLIENTS', '10000'))
BROADCAST_FLUSH_MS = int(os.getenv('BROADCAST_FLUSH_MS', '200'))
MAX_BROADCAST_QUEUE = int(os.getenv('MAX_BROADCAST_QUEUE', '5000'))

cache_store: OrderedDict[str, tuple[float, object]] = OrderedDict()
cache_lock = asyncio.Lock()
inflight_tasks: dict[str, asyncio.Task] = {}

# Lightweight in-process rate limiter. When a reverse proxy/CDN is in front of the app,
# use that layer for distributed limiting as well; this layer is the last line of defence.
rate_buckets: dict[tuple[str, str], deque[float]] = defaultdict(deque)
rate_lock = asyncio.Lock()
RATE_LIMITS = {
    'data': (30, 60),
    'detail': (90, 60),
    'default': (180, 60),
}

broadcast_queue: deque[dict] = deque()
broadcast_event = asyncio.Event()


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def row_dict(row):
    return dict(row) if row else None


def build_stream_label(row):
    if not row:
        return 'Yayın'
    return f"{row['label_date']} offstream" if row['session_type'] == 'offstream' else f"{row['label_date']} yayını"


def _cache_get(key: str, max_age: int):
    item = cache_store.get(key)
    if not item:
        return None
    created_at, value = item
    if time.monotonic() - created_at > max_age:
        cache_store.pop(key, None)
        return None
    cache_store.move_to_end(key)
    return copy.deepcopy(value)


def _cache_set(key: str, value):
    cache_store[key] = (time.monotonic(), copy.deepcopy(value))
    cache_store.move_to_end(key)
    while len(cache_store) > CACHE_MAX_ENTRIES:
        cache_store.popitem(last=False)


def invalidate_live_cache():
    for key in list(cache_store):
        if key.startswith('data:live') or key == 'active_stream' or key.startswith('streams:'):
            cache_store.pop(key, None)


async def cached_value(key: str, factory, ttl: int):
    async with cache_lock:
        hit = _cache_get(key, ttl)
        if hit is not None:
            return hit
        task = inflight_tasks.get(key)
        if task is None:
            task = asyncio.create_task(factory())
            inflight_tasks[key] = task

    try:
        value = await task
        async with cache_lock:
            _cache_set(key, value)
        return copy.deepcopy(value)
    finally:
        async with cache_lock:
            current = inflight_tasks.get(key)
            if current is task:
                inflight_tasks.pop(key, None)


def _client_ip_from_request(request: Request) -> str:
    if os.getenv('TRUST_PROXY', '0') == '1':
        forwarded = request.headers.get('x-forwarded-for')
        if forwarded:
            return forwarded.split(',')[0].strip()
    return request.client.host if request.client else 'unknown'


def _client_ip_from_ws(ws: WebSocket) -> str:
    if os.getenv('TRUST_PROXY', '0') == '1':
        forwarded = ws.headers.get('x-forwarded-for')
        if forwarded:
            return forwarded.split(',')[0].strip()
    return ws.client.host if ws.client else 'unknown'


def _rate_bucket(path: str) -> tuple[str, int, int]:
    if path.startswith('/api/data'):
        limit, window = RATE_LIMITS['data']
        return 'data', limit, window
    if any(part in path for part in ('/user_detail', '/user_messages', '/word_detail', '/emotes')):
        limit, window = RATE_LIMITS['detail']
        return 'detail', limit, window
    limit, window = RATE_LIMITS['default']
    return 'default', limit, window


@app.middleware('http')
async def protection_and_headers(request: Request, call_next):
    path = request.url.path
    if path.startswith('/api/'):
        bucket, limit, window = _rate_bucket(path)
        ip = _client_ip_from_request(request)
        now = time.monotonic()
        key = (ip, bucket)
        async with rate_lock:
            q = rate_buckets[key]
            while q and now - q[0] > window:
                q.popleft()
            if len(q) >= limit:
                return JSONResponse(
                    {'ok': False, 'error': 'rate_limited', 'retry_after': max(1, int(window - (now - q[0])))},
                    status_code=429,
                    headers={'Retry-After': str(max(1, int(window - (now - q[0]))))},
                )
            q.append(now)

    response = await call_next(request)
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
    response.headers['X-Frame-Options'] = 'SAMEORIGIN'
    response.headers['Permissions-Policy'] = 'camera=(), microphone=(), geolocation=()'

    if path.startswith('/api/data'):
        response.headers['Cache-Control'] = 'public, max-age=15, stale-while-revalidate=45'
    elif path.startswith('/api/streams'):
        response.headers['Cache-Control'] = 'public, max-age=15, stale-while-revalidate=45'
    elif path.startswith('/api/'):
        response.headers['Cache-Control'] = 'private, max-age=5'
    elif path.startswith('/assets/'):
        response.headers['Cache-Control'] = 'public, max-age=3600, immutable'
    return response


def push_recent(payload: dict):
    if payload.get('type') == 'chat':
        recent_messages.append(payload)


async def broadcast(payload: dict):
    push_recent(payload)
    if not connected_clients:
        return
    if len(broadcast_queue) >= MAX_BROADCAST_QUEUE:
        # Drop the oldest low-value chat payload first. Moderation/system events are rare,
        # so retaining them is more useful under extreme pressure.
        drop_index = None
        for i, item in enumerate(broadcast_queue):
            if item.get('type') == 'chat':
                drop_index = i
                break
        if drop_index is None:
            broadcast_queue.popleft()
        else:
            del broadcast_queue[drop_index]
    broadcast_queue.append(payload)
    broadcast_event.set()


async def broadcast_loop(stop_event: asyncio.Event):
    global broadcast_task
    interval = max(0.05, BROADCAST_FLUSH_MS / 1000)
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(broadcast_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass
        broadcast_event.clear()
        if not broadcast_queue or not connected_clients:
            continue
        batch = []
        while broadcast_queue and len(batch) < 300:
            batch.append(broadcast_queue.popleft())
        payload = batch[0] if len(batch) == 1 else {'type': 'batch', 'items': batch, 'time': now_iso()}
        text = json.dumps(payload, ensure_ascii=False, separators=(',', ':'))
        sockets = list(connected_clients)

        async def send_one(ws):
            try:
                await asyncio.wait_for(ws.send_text(text), timeout=1.0)
                return None
            except Exception:
                return ws

        results = await asyncio.gather(*(send_one(ws) for ws in sockets), return_exceptions=True)
        for result in results:
            if result is not None and result is not True:
                connected_clients.discard(result)


def _event_payload(entry):
    if entry.get('type') == 'chat':
        return {
            'type': 'chat',
            'event': entry.get('e'),
            'user': entry.get('user'),
            'user_id': entry.get('user_id'),
            'msg': entry.get('msg'),
            'message_id': entry.get('message_id'),
            'time': entry.get('t'),
            'stream_id': entry.get('stream_id'),
        }
    return {
        'type': 'other',
        'event': entry.get('e'),
        'user': entry.get('user'),
        'data': entry.get('raw', {}),
        'time': entry.get('t'),
        'stream_id': entry.get('stream_id'),
    }


async def recorder_event(entry):
    # Do not invalidate the expensive live snapshot on every chat event. The
    # snapshot intentionally lives for LIVE_CACHE_TTL seconds and the lightweight
    # /api/live/stats endpoint handles fast counters. This is a key anti-stampede
    # optimisation under high traffic.
    await broadcast(_event_payload(entry))


def stream_ids_for_mode(conn, mode, date=None, month=None, stream_id=None):
    if mode == 'live':
        row = conn.execute("SELECT * FROM streams WHERE status='live' AND session_type='stream' ORDER BY id DESC LIMIT 1").fetchone()
        return [row['id']] if row else [], row
    if mode == 'stream':
        row = conn.execute("SELECT * FROM streams WHERE id=? AND session_type='stream'", (stream_id,)).fetchone() if stream_id else None
        return [row['id']] if row else [], row
    if mode == 'day':
        rows = conn.execute("SELECT * FROM streams WHERE label_date=? AND session_type='stream' ORDER BY id DESC", (date,)).fetchall()
        return [r['id'] for r in rows], rows[0] if rows else None
    if mode == 'offstream_day':
        rows = conn.execute("SELECT * FROM streams WHERE label_date=? AND session_type='offstream' ORDER BY id DESC", (date,)).fetchall()
        return [r['id'] for r in rows], rows[0] if rows else None
    if mode == 'week':
        rows = conn.execute("SELECT * FROM streams WHERE label_date >= date('now','-7 day') ORDER BY id DESC").fetchall()
        return [r['id'] for r in rows], rows[0] if rows else None
    if mode == 'month':
        rows = conn.execute("SELECT * FROM streams WHERE substr(label_date,1,7)=? ORDER BY id DESC", (month,)).fetchall()
        return [r['id'] for r in rows], rows[0] if rows else None
    rows = conn.execute("SELECT * FROM streams ORDER BY id DESC").fetchall()
    return [r['id'] for r in rows], rows[0] if rows else None


def _empty_summary():
    empty_stats = {
        'total_messages': 0, 'total_msgs': 0, 'unique_users': 0, 'total_users': 0,
        'deleted_messages': 0, 'timeouts': 0, 'bans': 0, 'unbans': 0,
        'subscriptions': 0, 'gift_subscriptions': 0, 'other_events': 0,
        'peak_chat_per_minute': 0, 'peak_chat_per_second': 0,
    }
    return {
        'stats': empty_stats, 'users': [], 'words': [], 'emotes': [], 'spam': [],
        'moderation': {'summary': {}, 'mods': [], 'recent_actions': []},
        'events': [],
        'game_special': {'all_pool': [], 'top_10_users': [], 'top_10_words': [], 'top_10_emotes': []},
    }


def summary_for_stream_ids(conn, ids: list[int], event_limit=300):
    """Set-based summary builder.

    The old implementation executed multiple SQL queries per user and per emote.
    This version keeps the response shape but uses grouped joins/window functions
    so one dashboard load no longer explodes into hundreds/thousands of queries.
    """
    if not ids:
        return _empty_summary()

    ph = ','.join('?' * len(ids))
    stats = dict(conn.execute(f"""SELECT COALESCE(SUM(total_messages),0) total_messages,
        COALESCE(SUM(deleted_messages),0) deleted_messages, COALESCE(SUM(timeouts),0) timeouts,
        COALESCE(SUM(bans),0) bans, COALESCE(SUM(unbans),0) unbans,
        COALESCE(SUM(subscriptions),0) subscriptions, COALESCE(SUM(gift_subscriptions),0) gift_subscriptions,
        COALESCE(SUM(other_events),0) other_events, COALESCE(MAX(peak_chat_per_minute),0) peak_chat_per_minute,
        COALESCE(MAX(peak_chat_per_second),0) peak_chat_per_second
        FROM stream_stats WHERE stream_id IN ({ph})""", ids).fetchone())

    unique = conn.execute(f'SELECT COUNT(DISTINCT user_id) c FROM user_stream_stats WHERE stream_id IN ({ph})', ids).fetchone()['c']
    stats.update({'unique_users': unique or 0, 'total_users': unique or 0, 'total_msgs': stats.get('total_messages', 0)})

    users_rows = conn.execute(f"""
        WITH wc AS (
            SELECT user_id, SUM(count) c FROM user_word_stats
            WHERE stream_id IN ({ph}) GROUP BY user_id
        ),
        ec AS (
            SELECT user_id, SUM(count) c FROM user_emote_stats
            WHERE stream_id IN ({ph}) GROUP BY user_id
        ),
        rec AS (
            SELECT LOWER(COALESCE(target_username,'')) target_name,
                SUM(CASE WHEN event_type='timeout' THEN 1 ELSE 0 END) timeouts,
                SUM(CASE WHEN event_type='ban' THEN 1 ELSE 0 END) bans,
                SUM(CASE WHEN event_type='unban' THEN 1 ELSE 0 END) unbans,
                SUM(CASE WHEN event_type='deleted' THEN 1 ELSE 0 END) deleted_messages
            FROM events WHERE stream_id IN ({ph}) GROUP BY LOWER(COALESCE(target_username,''))
        )
        SELECT us.user_id id, MAX(us.username) n, SUM(us.messages) mc,
            MIN(us.first_message_at) first_message_at, MAX(us.last_message_at) last_message_at,
            COALESCE(wc.c,0) wc, COALESCE(ec.c,0) ec,
            COALESCE(rec.timeouts,0) timeouts, COALESCE(rec.bans,0) bans,
            COALESCE(rec.unbans,0) unbans, COALESCE(rec.deleted_messages,0) deleted_messages
        FROM user_stream_stats us
        LEFT JOIN wc ON wc.user_id=us.user_id
        LEFT JOIN ec ON ec.user_id=us.user_id
        LEFT JOIN rec ON rec.target_name=LOWER(us.username)
        WHERE us.stream_id IN ({ph})
        GROUP BY us.user_id
        ORDER BY mc DESC
        LIMIT 500
    """, ids + ids + ids + ids).fetchall()
    users = [{
        'n': r['n'], 'mc': r['mc'] or 0, 'id': r['id'],
        'first_message_at': r['first_message_at'], 'last_message_at': r['last_message_at'],
        'wc': r['wc'] or 0, 'ec': r['ec'] or 0,
        'mod_received': {
            'timeouts': r['timeouts'] or 0, 'bans': r['bans'] or 0,
            'unbans': r['unbans'] or 0, 'deleted_messages': r['deleted_messages'] or 0,
        },
    } for r in users_rows]

    word_rows = conn.execute(f"""SELECT word w, SUM(count) c FROM word_stats
        WHERE stream_id IN ({ph}) AND word NOT LIKE 'emote:%' COLLATE NOCASE
        AND word NOT LIKE '[emote:%' COLLATE NOCASE
        GROUP BY word ORDER BY c DESC LIMIT 2500""", ids).fetchall()
    words = [{'w': r['w'], 'c': r['c'], 'top': []} for r in word_rows]

    emote_rows = conn.execute(f"""SELECT emote_id id, MAX(emote_name) n, SUM(count) c
        FROM emote_stats WHERE stream_id IN ({ph}) GROUP BY emote_id ORDER BY c DESC LIMIT 1200""", ids).fetchall()
    emote_unique = conn.execute(f"""SELECT emote_id, COUNT(DISTINCT user_id) unique_users
        FROM user_emote_stats WHERE stream_id IN ({ph}) GROUP BY emote_id""", ids).fetchall()
    unique_map = {str(r['emote_id']): r['unique_users'] or 0 for r in emote_unique}

    top_emote_users = conn.execute(f"""
        WITH ranked AS (
            SELECT ue.emote_id, MAX(u.username) username, SUM(ue.count) c,
                ROW_NUMBER() OVER (PARTITION BY ue.emote_id ORDER BY SUM(ue.count) DESC) rn
            FROM user_emote_stats ue
            LEFT JOIN users u ON u.id=ue.user_id
            WHERE ue.stream_id IN ({ph})
            GROUP BY ue.emote_id, ue.user_id
        )
        SELECT emote_id, username, c FROM ranked WHERE rn <= 5
        ORDER BY emote_id, c DESC
    """, ids).fetchall()
    top_map: dict[str, list[dict]] = defaultdict(list)
    for r in top_emote_users:
        top_map[str(r['emote_id'])].append({'u': r['username'] or 'Unknown', 'c': r['c'] or 0})

    # lastUsed is intentionally derived from a bounded recent chat sample rather than
    # running a LIKE query for every emote.
    last_chat_rows = conn.execute(f"""SELECT timestamp, message FROM chat_messages
        WHERE stream_id IN ({ph}) ORDER BY id DESC LIMIT 5000""", ids).fetchall()
    last_used: dict[str, str] = {}
    for row in last_chat_rows:
        msg = row['message'] or ''
        for piece in msg.split('[emote:')[1:]:
            eid = piece.split(':', 1)[0]
            if eid and eid not in last_used:
                last_used[eid] = row['timestamp']

    emotes = [{
        'id': str(r['id']), 'n': r['n'], 'c': r['c'] or 0,
        'unique_users': unique_map.get(str(r['id']), 0),
        'top': top_map.get(str(r['id']), []),
        'lastUsed': last_used.get(str(r['id'])),
    } for r in emote_rows]

    spam_rows = conn.execute(f"""SELECT message_key key, MAX(message) m, SUM(count) c,
        MAX(unique_users) unique_users, MAX(last_username) username, MAX(last_timestamp) last_t,
        MAX(usernames_json) usernames_json FROM spam_stats
        WHERE stream_id IN ({ph}) GROUP BY message_key ORDER BY c DESC LIMIT 500""", ids).fetchall()
    spam = []
    for r in spam_rows:
        try:
            names = json.loads(r['usernames_json'] or '[]')
        except Exception:
            names = []
        spam.append({'key': r['key'], 'm': r['m'], 'c': r['c'], 'unique_users': r['unique_users'] or 0,
                     'last_t': r['last_t'], 'top': [{'u': n, 'c': None} for n in names[:10]]})

    event_rows = conn.execute(f"""SELECT id,stream_id,timestamp,event_name,event_type,username,target_username,
        moderator,message,reason,duration,permanent,session_type,message_id,quantity
        FROM events WHERE stream_id IN ({ph}) ORDER BY id DESC LIMIT ?""", ids + [event_limit]).fetchall()
    events = [dict(r) for r in reversed(event_rows)]

    # Build moderation views from one bounded event set. This removes the old
    # per-moderator target/reason/log query explosion.
    recent_mod_rows = conn.execute(f"""SELECT id,stream_id,timestamp,event_name,event_type,username,target_username,
        moderator,message,reason,duration,permanent,session_type,message_id,quantity
        FROM events WHERE stream_id IN ({ph}) AND event_type IN ('ban','timeout','unban','deleted')
        ORDER BY id DESC LIMIT 1000""", ids).fetchall()
    recent_mod = [dict(r) for r in reversed(recent_mod_rows)]
    mods_map = {}
    for ev in recent_mod:
        mod_name = ev.get('moderator') or 'Kick/System'
        mod = mods_map.setdefault(mod_name, {
            'n': mod_name, 'total_actions': 0, 'timeouts': 0, 'bans': 0, 'unbans': 0,
            'deleted_messages': 0, 'last_action_at': None, 'top_targets': [], 'top_reasons': [], 'logs': []
        })
        et = ev.get('event_type')
        mod['total_actions'] += 1
        if et == 'timeout': mod['timeouts'] += 1
        elif et == 'ban':
            if ev.get('permanent'): mod['bans'] += 1
            else: mod['timeouts'] += 1
        elif et == 'unban': mod['unbans'] += 1
        elif et == 'deleted': mod['deleted_messages'] += 1
        mod['last_action_at'] = max(mod['last_action_at'] or '', ev.get('timestamp') or '')
        mod['logs'].append({
            't': ev.get('timestamp'), 'action': ('timeout' if et == 'ban' and not ev.get('permanent') else et),
            'target': ev.get('target_username'), 'reason': ev.get('reason'),
            'duration': ev.get('duration'), 'msg': ev.get('message'),
            'permanent': ev.get('permanent'), 'quantity': ev.get('quantity'),
        })

    for mod in mods_map.values():
        target_counts = defaultdict(int)
        reason_counts = defaultdict(int)
        for log in mod['logs']:
            target_counts[log.get('target') or 'Bilinmiyor'] += 1
            reason_counts[log.get('reason') or 'Sebep belirtilmemiş'] += 1
        mod['top_targets'] = [{'n': k, 'c': v} for k, v in sorted(target_counts.items(), key=lambda x: x[1], reverse=True)[:10]]
        mod['top_reasons'] = [{'r': k, 'c': v} for k, v in sorted(reason_counts.items(), key=lambda x: x[1], reverse=True)[:10]]
        mod['logs'] = mod['logs'][-200:]

    mod_list = sorted(mods_map.values(), key=lambda x: x['total_actions'], reverse=True)
    moderation_summary = {
        'total_actions': sum(m['total_actions'] for m in mod_list),
        'timeouts': sum(m['timeouts'] for m in mod_list),
        'bans': sum(m['bans'] for m in mod_list),
        'unbans': sum(m['unbans'] for m in mod_list),
        'deleted_messages': sum(m['deleted_messages'] for m in mod_list),
    }

    return {
        'stats': stats,
        'users': users,
        'words': words,
        'emotes': emotes,
        'spam': spam,
        'moderation': {
            'summary': moderation_summary,
            'mods': mod_list,
            'recent_actions': recent_mod[-300:],
        },
        'events': events,
        'game_special': {
            'all_pool': words,
            'top_10_users': users[:10],
            'top_10_words': words[:10],
            'top_10_emotes': emotes[:10],
        },
    }


def _latest_chat_row(conn, stream_id):
    if not stream_id:
        return None
    row = conn.execute(
        'SELECT username,timestamp,message,message_id FROM chat_messages WHERE stream_id=? ORDER BY id DESC LIMIT 1',
        (stream_id,),
    ).fetchone()
    return dict(row) if row else None


def _build_data_response(mode, date, month, stream_id, *, force_refresh=False):
    conn = connect()
    try:
        ids, selected = stream_ids_for_mode(conn, mode, date, month, stream_id)
        meta = {'label': 'Canlı yayın' if mode == 'live' else mode, 'live_stream_active': False, 'session_type': 'stream'}
        if selected:
            meta.update({'stream_count': len(ids), 'stream_ids': ids, 'session_type': selected['session_type']})
        if mode == 'live' and not selected:
            meta['warning'] = 'Canlı yayın kapalı'
            return {'ok': True, 'mode': mode, 'stream': None, 'meta': meta, 'summary': _empty_summary()}
        if mode == 'stream' and not selected:
            return {'ok': False, 'error': 'stream_not_found'}

        cache_key = f'data:{mode}:{selected["id"] if selected else ",".join(map(str, ids))}'
        is_finished_single = mode == 'stream' and selected and selected['status'] == 'ended'
        ttl = HISTORICAL_CACHE_TTL if is_finished_single else LIVE_CACHE_TTL
        cached = None if force_refresh else _cache_get(cache_key, ttl)

        if cached is not None:
            return cached

        # Persistent cache for finished broadcasts survives app restarts.
        if is_finished_single and not force_refresh:
            persisted = get_stream_summary_cache(selected['id'])
            if persisted:
                try:
                    summary = json.loads(persisted['summary_json'])
                    viewer_history = json.loads(persisted.get('viewer_history_json') or '[]')
                    stream = row_dict(selected)
                    stream['display_label'] = build_stream_label(selected)
                    result = {'ok': True, 'mode': mode, 'stream': stream,
                              'meta': {**meta, 'viewer_history': viewer_history,
                                       'cache_generated_at': persisted['generated_at'], 'cache_source': 'persistent'},
                              'summary': summary}
                    _cache_set(cache_key, result)
                    return result
                except Exception:
                    pass

        summary = summary_for_stream_ids(conn, ids)
        if mode == 'live':
            meta['live_stream_active'] = True
        stream = row_dict(selected)
        if stream:
            stream['display_label'] = build_stream_label(selected)
            snaps = conn.execute(
                'SELECT captured_at,viewer_count FROM stream_snapshots WHERE stream_id=? ORDER BY id DESC LIMIT 360',
                (selected['id'],),
            ).fetchall()
            meta['viewer_history'] = [dict(r) for r in reversed(snaps)]
        result = {'ok': True, 'mode': mode, 'stream': stream, 'meta': meta, 'summary': summary}
        _cache_set(cache_key, result)

        if is_finished_single:
            set_stream_summary_cache(
                selected['id'],
                now_iso(),
                json.dumps(summary, ensure_ascii=False, separators=(',', ':')),
                json.dumps(meta.get('viewer_history', []), ensure_ascii=False, separators=(',', ':')),
            )
        return result
    finally:
        conn.close()


@app.get('/health')
async def health():
    active = await asyncio.to_thread(get_active_stream)
    conn = connect()
    try:
        latest = _latest_chat_row(conn, active['id'] if active else None)
    finally:
        conn.close()
    return {
        'ok': True,
        'channel': CHANNEL_SLUG,
        'live': bool(active),
        'db': 'ok',
        'recorder_running': bool(recorder and recorder.running),
        'recorder_task_done': bool(recorder_task and recorder_task.done()),
        'queue_size': recorder.queue.qsize() if recorder else 0,
        'ws_clients': len(connected_clients),
        'cache_entries': len(cache_store),
        'latest_chat': latest,
    }


@app.get('/api/status')
async def api_status():
    active = await asyncio.to_thread(get_active_stream)
    meta = await asyncio.to_thread(get_channel_meta)
    conn = connect()
    try:
        latest = _latest_chat_row(conn, active['id'] if active else None)
        db_stats = None
        if active:
            row = conn.execute(
                'SELECT total_messages,last_event_at,updated_at FROM stream_stats WHERE stream_id=?',
                (active['id'],),
            ).fetchone()
            db_stats = dict(row) if row else None
    finally:
        conn.close()
    return {
        'ok': True,
        'channel': meta or {'slug': CHANNEL_SLUG},
        'live': bool(meta and meta.get('is_live')),
        'active_stream': row_dict(active),
        'recorder_stats': recorder.stats if recorder else {},
        'recorder_task_done': bool(recorder_task and recorder_task.done()),
        'queue_size': recorder.queue.qsize() if recorder else 0,
        'ws_clients': len(connected_clients),
        'db_stats': db_stats,
        'latest_chat': latest,
    }


@app.get('/api/streams')
async def api_streams(limit: int = Query(30, ge=1, le=120)):
    key = f'streams:{limit}'
    async def factory():
        def fetch():
            conn = connect()
            try:
                rows = conn.execute(
                    "SELECT * FROM streams WHERE session_type='stream' AND status='ended' ORDER BY id DESC LIMIT ?",
                    (limit,),
                ).fetchall()
                return {'ok': True, 'streams': [row_dict(r) for r in rows]}
            finally:
                conn.close()
        return await asyncio.to_thread(fetch)
    return await cached_value(key, factory, STREAMS_CACHE_TTL)


@app.get('/api/streams/active')
async def api_active_stream():
    row = await asyncio.to_thread(get_active_stream)
    return {'ok': True, 'active': row_dict(row)}


@app.get('/api/streams/{stream_id}/events')
async def api_stream_events(stream_id: int, limit: int = Query(250, ge=1, le=1000), before_id: Optional[int] = None):
    def fetch():
        conn = connect()
        try:
            if before_id:
                rows = conn.execute(
                    'SELECT * FROM events WHERE stream_id=? AND id<? ORDER BY id DESC LIMIT ?',
                    (stream_id, before_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    'SELECT * FROM events WHERE stream_id=? ORDER BY id DESC LIMIT ?',
                    (stream_id, limit),
                ).fetchall()
            return {'ok': True, 'events': [dict(r) for r in reversed(rows)]}
        finally:
            conn.close()
    return await asyncio.to_thread(fetch)


@app.get('/api/user_messages')
async def api_user_messages(username: str, stream_id: Optional[int] = None, limit: int = Query(100, ge=1, le=500), before_id: Optional[int] = None):
    def fetch():
        conn = connect()
        try:
            params = [username]
            where = 'username=?'
            if stream_id:
                where += ' AND stream_id=?'
                params.append(stream_id)
            if before_id:
                where += ' AND id<?'
                params.append(before_id)
            rows = conn.execute(
                f'SELECT id,username,timestamp,message,message_id FROM chat_messages WHERE {where} ORDER BY id DESC LIMIT ?',
                params + [limit],
            ).fetchall()
            return {'ok': True, 'messages': [dict(r) for r in reversed(rows)], 'next_cursor': rows[-1]['id'] if rows else None}
        finally:
            conn.close()
    return await asyncio.to_thread(fetch)


@app.get('/api/user_detail')
async def api_user_detail(username: str, stream_id: Optional[int] = None, limit: int = Query(100, ge=1, le=300)):
    def fetch():
        conn = connect()
        try:
            params = [username]
            where = 'cm.username=?'
            if stream_id:
                where += ' AND cm.stream_id=?'
                params.append(stream_id)
            user = conn.execute('SELECT * FROM users WHERE username=? COLLATE NOCASE LIMIT 1', (username,)).fetchone()
            messages = conn.execute(
                f'SELECT id,timestamp,message,stream_id,message_id FROM chat_messages cm WHERE {where} ORDER BY cm.id DESC LIMIT ?',
                params + [limit],
            ).fetchall()
            uid = user['id'] if user else None
            words, emotes = [], []
            if uid:
                if stream_id:
                    words = [dict(r) for r in conn.execute(
                        '''SELECT word,count FROM user_word_stats WHERE user_id=? AND stream_id=?
                           AND word NOT LIKE 'emote:%' COLLATE NOCASE AND word NOT LIKE '[emote:%' COLLATE NOCASE
                           ORDER BY count DESC LIMIT 20''', (uid, stream_id)).fetchall()]
                    emotes = [dict(r) for r in conn.execute(
                        'SELECT emote_id id,emote_name n,count c FROM user_emote_stats WHERE user_id=? AND stream_id=? ORDER BY count DESC LIMIT 20',
                        (uid, stream_id)).fetchall()]
                else:
                    words = [dict(r) for r in conn.execute(
                        '''SELECT word,SUM(count) count FROM user_word_stats WHERE user_id=?
                           AND word NOT LIKE 'emote:%' COLLATE NOCASE AND word NOT LIKE '[emote:%' COLLATE NOCASE
                           GROUP BY word ORDER BY count DESC LIMIT 20''', (uid,)).fetchall()]
                    emotes = [dict(r) for r in conn.execute(
                        'SELECT emote_id id,MAX(emote_name) n,SUM(count) c FROM user_emote_stats WHERE user_id=? GROUP BY emote_id ORDER BY c DESC LIMIT 20',
                        (uid,)).fetchall()]
            history = []
            mod_received = {'timeouts': 0, 'bans': 0, 'unbans': 0, 'deleted_messages': 0}
            if user:
                rows = conn.execute(
                    '''SELECT timestamp,event_type,moderator,target_username,reason,duration,permanent,message,quantity
                       FROM events WHERE LOWER(COALESCE(target_username,''))=LOWER(?) ORDER BY id DESC LIMIT 100''',
                    (user['username'],),
                ).fetchall()
                for r in rows:
                    d = dict(r)
                    history.append(d)
                    if d['event_type'] in ('timeout', 'ban', 'unban'):
                        mod_received[d['event_type'] if d['event_type'] != 'ban' or d['permanent'] else 'timeouts'] += 1
                    elif d['event_type'] == 'deleted':
                        mod_received['deleted_messages'] += 1
            return {
                'ok': True, 'user': dict(user) if user else None, 'words': words, 'emotes': emotes,
                'messages': [dict(r) for r in reversed(messages)],
                'next_cursor': messages[-1]['id'] if messages else None,
                'mod_received': mod_received, 'mod_history_received': list(reversed(history)),
            }
        finally:
            conn.close()
    return await asyncio.to_thread(fetch)


@app.get('/api/word_detail')
async def api_word_detail(word: str, stream_id: Optional[int] = None, limit: int = Query(10, ge=1, le=50)):
    def fetch():
        conn = connect()
        try:
            active = conn.execute("SELECT id FROM streams WHERE status='live' AND session_type='stream' ORDER BY id DESC LIMIT 1").fetchone()
            ids = [stream_id] if stream_id else ([active['id']] if active else [])
            if not ids:
                return {'ok': True, 'word': word, 'count': 0, 'unique_users': 0, 'top_users': []}
            ph = ','.join('?' * len(ids))
            row = conn.execute(
                f'SELECT COALESCE(SUM(count),0) c FROM word_stats WHERE stream_id IN ({ph}) AND word=? COLLATE NOCASE',
                ids + [word],
            ).fetchone()
            top = conn.execute(
                f'''SELECT uw.user_id id,MAX(u.username) name,SUM(uw.count) count
                    FROM user_word_stats uw LEFT JOIN users u ON u.id=uw.user_id
                    WHERE uw.stream_id IN ({ph}) AND uw.word=? COLLATE NOCASE
                    GROUP BY uw.user_id ORDER BY count DESC LIMIT ?''',
                ids + [word, limit],
            ).fetchall()
            unique = conn.execute(
                f'SELECT COUNT(DISTINCT user_id) c FROM user_word_stats WHERE stream_id IN ({ph}) AND word=? COLLATE NOCASE',
                ids + [word],
            ).fetchone()['c']
            return {'ok': True, 'word': word, 'count': row['c'] if row else 0, 'unique_users': unique or 0,
                    'top_users': [{'id': r['id'], 'name': r['name'] or 'Unknown', 'count': r['count']} for r in top]}
        finally:
            conn.close()
    return await asyncio.to_thread(fetch)


@app.get('/api/emotes')
async def api_emotes(stream_id: Optional[int] = None):
    def fetch():
        conn = connect()
        try:
            active = conn.execute("SELECT id FROM streams WHERE status='live' AND session_type='stream' ORDER BY id DESC LIMIT 1").fetchone()
            ids = [stream_id] if stream_id else ([active['id']] if active else [])
            if not ids:
                return {'ok': True, 'emotes': []}
            ph = ','.join('?' * len(ids))
            rows = conn.execute(
                f'SELECT emote_id id,MAX(emote_name) n,SUM(count) c FROM emote_stats WHERE stream_id IN ({ph}) GROUP BY emote_id ORDER BY c DESC LIMIT 1200',
                ids,
            ).fetchall()
            unique = {str(r['emote_id']): r['unique_users'] for r in conn.execute(
                f'SELECT emote_id,COUNT(DISTINCT user_id) unique_users FROM user_emote_stats WHERE stream_id IN ({ph}) GROUP BY emote_id', ids
            ).fetchall()}
            return {'ok': True, 'emotes': [
                {'id': str(r['id']), 'n': r['n'], 'c': r['c'], 'unique_users': unique.get(str(r['id']), 0), 'top': []}
                for r in rows
            ]}
        finally:
            conn.close()
    return await asyncio.to_thread(fetch)


@app.get('/api/live/stats')
async def api_live_stats(refresh: int = Query(0, ge=0, le=1)):
    """Tiny live endpoint used by the dashboard ticker instead of reloading the full dataset."""
    active = await asyncio.to_thread(get_active_stream)
    if not active:
        return {'ok': True, 'live': False, 'stats': _empty_summary()['stats'], 'updated_at': now_iso()}

    def fetch():
        conn = connect()
        try:
            row = conn.execute('''SELECT total_messages,deleted_messages,timeouts,bans,unbans,subscriptions,
                gift_subscriptions,other_events,peak_chat_per_minute,peak_chat_per_second
                FROM stream_stats WHERE stream_id=?''', (active['id'],)).fetchone()
            unique = conn.execute('SELECT COUNT(DISTINCT user_id) c FROM user_stream_stats WHERE stream_id=?', (active['id'],)).fetchone()['c']
            latest = _latest_chat_row(conn, active['id'])
            stats = dict(row) if row else {}
            stats.update({'total_users': unique or 0, 'total_msgs': stats.get('total_messages', 0), 'unique_users': unique or 0})
            return {'ok': True, 'live': True, 'stream_id': active['id'], 'stats': stats, 'latest_chat': latest, 'updated_at': now_iso()}
        finally:
            conn.close()
    # The live stats endpoint is intentionally not tied to the heavy dashboard cache.
    return await asyncio.to_thread(fetch)


@app.get('/api/data')
async def api_data(mode: str = 'live', date: Optional[str] = None, month: Optional[str] = None,
                   stream_id: Optional[int] = None, refresh: int = Query(0, ge=0, le=1)):
    mode = mode if mode in ('live', 'stream', 'day', 'offstream_day', 'week', 'month', 'all') else 'live'
    # Avoid doing the expensive work directly on the event loop.
    key = f'api:{mode}:{date}:{month}:{stream_id}:{refresh}'
    if refresh:
        result = await asyncio.to_thread(_build_data_response, mode, date, month, stream_id, force_refresh=True)
    else:
        async def factory():
            return await asyncio.to_thread(_build_data_response, mode, date, month, stream_id, force_refresh=False)
        ttl = HISTORICAL_CACHE_TTL if mode == 'stream' else LIVE_CACHE_TTL
        result = await cached_value(key, factory, ttl)
    return result


@app.websocket('/ws')
async def websocket_endpoint(websocket: WebSocket):
    if len(connected_clients) >= MAX_WS_CLIENTS:
        await websocket.close(code=1013, reason='Sunucu kapasitesi dolu')
        return

    await websocket.accept()
    connected_clients.add(websocket)
    ip = _client_ip_from_ws(websocket)
    try:
        await websocket.send_text(json.dumps({
            'type': 'bootstrap',
            'time': now_iso(),
            'state': {'channel': CHANNEL_SLUG},
        }, ensure_ascii=False, separators=(',', ':')))

        # Hydrate from the in-memory recent cache first. Only fall back to SQLite after
        # a fresh process start when the cache is empty.
        active = await asyncio.to_thread(get_active_stream)
        recent_for_stream = [m for m in reversed(recent_messages) if active and m.get('stream_id') == active['id']]
        if active and not recent_for_stream:
            def load_bootstrap():
                conn = connect()
                try:
                    rows = conn.execute(
                        '''SELECT username,message,timestamp,message_id
                           FROM chat_messages WHERE stream_id=? ORDER BY id DESC LIMIT 50''',
                        (active['id'],),
                    ).fetchall()
                    return [
                        {'type': 'chat', 'event': 'ChatMessageEvent', 'user': row['username'], 'msg': row['message'],
                         'message_id': row['message_id'], 'time': row['timestamp'], 'stream_id': active['id']}
                        for row in reversed(rows)
                    ]
                finally:
                    conn.close()
            recent_for_stream = await asyncio.to_thread(load_bootstrap)
        for payload in recent_for_stream[-50:]:
            await asyncio.wait_for(websocket.send_text(json.dumps(payload, ensure_ascii=False, separators=(',', ':'))), timeout=1.0)

        while True:
            await websocket.receive_text()
    except (asyncio.CancelledError, WebSocketDisconnect):
        pass
    except Exception:
        pass
    finally:
        connected_clients.discard(websocket)


@app.on_event('startup')
async def startup_event():
    global recorder, recorder_task, broadcast_task
    init_db()
    stop_event = asyncio.Event()
    app.state.stop_event = stop_event
    recorder = KickRecorder()
    recorder.on_event = recorder_event
    recorder_task = asyncio.create_task(recorder.run(), name='kick-recorder-service')
    broadcast_task = asyncio.create_task(broadcast_loop(stop_event), name='kick-ws-broadcast-service')
    print(
        f'[SERVER] started recorder pid={os.getpid()} db={DB_PATH} ws_max={MAX_WS_CLIENTS} cache_live={LIVE_CACHE_TTL}s',
        flush=True,
    )


@app.on_event('shutdown')
async def shutdown_event():
    global recorder_task, broadcast_task
    if recorder:
        recorder.running = False
        recorder.stop_event.set()
    stop_event = getattr(app.state, 'stop_event', None)
    if stop_event:
        stop_event.set()
    for task in (broadcast_task, recorder_task):
        if task:
            try:
                await asyncio.wait_for(task, timeout=20)
            except asyncio.TimeoutError:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


app.mount('/', StaticFiles(directory=str(BASE_DIR / 'static'), html=True), name='static')
