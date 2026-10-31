# Release 14 — Kick-inspired redesign, private admin and chat archive search

## UI
- Reworked global color tokens toward Kick's black/neon-green identity without copying KickLogz layouts.
- Improved sidebar, cards, forms, borders, scrollbars and responsive spacing.
- Fixed dashboard quick-summary grid so labels and values do not overflow narrow columns.
- Added dedicated Site Girişleri and Mesaj Arama pages.

## Access control
- All dashboard pages, APIs and WebSocket connections require an admin session.
- Login: `ADMIN_USERNAME` / `ADMIN_PASSWORD` from `.env` (defaults match the requested local test credentials).
- Session cookie is HttpOnly, SameSite=Strict, expires after 7 days, and is signed with a persistent local secret in `data/auth_secret.key`.
- Failed login attempts are rate-limited per IP in-process.
- Page visits are stored in `site_access_log` inside the existing SQLite database. API polling is not counted as page visits.
- Access history displays session identifier, IP, page, timestamp and browser user-agent.

## Chat search
- `/search.html` searches messages already captured in this installation's `chat_messages` archive by text, username and date range.
- It does not claim to search messages from before the recorder started.
- Historical all-Kick search requires a legitimate external data source/API entitlement. KickLogz documents its own customer API and API key; this project does not scrape or bypass its service.

## Local test
1. Copy `.env.example` to `.env` and set `KICK_CHANNEL_SLUG=rraenee`.
2. Set `ADMIN_USERNAME=admin` and `ADMIN_PASSWORD=arda2004` for the requested local test.
3. Run `powershell -ExecutionPolicy Bypass -File .\\run_local.ps1`.
4. Open `http://127.0.0.1:8000` and sign in.
5. Visit **Yönetim → Site Girişleri** to inspect access history.

## Security before VPS/public use
- Change the requested test password to a unique strong password before publishing.
- Use HTTPS only. The cookie is marked Secure automatically when the request uses HTTPS.
- Keep `data/auth_secret.key` and the SQLite database backed up and private.
- The login rate limiter is per process; put a reverse proxy/WAF rate limit in front for public exposure.
