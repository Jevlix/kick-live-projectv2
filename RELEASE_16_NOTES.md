# V16 — Live Chat & UI polish

- Public dashboard WebSocket `/ws` no longer requires the private admin session. This fixes live chat for public pages.
- Private admin panel remains protected at `/admin`; `/api/admin/access` remains authenticated.
- Removed the global top-right logout control from public dashboard pages.
- Emoji leaderboard top-user cards now show full usernames with wrapping instead of ellipsis/clipping.
- Emoji user cards use a stable responsive grid to prevent overlap.
- Updated static asset cache versions to 16.0.0.

Design direction follows dashboard usability guidance: clear hierarchy, restrained accent color, whitespace, consistent typography, and responsive layouts.
