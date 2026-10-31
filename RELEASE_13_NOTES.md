# Release 13.0.0 — UI redesign and timestamp fixes

## Changes
- Reworked the shared visual system with a Kick-inspired dark/green palette, cleaner panels, responsive spacing and safer text wrapping.
- Improved moderator identity icon treatment and moderation event cards.
- Recent moderation events are sorted newest-first before rendering.
- Date parsing now treats timezone-less backend/SQLite timestamps as UTC, then displays them in Europe/Istanbul time.
- Bumped static asset cache versions to 13.0.0.

## Deployment
1. Back up the current project folder and SQLite database.
2. Stop `KickLive` (and `KickCaddy` only if you want the whole site offline).
3. Replace application source/static files with this release.
4. Keep your existing `.env` and database; they are intentionally not included in this archive.
5. Start `KickLive`, then `KickCaddy` if it was stopped.
6. Hard-refresh the browser (Ctrl+F5) and check the dashboard/moderation pages.

This archive intentionally excludes `.env`, virtualenv, Git metadata, and database backups.
