# Release 15.0.0

- Public analytics pages no longer require login.
- Admin login and access history are available only at `/admin`; direct `/admin-access.html` access returns 404.
- Clean extensionless page URLs are supported, and `.html` URLs redirect to them.
- Message search remains username-only.
- Emoji top-user pills use Kick profile photos and a responsive non-overlapping grid.
- Moderator cards use Kick profile photos when available.
- Emoji last-used time continues to be derived from captured chat archive; it can be empty if the selected stream's captured messages do not include that emote.
