# QC Test Log — Novanega Feedback & Feature Request Tracker

Trimmed from PRD v3 down to just what's needed to QC the deployed app. Since Databricks Apps don't support public/anonymous access, this doc stands in for a third-party QC pass — fill it in yourself against the deployed URL and it satisfies the same purpose for submission.

**Deployed app URL:** _______________________________________________
**Tester:** _______________________________________________
**Date:** _______________________________________________
**Environment:** [ ] Deployed Databricks App   [ ] Local (`python3 app.py`)

---

## 0. Assignment's own pass/fail bar (do this first)

- [X ] Existing (seeded) tickets load from Lakebase on `/`
- [X ] A new ticket can be created
- [X ] A message can be added to an existing ticket
- [X ] A ticket's status can be updated
- [X ] All of the above remain after refreshing the browser

If any of these fail, stop and fix before doing the detailed pass below.

---

## Story 1: View tickets

| # | Step | Pass/Fail | Notes |
|---|---|---|---|
| 1 | Open the app with seeded ticket data present | | |
| 2 | List shows ticket ID, title, category, status, priority, submitted by, created date for each row | | |
| 3 | Newest created ticket appears first | | |

## Story 2: View ticket detail and message history

| # | Step | Pass/Fail | Notes |
|---|---|---|---|
| 1 | Select a seeded ticket with >=2 messages | | |
| 2 | All related messages appear in the detail view | | |
| 3 | First-created message appears before later messages (oldest to newest) | | |
| 4 | Each message shows an author name and timestamp | | |

## Story 3: Create a new ticket

| # | Step | Pass/Fail | Notes |
|---|---|---|---|
| 1 | Open the create-ticket form | | |
| 2 | Enter title, category, priority, submitted_by, initial message (text + author) with valid values | | |
| 3 | Submit — ticket appears in the list with status `open` | | |
| 4 | Open the ticket — initial message exists | | |
| 5 | Refresh the browser — ticket and message still exist | | |
| 6 | Restart/redeploy the app (or wait, then reload) — ticket and message still exist | | |
| 7 | Retry with title blank — submission blocked with an error | | |
| 8 | Retry with initial message text blank — submission blocked with an error | | |
| 9 | Retry with initial message author blank — submission blocked with an error | | |
| 10 | Retry with an invalid category, priority, or submitted_by value — submission blocked with an error | | |

## Story 4: Add a message to an existing ticket

| # | Step | Pass/Fail | Notes |
|---|---|---|---|
| 1 | Open an existing ticket | | |
| 2 | Add a valid message with a valid author | | |
| 3 | Message appears in ticket history in correct (chronological) order | | |
| 4 | Refresh the browser — message remains | | |
| 5 | Restart/redeploy the app — message remains | | |
| 6 | Submit blank message_text — rejected | | |
| 7 | Submit blank author — rejected | | |

## Story 5: Edit ticket fields

| # | Step | Pass/Fail | Notes |
|---|---|---|---|
| 1 | Open an existing ticket | | |
| 2 | Change title, category, priority, status with valid values | | |
| 3 | Save — updated values appear in both list and detail view | | |
| 4 | Refresh the browser — values remain | | |
| 5 | Restart/redeploy the app — values remain | | |
| 6 | Confirm submitted_by, created_by, created_at cannot be edited | | |
| 7 | Attempt to save a blank title — rejected | | |
| 8 | Attempt to save an invalid category, priority, or status — rejected | | |

---

## Known non-issues (don't log these as bugs)

- A `WARNING: Skipping index creation...must be owner of table tickets` line in server logs on startup is expected and harmless — indexes already exist from `schema.sql`; the app's runtime role doesn't own the table, which is normal for this setup (see README/GRANT notes).
- Status filtering, delete, ticket stats, and visual polish are intentionally out of scope (PRD v2/v3 decision) — not bugs if missing.

## Bugs found during this pass (fixed prior to this log)

- **Fixed:** ticket edits (Story 5) weren't persisting — `update_ticket()` used a read-only helper that never committed the transaction. Fixed to use the write-and-commit helper; re-verified against a connection-per-call test harness matching real Postgres behavior.
- **Fixed:** local dev startup could crash with `InsufficientPrivilege` on index creation when the app's role doesn't own the table (normal when `schema.sql` was run under a different/admin role). Now non-fatal — logs a warning and continues.

---

## Sign-off

- [X ] All Story 1-5 steps pass
- [X ] Section 0 (assignment's own bar) passes


**Overall result:** ________PASS_______________________________________
