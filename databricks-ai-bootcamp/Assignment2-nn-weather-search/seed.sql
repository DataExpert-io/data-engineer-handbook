-- Seed data for the Novanega Feedback & Feature Request Tracker.
-- Satisfies the PRD's seed data requirement: >=3 tickets, >=2 messages
-- each, >=2 distinct statuses. Run after schema.sql.

INSERT INTO tickets (title, category, status, priority, submitted_by)
VALUES
    ('Add Spanish-language filter to provider search', 'Feature Request', 'open', 'Medium', 'pilot_user'),
    ('Insurance panel info isn''t clear on provider cards', 'Feedback', 'in_progress', 'Medium', 'pilot_user'),
    ('Heat map legend colors hard to distinguish', 'Bug', 'resolved', 'Low', 'internal');

-- Ticket 1: "Add Spanish-language filter to provider search"
INSERT INTO ticket_messages (ticket_id, message_text, author)
SELECT ticket_id, 'A lot of the pilot users searching from South Lawndale and Little Village are switching their browser to Spanish first, then giving up when the provider list stays in English-only labels.', 'Maria G.'
FROM tickets WHERE title = 'Add Spanish-language filter to provider search';

INSERT INTO ticket_messages (ticket_id, message_text, author)
SELECT ticket_id, 'Logged as a filter toggle for now, not a full localization pass -- scoping just the search/filter labels first.', 'Rachel Martz'
FROM tickets WHERE title = 'Add Spanish-language filter to provider search';

-- Ticket 2: "Insurance panel info isn't clear on provider cards"
INSERT INTO ticket_messages (ticket_id, message_text, author)
SELECT ticket_id, 'Provider card shows "accepts most major insurers" but doesn''t say which ones -- pilot users want to know before they call.', 'Devon P.'
FROM tickets WHERE title = 'Insurance panel info isn''t clear on provider cards';

INSERT INTO ticket_messages (ticket_id, message_text, author)
SELECT ticket_id, 'In progress -- redesigning the card to list accepted panels explicitly instead of the vague summary line.', 'Rachel Martz'
FROM tickets WHERE title = 'Insurance panel info isn''t clear on provider cards';

-- Ticket 3: "Heat map legend colors hard to distinguish"
INSERT INTO ticket_messages (ticket_id, message_text, author)
SELECT ticket_id, 'The two middle tiers on the desert heat map read almost identically for anyone with red-green color vision deficiency.', 'internal-qa'
FROM tickets WHERE title = 'Heat map legend colors hard to distinguish';

INSERT INTO ticket_messages (ticket_id, message_text, author)
SELECT ticket_id, 'Resolved -- swapped to a colorblind-safe sequential palette and added a texture pattern as a second visual cue.', 'Rachel Martz'
FROM tickets WHERE title = 'Heat map legend colors hard to distinguish';
