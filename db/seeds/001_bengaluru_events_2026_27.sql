-- QuickTicket demo seed: Bengaluru live events and concerts.
-- Source snapshot checked on 2026-11-18. Event listings can change; verify with the
-- organiser/ticketing partner before treating any listing as live inventory.
-- Venue capacities below are approximate demo values, not official seating manifests.
-- Re-runnable: inserts only records that do not already exist.

INSERT INTO venues (name, city, capacity)
SELECT v.name, 'Bengaluru', v.capacity
FROM (VALUES
  ('District Arena @ Terraform', 12000),
  ('Bhartiya Mall of Bengaluru', 8000),
  ('NICE Grounds', 30000),
  ('Phoenix Marketcity, Whitefield', 5000),
  ('Karnataka Trade Promotion Organisation (KTPO)', 10000),
  ('Bengaluru LIVE', 15000)
) AS v(name, capacity)
WHERE NOT EXISTS (
  SELECT 1 FROM venues existing
  WHERE lower(existing.name) = lower(v.name) AND lower(existing.city) = 'bengaluru'
);

INSERT INTO performers (name, description)
SELECT p.name, p.description
FROM (VALUES
  ('Shreya Ghoshal', 'Indian playback singer; Unstoppable India Tour'),
  ('Alesso', 'Swedish DJ and electronic music producer'),
  ('Diljit Dosanjh', 'Punjabi singer and actor; AURA India Tour'),
  ('Darshan Raval', 'Indian singer-songwriter; Love Letters India Tour'),
  ('Yuvan Shankar Raja', 'Indian film composer and playback singer; U1NIVERSE Tour'),
  ('Sonu Nigam', 'Indian playback singer; The Revolution India Tour'),
  ('Fred again..', 'British electronic music producer and DJ'),
  ('Khalid', 'American singer-songwriter'),
  ('DJ Snake', 'French DJ and record producer'),
  ('The Chainsmokers', 'American electronic DJ and production duo'),
  ('Gorillaz', 'British virtual band'),
  ('Foo Fighters', 'American rock band')
) AS p(name, description)
WHERE NOT EXISTS (
  SELECT 1 FROM performers existing WHERE lower(existing.name) = lower(p.name)
);

INSERT INTO events (name, description, event_type, starts_at, venue_id, performer_id)
SELECT e.name, e.description, 'concert', e.starts_at::timestamptz, v.id, p.id
FROM (VALUES
  ('Shreya Ghoshal - The Unstoppable India Tour', 'Listed for Bengaluru on 22 November 2026. Verify event status and ticketing with the organiser.', '2026-11-22 18:00:00+05:30', 'District Arena @ Terraform', 'Shreya Ghoshal'),
  ('Alesso - Bengaluru', 'Listed for Bengaluru on 27 November 2026. Verify event status and ticketing with the organiser.', '2026-11-27 16:00:00+05:30', 'Bhartiya Mall of Bengaluru', 'Alesso'),
  ('Diljit Dosanjh - AURA India Tour', 'Listed for Bengaluru on 29 November 2026. Verify event status and ticketing with the organiser.', '2026-11-29 16:00:00+05:30', 'NICE Grounds', 'Diljit Dosanjh'),
  ('Darshan Raval - Love Letters India Tour', 'Listed for Bengaluru on 29 November 2026. Verify event status and ticketing with the organiser.', '2026-11-29 17:30:00+05:30', 'Bhartiya Mall of Bengaluru', 'Darshan Raval'),
  ('Yuvan Shankar Raja - U1NIVERSE Tour', 'Listed for Bengaluru on 5 December 2026. Verify event status and ticketing with the organiser.', '2026-12-05 18:00:00+05:30', 'NICE Grounds', 'Yuvan Shankar Raja'),
  ('Sonu Nigam - The Revolution India Tour', 'Listed for Bengaluru on 6 December 2026. Verify event status and ticketing with the organiser.', '2026-12-06 19:00:00+05:30', 'Karnataka Trade Promotion Organisation (KTPO)', 'Sonu Nigam'),
  ('Fred again.. - India Tour', 'Listed for Bengaluru on 13 December 2026. Verify event status and ticketing with the organiser.', '2026-12-13 17:00:00+05:30', 'NICE Grounds', 'Fred again..'),
  ('Khalid - Live in Bengaluru', 'Listed for Bengaluru on 15 December 2026. Verify event status and ticketing with the organiser.', '2026-12-15 19:00:00+05:30', 'Bhartiya Mall of Bengaluru', 'Khalid'),
  ('Sunburn Arena ft. DJ Snake', 'Listed for Bengaluru on 18 December 2026. Verify event status and ticketing with the organiser.', '2026-12-18 16:00:00+05:30', 'NICE Grounds', 'DJ Snake'),
  ('Sunburn Arena ft. The Chainsmokers', 'Listed for Bengaluru on 20 December 2026. Verify event status and ticketing with the organiser.', '2026-12-20 16:00:00+05:30', 'NICE Grounds', 'The Chainsmokers'),
  ('Gorillaz - Bengaluru', 'Listed for Bengaluru on 23 January 2027. Verify event status and ticketing with the organiser.', '2027-01-23 18:00:00+05:30', 'District Arena @ Terraform', 'Gorillaz'),
  ('Foo Fighters - Bengaluru', 'Listed for Bengaluru on 29 January 2027. Verify event status and ticketing with the organiser.', '2027-01-29 17:00:00+05:30', 'Bengaluru LIVE', 'Foo Fighters')
) AS e(name, description, starts_at, venue_name, performer_name)
JOIN venues v ON lower(v.name) = lower(e.venue_name) AND lower(v.city) = 'bengaluru'
JOIN performers p ON lower(p.name) = lower(e.performer_name)
WHERE NOT EXISTS (
  SELECT 1 FROM events existing
  WHERE lower(existing.name) = lower(e.name)
    AND existing.starts_at = e.starts_at::timestamptz
);

-- Add a small, clearly synthetic seat inventory for each seeded event so the
-- waiting-room -> hold -> mock-payment booking flow can be tested immediately.
-- These are demo seat labels/prices, not actual venue seating or sale prices.
INSERT INTO tickets (event_id, section, seat_number, price)
SELECT e.id, 'Demo General Admission', 'DEMO-' || seat_no, 1499.00
FROM events e
JOIN (VALUES
  ('Shreya Ghoshal - The Unstoppable India Tour'),
  ('Alesso - Bengaluru'),
  ('Diljit Dosanjh - AURA India Tour'),
  ('Darshan Raval - Love Letters India Tour'),
  ('Yuvan Shankar Raja - U1NIVERSE Tour'),
  ('Sonu Nigam - The Revolution India Tour'),
  ('Fred again.. - India Tour'),
  ('Khalid - Live in Bengaluru'),
  ('Sunburn Arena ft. DJ Snake'),
  ('Sunburn Arena ft. The Chainsmokers'),
  ('Gorillaz - Bengaluru'),
  ('Foo Fighters - Bengaluru')
) AS seeded(event_name) ON lower(seeded.event_name) = lower(e.name)
CROSS JOIN generate_series(1, 20) AS seat_no
ON CONFLICT (event_id, section, seat_number) DO NOTHING;
