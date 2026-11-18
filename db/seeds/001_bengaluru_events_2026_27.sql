-- QuickTicket demo seed: Bengaluru concerts and live shows.
-- Source snapshot: 2026-11-18. Listings are third-party ticketing/event listings,
-- not a guarantee that tickets remain on sale. Re-check the official organiser
-- or ticketing partner before using these as real-world availability.
--
-- Sources checked:
-- District Bengaluru event listings:
-- https://www.district.in/events/upcoming-events-in-bengaluru
-- India What's On Bengaluru calendar:
-- https://indiawhatson.com/cities/bengaluru/
-- All Day Music Bengaluru gig guide:
-- https://www.alldaymusic.club/events/bengaluru/november-2026
--
-- Venue capacities and ticket inventory below are illustrative demo data.
-- Demo prices are NOT the advertised ticket prices. Times are local IST.
-- Re-runnable: inserts only missing records.

INSERT INTO venues (name, city, capacity)
SELECT v.name, 'Bengaluru', v.capacity
FROM (VALUES
  ('District Arena @ Terraform', 12000),
  ('NICE Grounds', 30000),
  ('The Foothills of Nandi', 15000),
  ('Karnataka Trade Promotion Organisation (KTPO)', 10000)
) AS v(name, capacity)
WHERE NOT EXISTS (
  SELECT 1 FROM venues existing
  WHERE lower(existing.name) = lower(v.name) AND lower(existing.city) = 'bengaluru'
);

INSERT INTO performers (name, description)
SELECT p.name, p.description
FROM (VALUES
  ('Dr. Satinder Sartaaj', 'Punjabi singer, poet and performer; The Philosophy of Resonance Tour'),
  ('Shreya Ghoshal', 'Indian playback singer; The Unstoppable India Tour'),
  ('Yuvan Shankar Raja', 'Indian film composer and playback singer; U1NIVERSE Tour'),
  ('Echoes of Earth', 'Bengaluru music and sustainability festival'),
  ('Sonu Nigam', 'Indian playback singer; The Revolution India Tour'),
  ('DJ Snake', 'French DJ and record producer; Sunburn Arena'),
  ('The Chainsmokers', 'American electronic DJ and production duo; Sunburn Arena')
) AS p(name, description)
WHERE NOT EXISTS (
  SELECT 1 FROM performers existing WHERE lower(existing.name) = lower(p.name)
);

INSERT INTO events (name, description, event_type, starts_at, venue_id, performer_id)
SELECT e.name, e.description, e.event_type, e.starts_at::timestamptz, v.id, p.id
FROM (VALUES
  ('The Philosophy of Resonance - Satinder Sartaaj Live', 'Listed for Bengaluru on 20 November 2026 at District Arena @ Terraform. Check the ticketing partner for current availability.', 'concert', '2026-11-20 19:00:00+05:30', 'District Arena @ Terraform', 'Dr. Satinder Sartaaj'),
  ('Shreya Ghoshal - The Unstoppable India Tour', 'Listed for Bengaluru on 22 November 2026 at District Arena @ Terraform. Check the ticketing partner for current availability.', 'concert', '2026-11-22 18:00:00+05:30', 'District Arena @ Terraform', 'Shreya Ghoshal'),
  ('Yuvan Shankar Raja - U1NIVERSE Tour', 'Listed for Bengaluru on 5 December 2026 at NICE Grounds. Check the ticketing partner for current availability.', 'concert', '2026-12-05 18:00:00+05:30', 'NICE Grounds', 'Yuvan Shankar Raja'),
  ('Echoes of Earth 2026', 'Two-day Bengaluru music and sustainability festival listed for 5-6 December 2026 at the Foothills of Nandi.', 'festival', '2026-12-05 13:00:00+05:30', 'The Foothills of Nandi', 'Echoes of Earth'),
  ('Sonu Nigam - The Revolution India Tour', 'Listed for Bengaluru on 6 December 2026 at Karnataka Trade Promotion Organisation. Check the ticketing partner for current availability.', 'concert', '2026-12-06 19:00:00+05:30', 'Karnataka Trade Promotion Organisation (KTPO)', 'Sonu Nigam'),
  ('Sunburn Arena ft. DJ Snake', 'Listed for Bengaluru on 18 December 2026 at NICE Grounds. Check the ticketing partner for current availability.', 'concert', '2026-12-18 16:00:00+05:30', 'NICE Grounds', 'DJ Snake'),
  ('Sunburn Arena ft. The Chainsmokers', 'Listed for Bengaluru on 20 December 2026 at NICE Grounds. Check the ticketing partner for current availability.', 'concert', '2026-12-20 16:00:00+05:30', 'NICE Grounds', 'The Chainsmokers')
) AS e(name, description, event_type, starts_at, venue_name, performer_name)
JOIN venues v ON lower(v.name) = lower(e.venue_name) AND lower(v.city) = 'bengaluru'
JOIN performers p ON lower(p.name) = lower(e.performer_name)
WHERE NOT EXISTS (
  SELECT 1 FROM events existing
  WHERE lower(existing.name) = lower(e.name)
    AND existing.starts_at = e.starts_at::timestamptz
);

-- Synthetic demo inventory only: not real seat maps, capacity, or sale prices.
INSERT INTO tickets (event_id, section, seat_number, price)
SELECT e.id, 'Demo General Admission', 'DEMO-' || seat_no, 1499.00
FROM events e
JOIN (VALUES
  ('The Philosophy of Resonance - Satinder Sartaaj Live'),
  ('Shreya Ghoshal - The Unstoppable India Tour'),
  ('Yuvan Shankar Raja - U1NIVERSE Tour'),
  ('Echoes of Earth 2026'),
  ('Sonu Nigam - The Revolution India Tour'),
  ('Sunburn Arena ft. DJ Snake'),
  ('Sunburn Arena ft. The Chainsmokers')
) AS seeded(event_name) ON lower(seeded.event_name) = lower(e.name)
CROSS JOIN generate_series(1, 20) AS seat_no
ON CONFLICT (event_id, section, seat_number) DO NOTHING;
