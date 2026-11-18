CREATE TABLE IF NOT EXISTS users (
 id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS venues (
 id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL, city TEXT NOT NULL,
 capacity INTEGER NOT NULL CHECK (capacity > 0)
);
CREATE TABLE IF NOT EXISTS performers (
 id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL, description TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS events (
 id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL, description TEXT NOT NULL DEFAULT '',
 event_type TEXT NOT NULL, starts_at TIMESTAMPTZ NOT NULL,
 venue_id BIGINT NOT NULL REFERENCES venues(id),
 performer_id BIGINT NOT NULL REFERENCES performers(id)
);
CREATE TABLE IF NOT EXISTS tickets (
 id BIGSERIAL PRIMARY KEY, event_id BIGINT NOT NULL REFERENCES events(id) ON DELETE CASCADE,
 section TEXT NOT NULL, seat_number TEXT NOT NULL,
 price NUMERIC(10,2) NOT NULL CHECK (price >= 0),
 status TEXT NOT NULL DEFAULT 'available' CHECK (status IN ('available','booked')),
 UNIQUE(event_id, section, seat_number)
);
CREATE TABLE IF NOT EXISTS bookings (
 id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES users(id),
 total NUMERIC(10,2) NOT NULL CHECK (total >= 0),
 status TEXT NOT NULL DEFAULT 'confirmed' CHECK (status IN ('confirmed','cancelled')),
 payment_status TEXT NOT NULL DEFAULT 'pending'
   CHECK (payment_status IN ('pending','succeeded','failed')),
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS booking_tickets (
 booking_id BIGINT NOT NULL REFERENCES bookings(id) ON DELETE CASCADE,
 ticket_id BIGINT NOT NULL REFERENCES tickets(id),
 PRIMARY KEY (booking_id,ticket_id)
);
CREATE INDEX IF NOT EXISTS idx_events_starts_at ON events(starts_at);
CREATE INDEX IF NOT EXISTS idx_tickets_event_status ON tickets(event_id,status);
