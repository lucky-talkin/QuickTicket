-- Existing databases created before the ticket-rebooking change.
-- Keep failed/cancelled booking history while allowing a ticket to be sold again.
ALTER TABLE booking_tickets
  DROP CONSTRAINT IF EXISTS booking_tickets_ticket_id_key;
