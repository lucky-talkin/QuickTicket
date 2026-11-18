# QuickTicket

Backend-only Ticketmaster system-design project. Small Go services, PostgreSQL, Redis, Kafka and mock payment.

## Services

| Service | Port | Responsibility |
|---|---:|---|
| Catalog | 8081 | CRUD users, venues, performers, events |
| Inventory | 8082 | Ticket CRUD, Redis seat holds and distributed locks |
| Booking | 8083 | Transactional booking and mock payment |
| Waiting room | 8084 | FIFO queue and bounded admission |
| Booking events | worker | Kafka consumer for booking events |

PostgreSQL is the durable source of truth. Redis provides expiring seat holds/locks and queue state. Kafka transports booking events. The mock payment is deliberately local and deterministic.

## Run

```bash
docker compose up
```

## CRUD

Catalog service:
- `GET/POST /users`, `GET/PUT/DELETE /users/{id}`
- Same pattern for `/venues`, `/performers`, and `/events`

Inventory service:
- Same CRUD pattern for `/tickets`
- `POST /holds` with `{"ticket_id":1,"user_id":1,"admission_token":"TOKEN_FROM_QUEUE"}`
- A hold lasts five minutes

Booking service:
- `POST /bookings` with `{"user_id":1,"ticket_ids":[1],"admission_token":"TOKEN_FROM_QUEUE","mock_payment_result":"success"}`
- Set mock_payment_result to `failure` to simulate a declined payment
- `GET /bookings`

Waiting room:
- `POST /queue/join` with `{"event_id":1,"user_id":1}`
- `POST /queue/status` with `{"event_id":1,"user_id":1}`
- Queue admission is automatic when a user joins or checks status and a slot is available.
- `QUEUE_CAPACITY` controls concurrent admitted users; admission tokens expire after `ADMISSION_TTL_SECONDS` (default 600).

## Example request flow\n\n1. Create user, venue, performer and event through Catalog.\n2. Create event tickets through Inventory.\n3. Join `POST /queue/join` and poll `POST /queue/status` until `status` is `admitted`. Copy `admission_token`.\n4. Hold the seat with `POST /holds`, passing `ticket_id`, `user_id` and `admission_token`.\n5. Create the booking with `POST /bookings`, passing the same token and `mock_payment_result` (`success` or `failure`).\n\n## Redis lock and hold

Inventory acquires a per-ticket Redis lock using SET NX with a random owner token and expiry. It then creates the short-lived hold. Lock release uses a Lua compare-and-delete operation, so one request cannot release another request's lock. Booking checks that the hold belongs to the requesting user and then uses a PostgreSQL transaction plus conditional ticket updates as the final consistency guard.

A Redis lock coordinates concurrent application requests; PostgreSQL constraints and transactions remain authoritative. A production system should also use fencing/lease renewal for long-running critical sections.

## Virtual waiting room

The waiting-room service stores an event-specific FIFO queue in Redis. It issues a token only when a user is admitted; both inventory holds and booking requests validate that token. Admission is bounded by `QUEUE_CAPACITY` concurrent users and produces a short-lived admission token (`ADMISSION_TTL_SECONDS`). The design illustrates controlled traffic release before users enter the booking flow. For production, admission tokens should be signed or stored with ownership, queue state should be resilient to Redis failover, and rate limits should be enforced at the gateway.

## Kafka

Booking publishes a booking event after the PostgreSQL transaction commits. The booking-events worker consumes the topic and logs the event. This demonstrates asynchronous service decoupling. There is a commit-to-publish failure window in this simple learning version; a transactional outbox is the next reliability improvement.

## Scope

No frontend, real payment provider, Python service, Kubernetes or extra infrastructure. Focus is CRUD, contention, Redis locks, FIFO admission, transaction boundaries and Kafka events.
