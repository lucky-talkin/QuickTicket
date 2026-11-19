# Search service and PostgreSQL CDC

QuickTicket Search is a read-only query service backed by Elasticsearch. PostgreSQL remains the source of truth; the search index is a rebuildable, eventually consistent projection.

## Data flow

```text
Catalog writes events
        |
        v
PostgreSQL WAL (logical replication)
        |
        v
Debezium PostgreSQL connector
        |
        v
Kafka topic: quickticket.public.events
        |
        v
Search service CDC consumer
        |
        v
Elasticsearch index: quickticket-events
        |
        v
GET /search
```

The connector takes an initial snapshot and then streams inserts, updates, and deletes from the PostgreSQL WAL. The Go consumer unwraps Debezium's `payload.before`, `payload.after`, and `payload.op`; event IDs are Elasticsearch document IDs, making retries idempotent. Delete operations remove the corresponding document.

## Search API

Base URL: `http://localhost:8085`

- `GET /health`
- `GET /search?q=rock&type=concert&from=2027-01-01T00:00:00Z&to=2027-12-31T23:59:59Z&page=1&size=20`
- Optional exact filter: `venue_id=1`

The indexed document currently includes event ID, name, description, type, start time, venue ID, and performer ID. Venue and performer names are intentionally not duplicated yet; extend the connector to their tables and maintain a denormalized projection if name-based venue/performer search is required.

## Local run

```bash
docker compose up --build -d
curl 'http://localhost:8085/search?q=music'
```

Debezium Connect registers the connector automatically via the one-shot registration container. Inspect connector state:

```bash
curl http://localhost:8086/connectors/quickticket-postgres/status
docker compose logs -f debezium search
```

For an existing PostgreSQL volume, Compose starts PostgreSQL with logical WAL enabled, but changing `wal_level` requires a PostgreSQL restart. The Compose service applies the setting on container startup.

## Operational notes

- CDC and search are asynchronous; a successful Catalog write does not mean the document is immediately searchable.
- Kafka consumer offsets are committed after successful indexing by kafka-go's `ReadMessage` behavior. For strict failure handling, switch to `FetchMessage` and explicitly `CommitMessages` only after Elasticsearch acknowledges the write, plus add retry/DLQ handling.
- This starter consumes the event table only. It does not index ticket availability or booking state; those are volatile transactional data and should not be used to decide whether a seat can be booked.
- Rebuild by deleting the Elasticsearch index and restarting with a fresh consumer group or resetting its offsets so Debezium's snapshot is replayed. In production, use versioned indices and aliases.
- Elasticsearch is exposed without authentication for local development only.
