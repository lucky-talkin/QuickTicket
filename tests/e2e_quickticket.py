#!/usr/bin/env python3
"""End-to-end smoke test for the QuickTicket local Docker Compose stack.

Uses only the Python standard library. Run from the repository root after
starting the stack and applying the required database migrations.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any


def request_json(base_url: str, method: str, path: str,
                 body: dict[str, Any] | None = None,
                 expected: int = 200) -> dict[str, Any] | list[Any] | None:
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        base_url.rstrip("/") + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            status = response.status
            raw = response.read()
    except urllib.error.HTTPError as exc:
        status = exc.code
        raw = exc.read()
    except urllib.error.URLError as exc:
        raise AssertionError(f"{method} {path}: service unreachable: {exc}") from exc

    if status != expected:
        raise AssertionError(
            f"{method} {path}: expected HTTP {expected}, got {status}: "
            f"{raw.decode('utf-8', errors='replace')}"
        )
    if status == 204 or not raw:
        return None
    return json.loads(raw.decode("utf-8"))


def wait_for_health(urls: list[str], timeout: int = 90) -> None:
    deadline = time.time() + timeout
    pending = list(urls)
    while pending and time.time() < deadline:
        for url in pending[:]:
            try:
                request_json(url, "GET", "/health")
                pending.remove(url)
            except (AssertionError, json.JSONDecodeError):
                pass
        if pending:
            time.sleep(2)
    if pending:
        raise AssertionError("Services did not become healthy: " + ", ".join(pending))


def create_resource(base: str, resource: str, payload: dict[str, Any]) -> int:
    result = request_json(base, "POST", f"/{resource}", payload, expected=201)
    assert isinstance(result, dict) and "id" in result, f"Unexpected {resource} response: {result}"
    return int(result["id"])


def assert_ticket_status(inventory: str, ticket_id: int, expected: str) -> None:
    ticket = request_json(inventory, "GET", f"/tickets/{ticket_id}")
    assert isinstance(ticket, dict), f"Unexpected ticket response: {ticket}"
    actual = ticket.get("status")
    assert actual == expected, (
        f"Ticket {ticket_id}: expected status {expected!r}, got {actual!r}"
    )


def verify_outbox(booking_ids: list[int], timeout: int = 20) -> None:
    """Optional local Docker/psql check for committed and published outbox rows."""
    ids = ",".join(str(value) for value in booking_ids)
    query = (
        "SELECT count(*) || ':' || count(*) FILTER (WHERE published_at IS NOT NULL) "
        f"FROM outbox_events WHERE aggregate_type='booking' AND aggregate_id IN ({ids});"
    )
    deadline = time.time() + timeout
    last_output = ""
    while time.time() < deadline:
        result = subprocess.run(
            ["docker", "compose", "exec", "-T", "postgres", "psql",
             "-U", "quickticket", "-d", "quickticket", "-Atc", query],
            capture_output=True, text=True, check=False,
        )
        if result.returncode == 0:
            last_output = result.stdout.strip()
            if last_output:
                total, published = (int(part) for part in last_output.split(":"))
                if total == len(booking_ids) and published == len(booking_ids):
                    print("PASS outbox: all booking events were published")
                    return
        time.sleep(1)
    raise AssertionError(
        "Outbox rows were not all published before timeout. "
        f"Last psql output: {last_output!r}. "
        "Check booking and Kafka logs, and confirm migration 003 was applied."
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run QuickTicket end-to-end tests")
    parser.add_argument("--catalog", default="http://localhost:8081")
    parser.add_argument("--inventory", default="http://localhost:8082")
    parser.add_argument("--booking", default="http://localhost:8083")
    parser.add_argument("--waiting-room", default="http://localhost:8084")
    parser.add_argument(
        "--skip-outbox-check", action="store_true",
        help="Skip the Docker/psql assertion for Kafka outbox publication",
    )
    args = parser.parse_args()

    catalog = args.catalog
    inventory = args.inventory
    booking = args.booking
    waiting = args.waiting_room

    print("Waiting for QuickTicket services...")
    wait_for_health([catalog, inventory, booking, waiting])
    print("PASS health checks")

    # Create an isolated event per run so old queue entries/admissions do not
    # consume this test's waiting-room capacity.
    stamp = int(time.time())
    user_one = create_resource(catalog, "users", {
        "name": "E2E Buyer One", "email": f"e2e-one-{stamp}@example.test"
    })
    user_two = create_resource(catalog, "users", {
        "name": "E2E Buyer Two", "email": f"e2e-two-{stamp}@example.test"
    })
    venue_id = create_resource(catalog, "venues", {
        "name": f"E2E Venue {stamp}", "city": "Bengaluru", "capacity": 100
    })
    performer_id = create_resource(catalog, "performers", {
        "name": f"E2E Performer {stamp}", "description": "Automated test performer"
    })
    starts_at = (datetime.now(timezone.utc) + timedelta(days=2)).isoformat()
    event_id = create_resource(catalog, "events", {
        "name": f"QuickTicket E2E Event {stamp}",
        "description": "Created by the automated end-to-end test",
        "event_type": "concert",
        "starts_at": starts_at,
        "venue_id": venue_id,
        "performer_id": performer_id,
    })
    print("PASS catalog setup")

    ticket_one = create_resource(inventory, "tickets", {
        "event_id": event_id, "section": "E2E", "seat_number": "E2E-01", "price": 100
    })
    ticket_two = create_resource(inventory, "tickets", {
        "event_id": event_id, "section": "E2E", "seat_number": "E2E-02", "price": 125
    })
    print("PASS ticket creation")

    def join(user_id: int) -> str:
        result = request_json(waiting, "POST", "/queue/join", {
            "event_id": event_id, "user_id": user_id
        }, expected=200)
        assert isinstance(result, dict) and result.get("status") == "admitted", (
            f"Expected immediate admission for user {user_id}; got {result}. "
            "Check QUEUE_CAPACITY and Redis state."
        )
        token = result.get("admission_token")
        assert token, f"Admission token missing: {result}"
        return str(token)

    token_one = join(user_one)
    token_two = join(user_two)
    print("PASS waiting-room admission for both users")

    hold = request_json(inventory, "POST", "/holds", {
        "ticket_id": ticket_one, "user_id": user_one, "admission_token": token_one
    }, expected=201)
    assert isinstance(hold, dict) and hold.get("ticket_id") == ticket_one

    # A second user must not be able to hold a ticket already held by buyer one.
    request_json(inventory, "POST", "/holds", {
        "ticket_id": ticket_one, "user_id": user_two, "admission_token": token_two
    }, expected=409)
    print("PASS exclusive seat hold (second buyer receives HTTP 409)")

    success = request_json(booking, "POST", "/bookings", {
        "user_id": user_one,
        "ticket_ids": [ticket_one],
        "admission_token": token_one,
        "mock_payment_result": "success",
    }, expected=201)
    assert isinstance(success, dict)
    assert success.get("status") == "confirmed"
    assert success.get("payment_status") == "succeeded"
    booking_one = int(success["booking_id"])
    assert_ticket_status(inventory, ticket_one, "booked")
    print(f"PASS successful booking #{booking_one}; ticket is booked")

    # The second seat exercises the declined-payment rollback path.
    request_json(inventory, "POST", "/holds", {
        "ticket_id": ticket_two, "user_id": user_two, "admission_token": token_two
    }, expected=201)
    failure = request_json(booking, "POST", "/bookings", {
        "user_id": user_two,
        "ticket_ids": [ticket_two],
        "admission_token": token_two,
        "mock_payment_result": "failure",
    }, expected=201)
    assert isinstance(failure, dict)
    assert failure.get("status") == "cancelled"
    assert failure.get("payment_status") == "failed"
    booking_two = int(failure["booking_id"])
    assert_ticket_status(inventory, ticket_two, "available")
    print(f"PASS failed payment #{booking_two}; ticket returned to available")

    bookings = request_json(booking, "GET", "/bookings")
    assert isinstance(bookings, list)
    returned_ids = {int(item["id"]) for item in bookings if "id" in item}
    # The list endpoint's response shape may expose booking_id rather than id.
    returned_ids.update(
        int(item["booking_id"]) for item in bookings if "booking_id" in item
    )
    assert {booking_one, booking_two}.issubset(returned_ids), (
        f"Created bookings missing from GET /bookings: {bookings}"
    )
    print("PASS booking history")

    if not args.skip_outbox_check:
        verify_outbox([booking_one, booking_two])

    print("\nALL QUICKTICKET E2E CHECKS PASSED")
    print(f"event_id={event_id}, users={user_one},{user_two}, bookings={booking_one},{booking_two}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (AssertionError, KeyError, ValueError, json.JSONDecodeError) as exc:
        print(f"\nE2E TEST FAILED: {exc}", file=sys.stderr)
        raise SystemExit(1)
