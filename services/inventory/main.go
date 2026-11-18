package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type API struct {
	db *pgxpool.Pool
	r  *redis.Client
}

func (a *API) tickets(w http.ResponseWriter, r *http.Request) {
	p := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	cols := []string{"id", "event_id", "section", "seat_number", "price", "status"}
	if len(p) == 1 && r.Method == http.MethodGet {
		rows, e := a.db.Query(r.Context(), "SELECT "+strings.Join(cols, ",")+" FROM tickets ORDER BY id")
		if e != nil {
			fail(w, 500, e.Error())
			return
		}
		defer rows.Close()
		result := []map[string]any{}
		for rows.Next() {
			v := make([]any, len(cols))
			ptr := make([]any, len(v))
			for i := range v {
				ptr[i] = &v[i]
			}
			if e = rows.Scan(ptr...); e != nil {
				fail(w, 500, e.Error())
				return
			}
			m := map[string]any{}
			for i, k := range cols {
				m[k] = v[i]
			}
			result = append(result, m)
		}
		out(w, 200, result)
		return
	}
	if len(p) == 1 && r.Method == http.MethodPost {
		var in struct {
			EventID    int64   `json:"event_id"`
			Section    string  `json:"section"`
			SeatNumber string  `json:"seat_number"`
			Price      float64 `json:"price"`
		}
		if json.NewDecoder(r.Body).Decode(&in) != nil || in.EventID < 1 || in.Section == "" || in.SeatNumber == "" || in.Price < 0 {
			fail(w, 400, "event_id, section, seat_number and non-negative price required")
			return
		}
		var id int64
		e := a.db.QueryRow(r.Context(), "INSERT INTO tickets(event_id,section,seat_number,price) VALUES($1,$2,$3,$4) RETURNING id", in.EventID, in.Section, in.SeatNumber, in.Price).Scan(&id)
		if e != nil {
			fail(w, 400, e.Error())
			return
		}
		out(w, 201, map[string]int64{"id": id})
		return
	}
	if len(p) == 2 {
		id, e := strconv.ParseInt(p[1], 10, 64)
		if e != nil {
			fail(w, 400, "invalid id")
			return
		}
		if r.Method == http.MethodGet {
			v := make([]any, len(cols))
			ptr := make([]any, len(v))
			for i := range v {
				ptr[i] = &v[i]
			}
			e = a.db.QueryRow(r.Context(), "SELECT "+strings.Join(cols, ",")+" FROM tickets WHERE id=$1", id).Scan(ptr...)
			if e != nil {
				if errors.Is(e, pgx.ErrNoRows) {
					fail(w, 404, "not found")
				} else {
					fail(w, 500, e.Error())
				}
				return
			}
			m := map[string]any{}
			for i, k := range cols {
				m[k] = v[i]
			}
			out(w, 200, m)
			return
		}
		if r.Method == http.MethodPut {
			var in struct {
				Section    *string  `json:"section"`
				SeatNumber *string  `json:"seat_number"`
				Price      *float64 `json:"price"`
			}
			if json.NewDecoder(r.Body).Decode(&in) != nil {
				fail(w, 400, "invalid JSON")
				return
			}
			sets, args := []string{}, []any{}
			if in.Section != nil {
				args = append(args, *in.Section)
				sets = append(sets, fmt.Sprintf("section=$%d", len(args)))
			}
			if in.SeatNumber != nil {
				args = append(args, *in.SeatNumber)
				sets = append(sets, fmt.Sprintf("seat_number=$%d", len(args)))
			}
			if in.Price != nil {
				args = append(args, *in.Price)
				sets = append(sets, fmt.Sprintf("price=$%d", len(args)))
			}
			if len(sets) == 0 {
				fail(w, 400, "no editable fields supplied")
				return
			}
			args = append(args, id)
			tag, e := a.db.Exec(r.Context(), "UPDATE tickets SET "+strings.Join(sets, ",")+fmt.Sprintf(" WHERE id=$%d", len(args)), args...)
			if e != nil {
				fail(w, 400, e.Error())
				return
			}
			if tag.RowsAffected() == 0 {
				fail(w, 404, "not found")
				return
			}
			out(w, 200, map[string]bool{"updated": true})
			return
		}
		if r.Method == http.MethodDelete {
			tag, e := a.db.Exec(r.Context(), "DELETE FROM tickets WHERE id=$1 AND status='available'", id)
			if e != nil {
				fail(w, 409, e.Error())
				return
			}
			if tag.RowsAffected() == 0 {
				fail(w, 409, "ticket not found or already booked")
				return
			}
			w.WriteHeader(204)
			return
		}
	}
	fail(w, 405, "method not allowed")
}

type holdRequest struct {
	TicketID       int64  `json:"ticket_id"`
	UserID         int64  `json:"user_id"`
	AdmissionToken string `json:"admission_token"`
}

func out(w http.ResponseWriter, c int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(c)
	_ = json.NewEncoder(w).Encode(v)
}
func fail(w http.ResponseWriter, c int, s string) { out(w, c, map[string]string{"error": s}) }

var unlock = redis.NewScript(`if redis.call("GET",KEYS[1]) == ARGV[1] then return redis.call("DEL",KEYS[1]) else return 0 end`)

func (a *API) hold(w http.ResponseWriter, r *http.Request) {
	var in holdRequest
	if json.NewDecoder(r.Body).Decode(&in) != nil || in.TicketID < 1 || in.UserID < 1 {
		fail(w, 400, "ticket_id and user_id required")
		return
	}
	ctx := r.Context()
	lockKey := fmt.Sprintf("ticket:lock:%d", in.TicketID)
	holdKey := fmt.Sprintf("ticket:hold:%d", in.TicketID)
	lockToken := fmt.Sprintf("%d-%d", in.UserID, time.Now().UnixNano())
	locked, e := a.r.SetNX(ctx, lockKey, lockToken, 4*time.Second).Result()
	if e != nil {
		fail(w, 503, "Redis unavailable")
		return
	}
	if !locked {
		fail(w, 409, "ticket operation in progress")
		return
	}
	defer unlock.Run(context.Background(), a.r, []string{lockKey}, lockToken)
	var available bool
	var eventID int64
	e = a.db.QueryRow(ctx, "SELECT status='available',event_id FROM tickets WHERE id=$1", in.TicketID).Scan(&available, &eventID)
	if e != nil || !available {
		fail(w, 409, "ticket is not available")
		return
	}
	admitted, e := a.r.Get(ctx, fmt.Sprintf("event:%d:token:%d", eventID, in.UserID)).Result()
	if e != nil || in.AdmissionToken == "" || admitted != in.AdmissionToken {
		fail(w, 403, "valid waiting-room admission token required")
		return
	}
	holdValue := strconv.FormatInt(in.UserID, 10)
	ok, e := a.r.SetNX(ctx, holdKey, holdValue, 5*time.Minute).Result()
	if e != nil {
		fail(w, 503, "Redis unavailable")
		return
	}
	if !ok {
		fail(w, 409, "ticket already held")
		return
	}
	out(w, 201, map[string]any{"ticket_id": in.TicketID, "user_id": in.UserID, "expires_in_seconds": 300})
}
func (a *API) release(w http.ResponseWriter, r *http.Request) {
	var in holdRequest
	if json.NewDecoder(r.Body).Decode(&in) != nil {
		fail(w, 400, "invalid JSON")
		return
	}
	ctx := r.Context()
	lockKey := fmt.Sprintf("ticket:lock:%d", in.TicketID)
	token := fmt.Sprintf("release-%d-%d", in.UserID, time.Now().UnixNano())
	ok, e := a.r.SetNX(ctx, lockKey, token, 4*time.Second).Result()
	if e != nil {
		fail(w, 503, e.Error())
		return
	}
	if !ok {
		fail(w, 409, "ticket operation in progress")
		return
	}
	defer unlock.Run(ctx, a.r, []string{lockKey}, token)
	holdKey := fmt.Sprintf("ticket:hold:%d", in.TicketID)
	owner, e := a.r.Get(ctx, holdKey).Result()
	if e != nil || owner != strconv.FormatInt(in.UserID, 10) {
		fail(w, 404, "hold not found for user")
		return
	}
	_ = a.r.Del(ctx, holdKey).Err()
	out(w, 200, map[string]bool{"released": true})
}
func main() {
	ctx := context.Background()
	db, e := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if e != nil {
		log.Fatal(e)
	}
	defer db.Close()
	if e = db.Ping(ctx); e != nil {
		log.Fatal(e)
	}
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "redis:6379"
	}
	rc := redis.NewClient(&redis.Options{Addr: addr})
	defer rc.Close()
	if e = rc.Ping(ctx).Err(); e != nil {
		log.Fatal(e)
	}
	a := &API{db, rc}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { out(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("/tickets", a.tickets)
	mux.HandleFunc("/tickets/", a.tickets)
	mux.HandleFunc("/holds", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			a.hold(w, r)
			return
		}
		if r.Method == http.MethodDelete {
			a.release(w, r)
			return
		}
		fail(w, 405, "method not allowed")
	})
	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
