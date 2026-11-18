package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/lucky-talkin/QuickTicket/services/waiting-room/internal/admission"
	"github.com/lucky-talkin/QuickTicket/services/waiting-room/internal/httpapi"
	"github.com/lucky-talkin/QuickTicket/services/waiting-room/internal/queue"
)

func main() {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" { addr = "redis:6379" }
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil { log.Fatal(err) }

	capacity, _ := strconv.Atoi(os.Getenv("QUEUE_CAPACITY"))
	if capacity < 1 { capacity = 100 }
	seconds, _ := strconv.Atoi(os.Getenv("ADMISSION_TTL_SECONDS"))
	if seconds < 1 { seconds = 600 }
	ttl := time.Duration(seconds) * time.Second

	q := queue.New(client, capacity, ttl)
	tokens := admission.New(client, ttl)
	api := httpapi.New(q, tokens)
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	api.Register(mux)

	port := os.Getenv("PORT")
	if port == "" { port = "8084" }
	log.Printf("waiting-room listening on :%s (capacity=%d, admission_ttl=%s)", port, capacity, ttl)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
