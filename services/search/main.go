package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type Event struct {
	ID int64 `json:"id"`
	Name string `json:"name"`
	Description string `json:"description"`
	EventType string `json:"event_type"`
	StartsAt time.Time `json:"starts_at"`
	VenueID int64 `json:"venue_id"`
	PerformerID int64 `json:"performer_id"`
}

type DebeziumMessage struct {
	Payload struct {
		Op string `json:"op"`
		Before *Event `json:"before"`
		After *Event `json:"after"`
	} `json:"payload"`
}

type SearchAPI struct { elasticURL, index string; client *http.Client }

func (s *SearchAPI) request(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var reader *bytes.Reader
	if body != nil { b, err := json.Marshal(body); if err != nil { return nil, err }; reader = bytes.NewReader(b) } else { reader = bytes.NewReader(nil) }
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(s.elasticURL, "/")+path, reader)
	if err != nil { return nil, err }
	if body != nil { req.Header.Set("Content-Type", "application/json") }
	return s.client.Do(req)
}

func (s *SearchAPI) indexEvent(ctx context.Context, e Event) error {
	path := fmt.Sprintf("/%s/_doc/%d", s.index, e.ID)
	resp, err := s.request(ctx, http.MethodPut, path, e)
	if err != nil { return err }; defer resp.Body.Close()
	if resp.StatusCode >= 300 { return fmt.Errorf("elasticsearch index returned %s", resp.Status) }
	return nil
}
func (s *SearchAPI) deleteEvent(ctx context.Context, id int64) error {
	resp, err := s.request(ctx, http.MethodDelete, fmt.Sprintf("/%s/_doc/%d", s.index, id), nil)
	if err != nil { return err }; defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusNotFound { return fmt.Errorf("elasticsearch delete returned %s", resp.Status) }
	return nil
}

func (s *SearchAPI) consume(ctx context.Context, brokers, topic, group string) {
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: strings.Split(brokers, ","), Topic: topic, GroupID: group, MinBytes: 1, MaxBytes: 10e6})
	defer r.Close()
	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil { if ctx.Err() != nil { return }; log.Printf("read CDC message: %v", err); time.Sleep(time.Second); continue }
		var envelope DebeziumMessage
		if err := json.Unmarshal(msg.Value, &envelope); err != nil { log.Printf("decode CDC message: %v", err); continue }
		p := envelope.Payload
		if p.Op == "d" && p.Before != nil {
			err = s.deleteEvent(ctx, p.Before.ID)
		} else if p.After != nil {
			err = s.indexEvent(ctx, *p.After)
		}
		if err != nil { log.Printf("index CDC offset %d: %v", msg.Offset, err); continue }
	}
}

func (s *SearchAPI) search(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	page, _ := strconv.Atoi(q.Get("page")); if page < 1 { page = 1 }
	size, _ := strconv.Atoi(q.Get("size")); if size < 1 || size > 100 { size = 20 }
	must := []any{}
	if term := strings.TrimSpace(q.Get("q")); term != "" {
		must = append(must, map[string]any{"multi_match": map[string]any{"query": term, "fields": []string{"name^3", "description", "event_type"}}})
	}
	filter := []any{}
	if typ := q.Get("type"); typ != "" { filter = append(filter, map[string]any{"term": map[string]any{"event_type.keyword": typ}}) }
	if venue := q.Get("venue_id"); venue != "" { filter = append(filter, map[string]any{"term": map[string]any{"venue_id": venue}}) }
	if from, to := q.Get("from"), q.Get("to"); from != "" || to != "" {
		rng := map[string]any{}; if from != "" { rng["gte"] = from }; if to != "" { rng["lte"] = to }
		filter = append(filter, map[string]any{"range": map[string]any{"starts_at": rng}})
	}
	body := map[string]any{"from": (page-1)*size, "size": size, "sort": []any{map[string]any{"_score": "desc"}, map[string]any{"starts_at": "asc"}}, "query": map[string]any{"bool": map[string]any{"must": must, "filter": filter}}}
	if len(must) == 0 { body["query"] = map[string]any{"bool": map[string]any{"filter": filter}} }
	resp, err := s.request(r.Context(), http.MethodPost, "/"+s.index+"/_search", body)
	if err != nil { http.Error(w, "search backend unavailable", http.StatusBadGateway); return }
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func main() {
	elasticURL := os.Getenv("ELASTICSEARCH_URL"); if elasticURL == "" { elasticURL = "http://elasticsearch:9200" }
	index := os.Getenv("ELASTICSEARCH_INDEX"); if index == "" { index = "quickticket-events" }
	api := &SearchAPI{elasticURL: elasticURL, index: index, client: &http.Client{Timeout: 5*time.Second}}
	ctx, cancel := context.WithCancel(context.Background()); defer cancel()
	// Create the index with explicit mappings before consuming the initial CDC snapshot.
	mapping := map[string]any{"mappings": map[string]any{"properties": map[string]any{
		"id": map[string]any{"type":"long"}, "name": map[string]any{"type":"text","fields":map[string]any{"keyword":map[string]any{"type":"keyword"}}},
		"description": map[string]any{"type":"text"}, "event_type": map[string]any{"type":"keyword"}, "starts_at": map[string]any{"type":"date"},
		"venue_id": map[string]any{"type":"long"}, "performer_id": map[string]any{"type":"long"},
	}}}
	for i:=0; i<30; i++ {
		resp, err := api.request(ctx, http.MethodPut, "/"+url.PathEscape(index), mapping)
		if err == nil { resp.Body.Close(); if resp.StatusCode < 300 || resp.StatusCode == http.StatusBadRequest { break } }
		time.Sleep(time.Second)
	}
	brokers := os.Getenv("KAFKA_BROKERS"); if brokers == "" { brokers = "kafka:9092" }
	topic := os.Getenv("CDC_TOPIC"); if topic == "" { topic = "quickticket.public.events" }
	group := os.Getenv("CDC_GROUP"); if group == "" { group = "quickticket-search-indexer" }
	go api.consume(ctx, brokers, topic, group)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		resp, err := api.request(r.Context(), http.MethodGet, "/", nil)
		if err != nil || resp.StatusCode >= 300 { http.Error(w, "elasticsearch unavailable", 503); return }
		resp.Body.Close(); w.Header().Set("Content-Type","application/json"); _, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("GET /search", api.search)
	port := os.Getenv("PORT"); if port == "" { port = "8085" }
	log.Printf("search service listening on :%s; CDC topic=%s", port, topic)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
