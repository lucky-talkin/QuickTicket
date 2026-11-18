package admission

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Service struct {
	redis *redis.Client
	ttl time.Duration
}

func New(client *redis.Client, ttl time.Duration) *Service {
	return &Service{redis: client, ttl: ttl}
}

// GetOrCreate is safe under concurrent polls: SETNX chooses one token,
// and every caller reads back the winning token.
func (s *Service) GetOrCreate(ctx context.Context, eventID, userID int64) (string, error) {
	key := fmt.Sprintf("event:%d:token:%d", eventID, userID)
	token, err := s.redis.Get(ctx, key).Result()
	if err == nil { return token, nil }
	if err != redis.Nil { return "", err }
	b := make([]byte, 24)
	if _, err = rand.Read(b); err != nil { return "", err }
	candidate := hex.EncodeToString(b)
	if err = s.redis.SetNX(ctx, key, candidate, s.ttl).Err(); err != nil { return "", err }
	return s.redis.Get(ctx, key).Result()
}
