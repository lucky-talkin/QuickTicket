package queue

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type Result struct {
	Admitted bool
	Position int64
}

type Service struct {
	redis *redis.Client
	capacity int
	ttl time.Duration
}

func New(client *redis.Client, capacity int, ttl time.Duration) *Service {
	return &Service{redis: client, capacity: capacity, ttl: ttl}
}

// The script expires old admissions, fills available slots in FIFO order,
// and calculates the caller's status in one atomic Redis operation.
var advance = redis.NewScript(`
local waiting=KEYS[1]; local admitted=KEYS[2]; local member=ARGV[1]
local now=tonumber(ARGV[2]); local capacity=tonumber(ARGV[3]); local ttl=tonumber(ARGV[4])
redis.call('ZREMRANGEBYSCORE',admitted,'-inf',now)
local current=redis.call('ZSCORE',admitted,member)
if current then
 redis.call('ZREM',waiting,member)
 return {1,0,tonumber(current)}
end
local active=redis.call('ZCARD',admitted)
while active < capacity do
 local next=redis.call('ZRANGE',waiting,0,0)
 if #next == 0 then break end
 redis.call('ZREM',waiting,next[1])
 redis.call('ZADD',admitted,now+ttl,next[1])
 active=active+1
end
current=redis.call('ZSCORE',admitted,member)
if current then return {1,0,tonumber(current)} end
local rank=redis.call('ZRANK',waiting,member)
if not rank then return {0,-1,0} end
return {0,rank+1,0}
`)

func (s *Service) Join(ctx context.Context, eventID, userID int64) (Result, error) {
	waiting, admitted, member := keys(eventID, userID)
	_, err := s.redis.ZAddArgs(ctx, waiting, redis.ZAddArgs{
		NX: true,
		Members: []redis.Z{{Score: float64(time.Now().UnixMilli()), Member: member}},
	}).Result()
	if err != nil { return Result{}, err }
	return s.advance(ctx, waiting, admitted, member)
}

func (s *Service) Status(ctx context.Context, eventID, userID int64) (Result, error) {
	waiting, admitted, member := keys(eventID, userID)
	return s.advance(ctx, waiting, admitted, member)
}

func (s *Service) advance(ctx context.Context, waiting, admitted, member string) (Result, error) {
	values, err := advance.Run(ctx, s.redis, []string{waiting, admitted}, member,
		time.Now().UnixMilli(), s.capacity, s.ttl.Milliseconds()).Slice()
	if err != nil { return Result{}, err }
	admittedNow := values[0].(int64) == 1
	return Result{Admitted: admittedNow, Position: values[1].(int64)}, nil
}

func (s *Service) AdmissionTTLSeconds() int { return int(s.ttl.Seconds()) }

func keys(eventID, userID int64) (string, string, string) {
	return fmt.Sprintf("event:%d:waiting", eventID),
		fmt.Sprintf("event:%d:admitted", eventID),
		strconv.FormatInt(userID, 10)
}
