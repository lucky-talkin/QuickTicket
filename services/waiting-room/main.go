package main

import (
 "context"
 "encoding/json"
 "fmt"
 "log"
 "net/http"
 "os"
 "strconv"
 "time"

 "github.com/redis/go-redis/v9"
)
type API struct{r *redis.Client;capacity int;ttl time.Duration}
type input struct{EventID int64 `json:"event_id"`;UserID int64 `json:"user_id"`}
func out(w http.ResponseWriter,c int,v any){w.Header().Set("Content-Type","application/json");w.WriteHeader(c);_=json.NewEncoder(w).Encode(v)}
func fail(w http.ResponseWriter,c int,s string){out(w,c,map[string]string{"error":s})}
// One atomic Lua operation expires old admissions and advances FIFO users up to capacity.
var advance=redis.NewScript(`
local waiting=KEYS[1]; local admitted=KEYS[2]; local member=ARGV[1]
local now=tonumber(ARGV[2]); local capacity=tonumber(ARGV[3]); local ttl=tonumber(ARGV[4])
redis.call('ZREMRANGEBYSCORE',admitted,'-inf',now)
local current=redis.call('ZSCORE',admitted,member)
if current then return {1,0,tonumber(current)} end
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
func(a *API)enqueue(w http.ResponseWriter,r *http.Request){var in input;if json.NewDecoder(r.Body).Decode(&in)!=nil||in.EventID<1||in.UserID<1{fail(w,400,"event_id and user_id required");return}
 ctx:=r.Context();member:=strconv.FormatInt(in.UserID,10);waiting:=fmt.Sprintf("event:%d:waiting",in.EventID);admitted:=fmt.Sprintf("event:%d:admitted",in.EventID)
 // Redis sorted-set score is enqueue time; NX keeps a user's original FIFO position.
 _,e:=a.r.ZAddArgs(ctx,waiting,redis.ZAddArgs{NX:true,Members:[]redis.Z{{Score:float64(time.Now().UnixMilli()),Member:member}}}).Result();if e!=nil{fail(w,503,e.Error());return}
 result,e:=advance.Run(ctx,a.r,[]string{waiting,admitted},member,time.Now().UnixMilli(),a.capacity,a.ttl.Milliseconds()).Slice();if e!=nil{fail(w,503,e.Error());return}
 admittedNow:=result[0].(int64)==1;position:=result[1].(int64);response:=map[string]any{"event_id":in.EventID,"user_id":in.UserID,"status":"waiting","position":position}
 if admittedNow{response["status"]="admitted";response["admission_expires_in_seconds"]=int(a.ttl.Seconds())};out(w,200,response)
}
func(a *API)status(w http.ResponseWriter,r *http.Request){var in input;if json.NewDecoder(r.Body).Decode(&in)!=nil||in.EventID<1||in.UserID<1{fail(w,400,"event_id and user_id required");return};ctx:=r.Context();member:=strconv.FormatInt(in.UserID,10)
 waiting:=fmt.Sprintf("event:%d:waiting",in.EventID);admitted:=fmt.Sprintf("event:%d:admitted",in.EventID)
 result,e:=advance.Run(ctx,a.r,[]string{waiting,admitted},member,time.Now().UnixMilli(),a.capacity,a.ttl.Milliseconds()).Slice();if e!=nil{fail(w,503,e.Error());return}
 if result[0].(int64)==1{out(w,200,map[string]any{"status":"admitted","event_id":in.EventID,"user_id":in.UserID,"admission_expires_in_seconds":int(a.ttl.Seconds())});return}
 pos:=result[1].(int64);if pos<0{fail(w,404,"user is not in this event queue");return};out(w,200,map[string]any{"status":"waiting","event_id":in.EventID,"user_id":in.UserID,"position":pos})
}
func main(){addr:=os.Getenv("REDIS_ADDR");if addr==""{addr="redis:6379"};rc:=redis.NewClient(&redis.Options{Addr:addr});defer rc.Close();ctx:=context.Background();if e:=rc.Ping(ctx).Err();e!=nil{log.Fatal(e)}
 cap,_:=strconv.Atoi(os.Getenv("QUEUE_CAPACITY"));if cap<1{cap=100};seconds,_:=strconv.Atoi(os.Getenv("ADMISSION_TTL_SECONDS"));if seconds<1{seconds=600};a:=&API{rc,cap,time.Duration(seconds)*time.Second}
 mux:=http.NewServeMux();mux.HandleFunc("/health",func(w http.ResponseWriter,r *http.Request){out(w,200,map[string]string{"status":"ok"})});mux.HandleFunc("/queue/join",func(w http.ResponseWriter,r *http.Request){if r.Method!=http.MethodPost{fail(w,405,"method not allowed");return};a.enqueue(w,r)});mux.HandleFunc("/queue/status",func(w http.ResponseWriter,r *http.Request){if r.Method!=http.MethodPost{fail(w,405,"method not allowed");return};a.status(w,r)})
 port:=os.Getenv("PORT");if port==""{port="8084"};log.Fatal(http.ListenAndServe(":"+port,mux))
}