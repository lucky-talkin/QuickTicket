package main

import (
 "context"
 "encoding/json"
 "fmt"
 "log"
 "net/http"
 "os"
 "strconv"
 "strings"
 "time"

 "github.com/jackc/pgx/v5/pgxpool"
 "github.com/redis/go-redis/v9"
 "github.com/segmentio/kafka-go"
)
type API struct{db *pgxpool.Pool;r *redis.Client;writer *kafka.Writer}
type request struct{UserID int64 `json:"user_id"`;TicketIDs []int64 `json:"ticket_ids"`;Payment string `json:"mock_payment_result"`;AdmissionToken string `json:"admission_token"`}
func out(w http.ResponseWriter,c int,v any){w.Header().Set("Content-Type","application/json");w.WriteHeader(c);_=json.NewEncoder(w).Encode(v)}
func fail(w http.ResponseWriter,c int,s string){out(w,c,map[string]string{"error":s})}
var unlock=redis.NewScript(`if redis.call("GET",KEYS[1]) == ARGV[1] then return redis.call("DEL",KEYS[1]) else return 0 end`)
func(a *API)create(w http.ResponseWriter,r *http.Request){
 var in request;if json.NewDecoder(r.Body).Decode(&in)!=nil||in.UserID<1||len(in.TicketIDs)==0{fail(w,400,"user_id and ticket_ids required");return}
 if in.Payment==""{in.Payment="success"};if in.Payment!="success"&&in.Payment!="failure"{fail(w,400,"mock_payment_result must be success or failure");return}
 ctx:=r.Context();ids:=append([]int64{},in.TicketIDs...);seen:=map[int64]bool{};for _,id:=range ids{if id<1||seen[id]{fail(w,400,"ticket IDs must be positive and unique");return};seen[id]=true}
 // Acquire per-seat Redis locks in sorted order to avoid deadlocks across multi-seat bookings.
 sortIDs(ids);tokens:=map[int64]string{}
 for _,id:=range ids{key:=fmt.Sprintf("ticket:lock:%d",id);token:=fmt.Sprintf("booking-%d-%d",in.UserID,time.Now().UnixNano());ok,e:=a.r.SetNX(ctx,key,token,8*time.Second).Result();if e!=nil||!ok{for tid,t:=range tokens{_ = unlock.Run(ctx,a.r,[]string{fmt.Sprintf("ticket:lock:%d",tid)},t).Err()};fail(w,409,"ticket is being modified; retry");return};tokens[id]=token}
 defer func(){for id,t:=range tokens{_ = unlock.Run(context.Background(),a.r,[]string{fmt.Sprintf("ticket:lock:%d",id)},t).Err()}}()
 for _,id:=range ids{
  owner,e:=a.r.Get(ctx,fmt.Sprintf("ticket:hold:%d",id)).Result();if e!=nil||owner!=strconv.FormatInt(in.UserID,10){fail(w,409,"ticket hold missing or expired");return}
  var eventID int64;if e=a.db.QueryRow(ctx,"SELECT event_id FROM tickets WHERE id=$1",id).Scan(&eventID);e!=nil{fail(w,404,"ticket not found");return}
  admitted,e:=a.r.Get(ctx,fmt.Sprintf("event:%d:token:%d",eventID,in.UserID)).Result()
  if e!=nil||in.AdmissionToken==""||admitted!=in.AdmissionToken{fail(w,403,"valid waiting-room admission token required");return}
 }
 tx,e:=a.db.Begin(ctx);if e!=nil{fail(w,500,e.Error());return};defer tx.Rollback(ctx)
 var total float64
 for _,id:=range ids{var price float64;e=tx.QueryRow(ctx,"UPDATE tickets SET status='booked' WHERE id=$1 AND status='available' RETURNING price",id).Scan(&price);if e!=nil{fail(w,409,"ticket no longer available");return};total+=price}
 status,payment:="confirmed","succeeded";if in.Payment=="failure"{status,payment="cancelled","failed"}
 var bookingID int64;e=tx.QueryRow(ctx,"INSERT INTO bookings(user_id,total,status,payment_status) VALUES($1,$2,$3,$4) RETURNING id",in.UserID,total,status,payment).Scan(&bookingID);if e!=nil{fail(w,400,e.Error());return}
 for _,id:=range ids{if _,e=tx.Exec(ctx,"INSERT INTO booking_tickets(booking_id,ticket_id) VALUES($1,$2)",bookingID,id);e!=nil{fail(w,400,e.Error());return}}
 if payment=="failed"{for _,id:=range ids{if _,e=tx.Exec(ctx,"UPDATE tickets SET status='available' WHERE id=$1",id);e!=nil{fail(w,500,e.Error());return}}}
 if e=tx.Commit(ctx);e!=nil{fail(w,500,e.Error());return}
 for _,id:=range ids{_ = a.r.Del(ctx,fmt.Sprintf("ticket:hold:%d",id)).Err()}
 event:=map[string]any{"booking_id":bookingID,"user_id":in.UserID,"ticket_ids":ids,"total":total,"status":status,"payment_status":payment};payload,_:=json.Marshal(event)
 if e=a.writer.WriteMessages(ctx,kafka.Message{Key:[]byte(strconv.FormatInt(bookingID,10)),Value:payload});e!=nil{log.Printf("booking %d committed but event publish failed: %v",bookingID,e)}
 out(w,201,event)
}
func sortIDs(a []int64){for i:=0;i<len(a);i++{for j:=i+1;j<len(a);j++{if a[j]<a[i]{a[i],a[j]=a[j],a[i]}}}}
func(a *API)list(w http.ResponseWriter,r *http.Request){rows,e:=a.db.Query(r.Context(),"SELECT id,user_id,total,status,payment_status,created_at FROM bookings ORDER BY id");if e!=nil{fail(w,500,e.Error());return};defer rows.Close();res:=[]map[string]any{}
 for rows.Next(){var id,uid int64;var total float64;var status,payment string;var created time.Time;if e=rows.Scan(&id,&uid,&total,&status,&payment,&created);e!=nil{fail(w,500,e.Error());return};res=append(res,map[string]any{"id":id,"user_id":uid,"total":total,"status":status,"payment_status":payment,"created_at":created})};out(w,200,res)}
func main(){ctx:=context.Background();db,e:=pgxpool.New(ctx,os.Getenv("DATABASE_URL"));if e!=nil{log.Fatal(e)};defer db.Close();if e=db.Ping(ctx);e!=nil{log.Fatal(e)}
 addr:=os.Getenv("REDIS_ADDR");if addr==""{addr="redis:6379"};rc:=redis.NewClient(&redis.Options{Addr:addr});defer rc.Close();if e=rc.Ping(ctx).Err();e!=nil{log.Fatal(e)}
 brokers:=strings.Split(os.Getenv("KAFKA_BROKERS"),",");topic:=os.Getenv("KAFKA_TOPIC");if topic==""{topic="booking-events"};writer:=&kafka.Writer{Addr:kafka.TCP(brokers...),Topic:topic,Balancer:&kafka.Hash{},RequiredAcks:kafka.RequireAll};defer writer.Close()
 a:=&API{db,rc,writer};mux:=http.NewServeMux();mux.HandleFunc("/health",func(w http.ResponseWriter,r *http.Request){out(w,200,map[string]string{"status":"ok"})});mux.HandleFunc("/bookings",func(w http.ResponseWriter,r *http.Request){if r.Method==http.MethodPost{a.create(w,r)}else if r.Method==http.MethodGet{a.list(w,r)}else{fail(w,405,"method not allowed")}})
 port:=os.Getenv("PORT");if port==""{port="8083"};log.Fatal(http.ListenAndServe(":"+port,mux))
}