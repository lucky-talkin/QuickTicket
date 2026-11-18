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
 "github.com/segmentio/kafka-go"
)

type App struct { db *pgxpool.Pool; redis *redis.Client; kafka *kafka.Writer }
type Resource struct { table string; fields []string }

var resources = map[string]Resource{
 "users":{"users",[]string{"name","email"}},
 "venues":{"venues",[]string{"name","city","capacity"}},
 "performers":{"performers",[]string{"name","description"}},
 "events":{"events",[]string{"name","description","event_type","starts_at","venue_id","performer_id"}},
 "tickets":{"tickets",[]string{"event_id","section","seat_number","price","status"}},
}

func jsonOut(w http.ResponseWriter, code int, v any) { w.Header().Set("Content-Type","application/json"); w.WriteHeader(code); _=json.NewEncoder(w).Encode(v) }
func problem(w http.ResponseWriter, code int, msg string) { jsonOut(w,code,map[string]string{"error":msg}) }
func decode(r *http.Request, v any) error { return json.NewDecoder(r.Body).Decode(v) }

func (a *App) crud(w http.ResponseWriter,r *http.Request) {
 p:=strings.Split(strings.Trim(r.URL.Path,"/"),"/"); if len(p)<2 { problem(w,404,"not found"); return }
 cfg,ok:=resources[p[1]]; if !ok { problem(w,404,"unknown resource"); return }
 cols:=append([]string{"id"},cfg.fields...)
 if len(p)==2 && r.Method==http.MethodGet {
  rows,err:=a.db.Query(r.Context(),"SELECT "+strings.Join(cols,",")+" FROM "+cfg.table+" ORDER BY id")
  if err!=nil { problem(w,500,err.Error()); return }; defer rows.Close()
  out:=[]map[string]any{}
  for rows.Next(){ vals:=make([]any,len(cols)); ptr:=make([]any,len(cols)); for i:=range vals {ptr[i]=&vals[i]}
   if err:=rows.Scan(ptr...);err!=nil {problem(w,500,err.Error());return}; m:=map[string]any{};for i,c:=range cols {m[c]=vals[i]};out=append(out,m) }
  jsonOut(w,200,out);return
 }
 if len(p)==2 && r.Method==http.MethodPost {
  body:=map[string]any{};if decode(r,&body)!=nil {problem(w,400,"invalid JSON");return}
  fields,args:=[]string{},[]any{};for _,f:=range cfg.fields {if v,ok:=body[f];ok {fields=append(fields,f);args=append(args,v)}}
  if len(fields)==0 {problem(w,400,"no supported fields");return}
  marks:=[]string{};for i:=range args {marks=append(marks,fmt.Sprintf("$%d",i+1))}
  var id int64;q:=fmt.Sprintf("INSERT INTO %s(%s) VALUES(%s) RETURNING id",cfg.table,strings.Join(fields,","),strings.Join(marks,","))
  if err:=a.db.QueryRow(r.Context(),q,args...).Scan(&id);err!=nil {problem(w,400,err.Error());return};jsonOut(w,201,map[string]int64{"id":id});return
 }
 if len(p)==3 {
  id,err:=strconv.ParseInt(p[2],10,64);if err!=nil {problem(w,400,"invalid id");return}
  if r.Method==http.MethodGet {
   vals:=make([]any,len(cols));ptr:=make([]any,len(cols));for i:=range vals {ptr[i]=&vals[i]}
   q:=fmt.Sprintf("SELECT %s FROM %s WHERE id=$1",strings.Join(cols,","),cfg.table)
   if err:=a.db.QueryRow(r.Context(),q,id).Scan(ptr...);err!=nil {if errors.Is(err,pgx.ErrNoRows){problem(w,404,"not found")}else{problem(w,500,err.Error())};return}
   m:=map[string]any{};for i,c:=range cols {m[c]=vals[i]};jsonOut(w,200,m);return
  }
  if r.Method==http.MethodPut {
   body:=map[string]any{};if decode(r,&body)!=nil {problem(w,400,"invalid JSON");return}
   sets,args:=[]string{},[]any{};for _,f:=range cfg.fields {if v,ok:=body[f];ok {args=append(args,v);sets=append(sets,fmt.Sprintf("%s=$%d",f,len(args)))}}
   if len(sets)==0 {problem(w,400,"no supported fields");return};args=append(args,id)
   tag,err:=a.db.Exec(r.Context(),fmt.Sprintf("UPDATE %s SET %s WHERE id=$%d",cfg.table,strings.Join(sets,","),len(args)),args...)
   if err!=nil {problem(w,400,err.Error());return};if tag.RowsAffected()==0 {problem(w,404,"not found");return};jsonOut(w,200,map[string]bool{"updated":true});return
  }
  if r.Method==http.MethodDelete {
   tag,err:=a.db.Exec(r.Context(),"DELETE FROM "+cfg.table+" WHERE id=$1",id);if err!=nil {problem(w,409,err.Error());return}
   if tag.RowsAffected()==0 {problem(w,404,"not found");return};w.WriteHeader(204);return
  }
 }
 problem(w,405,"method not allowed")
}

// POST /holds: Redis SET NX reserves a seat temporarily. The TTL releases abandoned seats.
func (a *App) hold(w http.ResponseWriter,r *http.Request) {
 var in struct{ TicketID int64 `json:"ticket_id"`; UserID int64 `json:"user_id"` }
 if decode(r,&in)!=nil || in.TicketID<=0 || in.UserID<=0 {problem(w,400,"ticket_id and user_id are required");return}
 var available bool
 err:=a.db.QueryRow(r.Context(),"SELECT status='available' FROM tickets WHERE id=$1",in.TicketID).Scan(&available)
 if err!=nil || !available {problem(w,409,"ticket is not available");return}
 key:=fmt.Sprintf("ticket:hold:%d",in.TicketID)
 ok,err:=a.redis.SetNX(r.Context(),key,strconv.FormatInt(in.UserID,10),5*time.Minute).Result()
 if err!=nil {problem(w,503,"Redis unavailable");return};if !ok {problem(w,409,"ticket is already held");return}
 jsonOut(w,201,map[string]any{"ticket_id":in.TicketID,"expires_in_seconds":300})
}

func (a *App) createBooking(w http.ResponseWriter,r *http.Request) {
 var in struct{UserID int64 `json:"user_id"`;TicketIDs []int64 `json:"ticket_ids"`;PaymentResult string `json:"mock_payment_result"`}
 if decode(r,&in)!=nil || in.UserID<=0 || len(in.TicketIDs)==0 {problem(w,400,"user_id and ticket_ids are required");return}
 if in.PaymentResult=="" {in.PaymentResult="success"};if in.PaymentResult!="success" && in.PaymentResult!="failure" {problem(w,400,"mock_payment_result must be success or failure");return}
 ctx:=r.Context()
 // Verify Redis holds belong to this user before committing ticket inventory.
 for _,id:=range in.TicketIDs { owner,err:=a.redis.Get(ctx,fmt.Sprintf("ticket:hold:%d",id)).Result();if err!=nil || owner!=strconv.FormatInt(in.UserID,10) {problem(w,409,"ticket hold missing or expired");return} }
 tx,err:=a.db.Begin(ctx);if err!=nil {problem(w,500,err.Error());return};defer tx.Rollback(ctx)
 var total float64
 for _,id:=range in.TicketIDs {var price float64
  err=tx.QueryRow(ctx,"UPDATE tickets SET status='booked' WHERE id=$1 AND status='available' RETURNING price",id).Scan(&price)
  if err!=nil {problem(w,409,"ticket no longer available");return};total+=price
 }
 payment:="succeeded";status:="confirmed"
 if in.PaymentResult=="failure" {payment="failed";status="cancelled"}
 var bookingID int64
 err=tx.QueryRow(ctx,"INSERT INTO bookings(user_id,total,status,payment_status) VALUES($1,$2,$3,$4) RETURNING id",in.UserID,total,status,payment).Scan(&bookingID)
 if err!=nil {problem(w,400,err.Error());return}
 for _,id:=range in.TicketIDs {
  if _,err=tx.Exec(ctx,"INSERT INTO booking_tickets(booking_id,ticket_id) VALUES($1,$2)",bookingID,id);err!=nil {problem(w,400,err.Error());return}
 }
 // Failed mock payment releases tickets within the same transaction.
 if payment=="failed" {for _,id:=range in.TicketIDs {if _,err=tx.Exec(ctx,"UPDATE tickets SET status='available' WHERE id=$1",id);err!=nil {problem(w,500,err.Error());return}}}
 if err=tx.Commit(ctx);err!=nil {problem(w,500,err.Error());return}
 for _,id:=range in.TicketIDs {_=a.redis.Del(ctx,fmt.Sprintf("ticket:hold:%d",id)).Err()}
 event:=map[string]any{"booking_id":bookingID,"user_id":in.UserID,"total":total,"status":status,"payment_status":payment}
 payload,_:=json.Marshal(event)
 // Publish only after the database commit. A production system should use a transactional outbox.
 if err:=a.kafka.WriteMessages(ctx,kafka.Message{Key:[]byte(strconv.FormatInt(bookingID,10)),Value:payload});err!=nil {log.Printf("booking %d committed; Kafka publish failed: %v",bookingID,err)}
 jsonOut(w,201,event)
}

func (a *App) listBookings(w http.ResponseWriter,r *http.Request) {
 rows,err:=a.db.Query(r.Context(),"SELECT id,user_id,total,status,payment_status,created_at FROM bookings ORDER BY id")
 if err!=nil {problem(w,500,err.Error());return};defer rows.Close()
 out:=[]map[string]any{}
 for rows.Next(){var id,uid int64;var total float64;var status,payment string;var created time.Time
  if err:=rows.Scan(&id,&uid,&total,&status,&payment,&created);err!=nil {problem(w,500,err.Error());return}
  out=append(out,map[string]any{"id":id,"user_id":uid,"total":total,"status":status,"payment_status":payment,"created_at":created})}
 jsonOut(w,200,out)
}

func main(){
 ctx:=context.Background()
 dsn:=os.Getenv("DATABASE_URL");if dsn=="" {dsn="postgres://quickticket:quickticket@localhost:5432/quickticket?sslmode=disable"}
 db,err:=pgxpool.New(ctx,dsn);if err!=nil {log.Fatal(err)};defer db.Close();if err=db.Ping(ctx);err!=nil {log.Fatal(err)}
 redisAddr:=os.Getenv("REDIS_ADDR");if redisAddr=="" {redisAddr="localhost:6379"}
 rc:=redis.NewClient(&redis.Options{Addr:redisAddr});defer rc.Close();if err=rc.Ping(ctx).Err();err!=nil {log.Fatal(err)}
 brokers:=strings.Split(env("KAFKA_BROKERS","localhost:9092"),",")
 writer:=&kafka.Writer{Addr:kafka.TCP(brokers...),Topic:env("KAFKA_TOPIC","booking-events"),Balancer:&kafka.Hash{},RequiredAcks:kafka.RequireAll};defer writer.Close()
 a:=&App{db:db,redis:rc,kafka:writer}
 mux:=http.NewServeMux()
 mux.HandleFunc("/health",func(w http.ResponseWriter,r *http.Request){jsonOut(w,200,map[string]string{"status":"ok"})})
 mux.HandleFunc("/holds",func(w http.ResponseWriter,r *http.Request){if r.Method!=http.MethodPost {problem(w,405,"method not allowed");return};a.hold(w,r)})
 mux.HandleFunc("/bookings",func(w http.ResponseWriter,r *http.Request){switch r.Method {case http.MethodGet:a.listBookings(w,r);case http.MethodPost:a.createBooking(w,r);default:problem(w,405,"method not allowed")}})
 mux.HandleFunc("/",a.crud)
 port:=env("PORT","8080");log.Printf("QuickTicket API listening on :%s",port);log.Fatal(http.ListenAndServe(":"+port,mux))
}
func env(k,d string)string{if v:=os.Getenv(k);v!=""{return v};return d}
