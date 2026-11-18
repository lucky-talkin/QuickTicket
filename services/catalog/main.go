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

 "github.com/jackc/pgx/v5"
 "github.com/jackc/pgx/v5/pgxpool"
)

type Resource struct { table string; fields []string }
var resources=map[string]Resource{
 "users":{"users",[]string{"name","email"}},
 "venues":{"venues",[]string{"name","city","capacity"}},
 "performers":{"performers",[]string{"name","description"}},
 "events":{"events",[]string{"name","description","event_type","starts_at","venue_id","performer_id"}},
}
type API struct{db *pgxpool.Pool}
func out(w http.ResponseWriter,c int,v any){w.Header().Set("Content-Type","application/json");w.WriteHeader(c);_=json.NewEncoder(w).Encode(v)}
func errOut(w http.ResponseWriter,c int,s string){out(w,c,map[string]string{"error":s})}
func(a *API)handler(w http.ResponseWriter,r *http.Request){
 p:=strings.Split(strings.Trim(r.URL.Path,"/"),"/");if len(p)<1||p[0]==""{errOut(w,404,"not found");return}
 cfg,ok:=resources[p[0]];if !ok{errOut(w,404,"unknown resource");return};cols:=append([]string{"id"},cfg.fields...)
 if len(p)==1&&r.Method==http.MethodGet{
 rows,e:=a.db.Query(r.Context(),"SELECT "+strings.Join(cols,",")+" FROM "+cfg.table+" ORDER BY id");if e!=nil{errOut(w,500,e.Error());return};defer rows.Close()
 result:=[]map[string]any{};for rows.Next(){v:=make([]any,len(cols));ptr:=make([]any,len(v));for i:=range v{ptr[i]=&v[i]};if e=rows.Scan(ptr...);e!=nil{errOut(w,500,e.Error());return};m:=map[string]any{};for i,k:=range cols{m[k]=v[i]};result=append(result,m)};out(w,200,result);return}
 if len(p)==1&&r.Method==http.MethodPost{
 body:=map[string]any{};if json.NewDecoder(r.Body).Decode(&body)!=nil{errOut(w,400,"invalid JSON");return};fields,args:=[]string{},[]any{}
 for _,f:=range cfg.fields{if v,ok:=body[f];ok{fields=append(fields,f);args=append(args,v)}};if len(fields)==0{errOut(w,400,"no supported fields");return}
 marks:=[]string{};for i:=range args{marks=append(marks,fmt.Sprintf("$%d",i+1))};var id int64
 q:=fmt.Sprintf("INSERT INTO %s(%s) VALUES(%s) RETURNING id",cfg.table,strings.Join(fields,","),strings.Join(marks,","))
 if e:=a.db.QueryRow(r.Context(),q,args...).Scan(&id);e!=nil{errOut(w,400,e.Error());return};out(w,201,map[string]int64{"id":id});return}
 if len(p)==2{
 id,e:=strconv.ParseInt(p[1],10,64);if e!=nil{errOut(w,400,"invalid id");return}
 if r.Method==http.MethodGet{v:=make([]any,len(cols));ptr:=make([]any,len(v));for i:=range v{ptr[i]=&v[i]};q:=fmt.Sprintf("SELECT %s FROM %s WHERE id=$1",strings.Join(cols,","),cfg.table)
 if e=a.db.QueryRow(r.Context(),q,id).Scan(ptr...);e!=nil{if e==pgx.ErrNoRows{errOut(w,404,"not found")}else{errOut(w,500,e.Error())};return};m:=map[string]any{};for i,k:=range cols{m[k]=v[i]};out(w,200,m);return}
 if r.Method==http.MethodPut{body:=map[string]any{};if json.NewDecoder(r.Body).Decode(&body)!=nil{errOut(w,400,"invalid JSON");return};sets,args:=[]string{},[]any{}
 for _,f:=range cfg.fields{if v,ok:=body[f];ok{args=append(args,v);sets=append(sets,fmt.Sprintf("%s=$%d",f,len(args)))}};if len(sets)==0{errOut(w,400,"no supported fields");return};args=append(args,id)
 tag,e:=a.db.Exec(r.Context(),fmt.Sprintf("UPDATE %s SET %s WHERE id=$%d",cfg.table,strings.Join(sets,","),len(args)),args...);if e!=nil{errOut(w,400,e.Error());return};if tag.RowsAffected()==0{errOut(w,404,"not found");return};out(w,200,map[string]bool{"updated":true});return}
 if r.Method==http.MethodDelete{tag,e:=a.db.Exec(r.Context(),"DELETE FROM "+cfg.table+" WHERE id=$1",id);if e!=nil{errOut(w,409,e.Error());return};if tag.RowsAffected()==0{errOut(w,404,"not found");return};w.WriteHeader(204);return}
 }
 errOut(w,405,"method not allowed")
}
func main(){ctx:=context.Background();dsn:=os.Getenv("DATABASE_URL");db,e:=pgxpool.New(ctx,dsn);if e!=nil{log.Fatal(e)};defer db.Close();if e=db.Ping(ctx);e!=nil{log.Fatal(e)}
 mux:=http.NewServeMux();a:=&API{db};mux.HandleFunc("/health",func(w http.ResponseWriter,r *http.Request){out(w,200,map[string]string{"status":"ok"})});mux.HandleFunc("/",a.handler)
 port:=os.Getenv("PORT");if port==""{port="8081"};log.Fatal(http.ListenAndServe(":"+port,mux))
}