FROM golang:1.23-alpine AS builder
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /search .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /search /search
EXPOSE 8085
ENTRYPOINT ["/search"]
