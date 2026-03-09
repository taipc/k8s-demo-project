FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY . .
# Tạo module cho worker
RUN go mod init worker && go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o worker-app .

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/worker-app .
CMD ["./worker-app"]