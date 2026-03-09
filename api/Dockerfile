FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY . .
# Tự động khởi tạo module và tải thư viện ngay trong Docker
RUN go mod init app && go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o app-binary .

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/app-binary .
CMD ["./app-binary"]