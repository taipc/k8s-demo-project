package main

import (
	"context"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go" 
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	// 1. Kết nối MongoDB
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		mongoURI = "mongodb://localhost:27017"
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatalf("Lỗi kết nối MongoDB: %v", err)
	}
	defer client.Disconnect(ctx)

	coll := client.Database("demo").Collection("logs")
	log.Println("--- Đã kết nối MongoDB ---")

	// 2. Kết nối RabbitMQ
	rabbitHost := os.Getenv("RABBITMQ_HOST")
	conn, err := amqp.Dial("amqp://guest:guest@" + rabbitHost + ":5672/")
	if err != nil {
		log.Fatalf("Lỗi kết nối RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Lỗi mở channel: %v", err)
	}
	defer ch.Close()

	// 3. Đăng ký nhận tin nhắn (Consume)
	msgs, err := ch.Consume(
		"demo", // tên queue phải khớp với bên API
		"",     // consumer tag
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Fatalf("Lỗi đăng ký consumer: %v", err)
	}

	log.Println(" [*] Người đưa thư đang trực chiến. Chờ tin nhắn...")

	// 4. Vòng lặp nhận thư và lưu vào DB
	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received: %s", d.Body)

			// Tạo context có timeout 5 giây cho mỗi lần lưu
			insertCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			
			_, err := coll.InsertOne(insertCtx, map[string]string{"val": string(d.Body)})
			cancel() // Luôn gọi cancel để giải phóng tài nguyên

			if err != nil {
				log.Printf("Lỗi khi lưu vào DB: %v", err)
			} else {
				log.Println(" OK – Đã lưu vào MongoDB thành công!")
			}
		}
	}()

	<-forever
}