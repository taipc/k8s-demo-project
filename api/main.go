package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	amqp "github.com/rabbitmq/amqp091-go"
)
func failOnError(err error, msg string) {
  if err != nil {
    log.Panicf("%s: %s", msg, err)
  }
}
func main() {
	// 1. Lấy biến môi trường đã cấu hình trong YAML
	rabbitHost := os.Getenv("RABBITMQ_HOST")
	if rabbitHost == "" {
		rabbitHost = "localhost" // Dự phòng nếu chạy local ngoài K8s
	}

	// 2. Thiết lập kết nối (Dial)
	conn, err := amqp.Dial("amqp://guest:guest@" + rabbitHost + ":5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	// 3. Mở Channel
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	// 4. Khai báo hàng đợi (Queue)
	q, err := ch.QueueDeclare(
		"demo", // tên hàng đợi
		true,   // durable: hàng đợi sẽ tồn tại khi server restart
		false,  // delete when unused
		false,  // exclusive
		false,  // no-wait
		nil,    // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}
	
	r := gin.Default()

	// Endpoint GET /ping để kiểm tra và đẩy metrics lên Prometheus
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{"message": "pong"})
	})

	// Endpoint POST /send để gửi tin nhắn vào RabbitMQ
	r.POST("/send", func(c *gin.Context) {
		var b struct {
			Msg string `json:"message"`
		}
		if err := c.BindJSON(&b); err != nil {
			c.JSON(400, gin.H{"error": "Invalid JSON"})
			return
		}

		//  dùng Context để quản lý timeout khi gửi tin nhắn
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err = ch.PublishWithContext(ctx,
			"",     // exchange
			q.Name, // routing key (tên queue)
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(b.Msg),
			})

		if err != nil {
			c.JSON(500, gin.H{"status": "Failed to send message"})
			return
		}

		c.JSON(200, gin.H{"status": "Sent", "val": b.Msg})
	})

	// Chạy ở cổng 5001 tương ứng với containerPort trong Deployment
	r.Run(":5001")
}