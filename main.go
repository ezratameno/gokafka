package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	err := run()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

type Message struct {
	data []byte
}

type Server struct {
	cOffsets map[string]int
	buffer   []Message

	ln net.Listener
}

func (s *Server) start() error {
	return nil

}

func NewServer() *Server {
	return &Server{
		cOffsets: map[string]int{},
		buffer:   []Message{},
	}
}

func (s *Server) listen() error {
	ln, err := net.Listen("tcp", ":9092")
	if err != nil {
		return err
	}

	s.ln = ln

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			slog.Error("server accept error", "err", err)

			return nil
		}

		go s.handleConn(conn)

	}
	return nil

}

func (s *Server) handleConn(conn net.Conn) {
	fmt.Printf("new connection: %s\n", conn.RemoteAddr().String())

	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}

			slog.Error("connection read error", "err", err)
			return
		}

		fmt.Println(string(buf[:n]))
	}
}

func run() error {

	server := NewServer()

	go func() {
		err := server.listen()
		if err != nil {
			log.Fatal(err)
		}
	}()
	time.Sleep(1 * time.Second)

	fmt.Println("producing...")
	produce()

	fmt.Println("consuming...")
	consume()
	return nil
}

func consume() error {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		// User-specific properties that you must set
		"bootstrap.servers": "localhost:9092",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest"})

	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	defer c.Close()

	topic := "fooTopic"
	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		return err
	}

	run := true
	for run {
		msg, err := c.ReadMessage(time.Second)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else if err != nil && !err.(kafka.Error).IsTimeout() {
			return fmt.Errorf("consumer error: %w (%v)", err, msg)
		}
	}

	return nil
}

func produce() error {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		return err
	}

	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	topic := "fooTopic"
	for _, word := range []string{"Welcome", "to", "the", "Confluent", "Kafka", "Golang", "client"} {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(word),
		}, nil)
	}

	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)

	return nil
}
