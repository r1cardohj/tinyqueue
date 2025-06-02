package main

import (
	"log"
	"os"
	"time"
)

func main() {
	q, err := NewQueue("queue.db", []byte("queue_"), true)
	if err != nil {
		log.Panic(err)
		return
	}
	app := NewApp(q, "127.0.0.1", "8000", 15*time.Second)

	err = app.Run()
	if err != nil {
		log.Println("server start error!")
		os.Exit(1)
	}
	log.Println("Server shutdown gracefully")
}
