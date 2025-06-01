package main

import (
	"fmt"
	"net/http"
)

func main() {
	q, err := NewQueue("queue.db", []byte("queue_"))
	if err != nil {
		fmt.Println(err)
		return
	}

	app := &App{
		queue: q,
		port: "8000",
		host: "127.0.0.1",
	}

	err = http.ListenAndServe(app.host + ":" + app.port, app);
	if err != nil {
		fmt.Println(err)
		return
	}
}

