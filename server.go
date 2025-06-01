package main

import (
	"net/http"
	"fmt"
	"time"
)

type App struct {
	queue      *Queue
	port       string
	host       string
	reqTimeout time.Duration
}


func (app *App) handleHttpReq(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		value, err := app.queue.Dequeue()
		if err != nil {
			fmt.Println(err)
			http.Error(w, "Internal server error: " + err.Error(), 500)
		}
		fmt.Fprintln(w, value)
	case "POST":
		fmt.Fprintln(w, app.queue.Size())
	}
}

// protocl for http/net inner server
func (app *App) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	app.handleHttpReq(w, r)
}

func Router(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
	} else if r.Method == "POST" {
	} else {
		fmt.Fprintln(w, "inaviliable request.")
	}
}

