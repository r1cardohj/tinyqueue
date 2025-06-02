package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type App struct {
	queue      *Queue
	port       string
	host       string
	router     map[string]http.HandlerFunc // path: hanlder
	reqTimeout time.Duration
	infoLog    *log.Logger
	errLog     *log.Logger
}

func NewApp(queue *Queue, host string, port string, reqTimeout time.Duration) *App {
	app := &App{
		queue:      queue,
		host:       host,
		port:       port,
		infoLog:    log.New(os.Stdout, "TQ [Info] ", log.LstdFlags),
		errLog:     log.New(os.Stderr, "TQ [Error] ", log.LstdFlags|log.Lshortfile),
		reqTimeout: reqTimeout,
	}
	app.initRouter()
	return app
}

func (app *App) initRouter() {
	app.router = make(map[string]http.HandlerFunc)
	app.router["/queue"] = app.handleQueueQuery
	app.router["/queue/size"] = app.handleQueueSize
}

func (app *App) handleQueueQuery(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		value, err := app.queue.Dequeue()
		if err != nil {
			if err.Error() == "queue is empty" {
				http.Error(w, "queue is empty", http.StatusNoContent)
			} else {
				app.errLog.Printf("Dequeue Error: %v", err)
				http.Error(w, "Internal server error: "+err.Error(), 500)
			}
			return
		}
		w.WriteHeader(200)
		w.Write(value)
	case "PUT":
		defer r.Body.Close()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			abort400(w, "Failed to read request body")
			return
		}
		err = app.queue.Enqueue(body)
		if err != nil {
			app.errLog.Printf("Enqueue Error: %v", err)
			abort500(w)
			return
		}

		code201(w)
	default:
		// other methods is not allowed
		abort405(w)
	}
}

func (app *App) handleQueueSize(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		fmt.Fprintln(w, app.queue.Size())
	default:
		abort405(w)
	}
}

// protocl for http/net inner server
func (app *App) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	path := r.URL.Path

	defer func() {
		app.infoLog.Printf("%s %s %s %v", r.Method, path, r.Proto, time.Since(start))
	}()

	handler, ok := app.router[r.URL.Path]
	if !ok {
		abort404(w)
		return
	}
	handler(w, r)
}

func (app *App) Run() error {
	defer app.Close()
	server := &http.Server{
		Addr:    app.host + ":" + app.port,
		Handler: http.TimeoutHandler(app, app.reqTimeout, "Request timeout"),
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	serverErr := make(chan error, 1)
	go func() {
		app.infoLog.Printf("Server starting on %s\n", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
	}()

	select {
	case sig := <-signalChan:
		app.infoLog.Printf("Received signal: %v, shutting down...\n", sig)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(ctx)
	case err := <-serverErr:

		app.errLog.Printf("Received server error: %v", err)
		return fmt.Errorf("server error: %v", err)
	}
}

func (app *App) Close() error {
	if app.queue != nil {
		if err := app.queue.Close(); err != nil {
			app.errLog.Printf("Queue close error: %v", err)
			return err
		}
	}
	app.infoLog.Println("Resources cleaned up successfully")
	return nil
}

func code201(w http.ResponseWriter) {
	w.WriteHeader(201)
	fmt.Fprintln(w, "created successfully")
}

func abort400(w http.ResponseWriter, msg string) {
	http.Error(w, msg, 400)
}

func abort404(w http.ResponseWriter) {
	http.Error(w, "path not found.", 404)
}

func abort405(w http.ResponseWriter) {
	http.Error(w, "method not allowed", 405)
}

func abort500(w http.ResponseWriter) {
	http.Error(w, "Internal Server Error", 500)
}
