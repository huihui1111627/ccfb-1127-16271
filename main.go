package main

import (
	_ "embed"
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	webapi "skewexec/internal/web"
)

//go:embed web/index.html
var indexHTML []byte

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	flag.Parse()

	api := webapi.New().Handler()
	mux := http.NewServeMux()
	mux.Handle("/api/", api)
	mux.HandleFunc("/acceptance_report.json", func(w http.ResponseWriter, r *http.Request) {
		if _, err := os.Stat("acceptance_report.json"); err != nil {
			http.Error(w, "report not generated yet: run `go run ./cmd/acceptance`", http.StatusNotFound)
			return
		}
		http.ServeFile(w, r, "acceptance_report.json")
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(indexHTML)
	})

	log.Printf("渔港冷库倾斜查询执行器已启动: http://localhost%s", *addr)
	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Fatal(srv.ListenAndServe())
}
