package main

import (
	"io"
	"io/fs"
	"net/http"
)

// serveWebUI registers embedded web UI routes
func serveWebUI() {
	sub, err := fs.Sub(webFS, "WEBUI")
	if err != nil {
		panic(err)
	}

	// Static assets: /static/*
	http.Handle("/static/",
		http.StripPrefix(
			"/static/",
			http.FileServer(http.FS(sub)),
		),
	)

	// Root: /
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}

		f, err := sub.Open("index.html")
		if err != nil {
			http.NotFound(w, r)
			return
		}
		defer f.Close()

		w.Header().Set("Content-Type", "text/html")

		// ⬇️ INI FIX UTAMANYA
		_, _ = io.Copy(w, f)
	})
}
