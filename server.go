package xwork

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
)

//go:embed static
var staticFs embed.FS

func (p *Processor) ServeMux() (*http.ServeMux, error) {
	subFs, err := fs.Sub(staticFs, "static")
	if err != nil {
		return nil, err
	}

	imagesFs, err := fs.Sub(subFs, "images")
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()

	mux.Handle("GET /api/count/{jobType}", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		jobType := r.PathValue("jobType")

		count, err := p.storage.Count(JobType(jobType))
		if err != nil {
			p.writeJsonError(w, err, http.StatusBadRequest)
		}

		p.writeJsonData(w, count)
	}))

	fileServer := http.FileServerFS(subFs)
	imagesFileServer := http.FileServerFS(imagesFs)
	mux.Handle("/", http.StripPrefix("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := imagesFs.Open(r.URL.Path); err == nil {
			imagesFileServer.ServeHTTP(w, r)
		} else {
			fileServer.ServeHTTP(w, r)
		}
	})))

	return mux, nil
}

type ResponseWriterWithCode struct {
	Code int
	http.ResponseWriter
}

func (w *ResponseWriterWithCode) WriteHeader(code int) {
	w.Code = code
	w.ResponseWriter.WriteHeader(code)
}

func (p *Processor) Serve() error {
	host := env("HOST", "0.0.0.0")
	port := env("PORT", "8080")
	addr := fmt.Sprintf("%s:%s", host, port)

	mux, err := p.ServeMux()
	if err != nil {
		return err
	}

	server := http.Server{
		Addr: addr,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			wWithCode := &ResponseWriterWithCode{ResponseWriter: w}
			start := time.Now()

			mux.ServeHTTP(wWithCode, req)

			end := time.Now()
			duration := end.Sub(start)

			p.logger.WithFields(logrus.Fields{
				"duration": duration,
				"code":     wWithCode.Code,
				"method":   req.Method,
				"path":     req.URL.Path,
			}).Info("Request complete")
		}),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	go func() {
		err := server.ListenAndServe()
		if err != nil {
			p.logger.Fatal(err)
			return
		}
	}()

	p.logger.Infof("Listening on %s", addr)

	return nil
}

func (p *Processor) writeJsonData(w http.ResponseWriter, data any) {
	err := p.doWriteJson(w, map[string]any{"data": data}, http.StatusOK)
	if err != nil {
		p.logger.Errorf("failed to respond with json data: %v", err)

		p.writeJsonError(w, errors.New("internal server error"), http.StatusInternalServerError)
		return
	}
}

func (p *Processor) writeJsonError(w http.ResponseWriter, originalErr error, code int) {
	err := p.doWriteJson(w, map[string]any{"error": originalErr.Error()}, code)
	if err != nil {
		p.logger.
			WithField("original_error", originalErr).
			Errorf("failed to respond with error json: %v", err)
		return
	}
}

func (p *Processor) doWriteJson(w http.ResponseWriter, data any, code int) error {
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	w.WriteHeader(code)
	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(jsonBytes)
	if err != nil {
		return err
	}

	return nil
}
