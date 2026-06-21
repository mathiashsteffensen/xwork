package xwork

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"strconv"
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
			return
		}

		p.writeJsonData(w, count)
	}))

	mux.Handle("GET /api/jobs/{jobType}", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		jobs, err := p.listJobs(r)
		if err != nil {
			p.writeJsonError(w, err, http.StatusBadRequest)
			return
		}

		p.writeJsonData(w, jobs)
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

func (p *Processor) listJobs(r *http.Request) (any, error) {
	limit := parseUintQuery(r, "limit", 25)
	offset := parseUintQuery(r, "offset", 0)

	switch JobType(r.PathValue("jobType")) {
	case JobTypeScheduled:
		return p.storage.ListScheduled(limit, offset)
	case JobTypeEnqueued:
		queue := r.URL.Query().Get("queue")
		if queue == "" {
			queue = "default"
		}

		return p.storage.ListEnqueued(queue, limit, offset)
	case JobTypeProcessing:
		return p.storage.ListProcessing(limit, offset)
	case JobTypeProcessed:
		return p.storage.ListProcessed(limit, offset)
	case JobTypeFailed:
		return p.storage.ListFailed(limit, offset)
	default:
		return nil, fmt.Errorf("unsupported job type %q", r.PathValue("jobType"))
	}
}

func parseUintQuery(r *http.Request, key string, fallback uint) uint {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		return fallback
	}

	parsed, err := strconv.ParseUint(raw, 10, 32)
	if err != nil {
		return fallback
	}

	return uint(parsed)
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

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, err = w.Write(jsonBytes)
	if err != nil {
		return err
	}

	return nil
}
