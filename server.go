package xwork

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gofrs/uuid"
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

	mux.Handle("GET /api/capabilities", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, supportsJobQuery := p.storage.(JobQueryAdapter)
		_, supportsFailedClaim := p.storage.(FailedJobClaimer)
		retryFailed := p.webActions.Load() && supportsFailedClaim

		p.writeJsonData(w, map[string]bool{
			"readOnly":    !retryFailed,
			"jobQuery":    supportsJobQuery,
			"retryFailed": retryFailed,
		})
	}))

	mux.Handle("GET /api/count/{jobType}", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		jobType, err := validJobType(r.PathValue("jobType"))
		if err != nil {
			p.writeJsonError(w, err, http.StatusBadRequest)
			return
		}

		count, err := p.storage.Count(jobType)
		if err != nil {
			p.logger.WithError(err).Error("failed to count jobs")
			p.writeJsonError(w, errors.New("failed to count jobs"), http.StatusInternalServerError)
			return
		}

		p.writeJsonData(w, count)
	}))

	mux.Handle("GET /api/jobs/{jobType}", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		jobs, hasMore, status, err := p.listJobs(r)
		if err != nil {
			p.writeJsonError(w, err, status)
			return
		}

		p.writeJsonDataWithMeta(w, jobs, map[string]bool{"hasMore": hasMore})
	}))

	mux.Handle("POST /api/jobs/failed/{id}/retry", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isSameOriginBrowserRequest(r) {
			p.writeJsonError(w, errors.New("cross-origin requests are not allowed"), http.StatusForbidden)
			return
		}
		if !hasJSONContentType(r) {
			p.writeJsonError(w, errors.New("content type must be application/json"), http.StatusBadRequest)
			return
		}
		if !hasValidJSONObjectBody(r) {
			p.writeJsonError(w, errors.New("request body must be a JSON object"), http.StatusBadRequest)
			return
		}
		if !p.webActions.Load() {
			p.writeJsonError(w, errors.New("web actions are disabled"), http.StatusForbidden)
			return
		}
		if _, ok := p.storage.(FailedJobClaimer); !ok {
			p.writeJsonError(w, errors.New("failed job retry is not supported"), http.StatusForbidden)
			return
		}

		id, err := uuid.FromString(r.PathValue("id"))
		if err != nil {
			p.writeJsonError(w, errors.New("invalid job id"), http.StatusBadRequest)
			return
		}

		job, err := p.retryFailedJob(id)
		switch {
		case errors.Is(err, errFailedJobNotFound):
			p.writeJsonError(w, errors.New("failed job not found"), http.StatusNotFound)
			return
		case errors.Is(err, errFailedJobClaimUnsupported):
			p.writeJsonError(w, errors.New("failed job retry is not supported"), http.StatusForbidden)
			return
		case err != nil:
			p.logger.WithError(err).WithField("job_id", id).Error("failed to retry job from web UI")
			p.writeJsonError(w, errors.New("failed to retry job"), http.StatusInternalServerError)
			return
		}

		p.writeJsonData(w, map[string]any{"job": job})
	}))

	fileServer := http.FileServerFS(subFs)
	imagesFileServer := http.FileServerFS(imagesFs)
	mux.Handle("/", http.StripPrefix("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "frame-ancestors 'none'")
		w.Header().Set("X-Frame-Options", "DENY")
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
		p.logger.Infof("Listening on %s", addr)
	}()

	return server.ListenAndServe()
}

func (p *Processor) writeJsonData(w http.ResponseWriter, data any) {
	err := p.doWriteJson(w, map[string]any{"data": data}, http.StatusOK)
	if err != nil {
		p.logger.Errorf("failed to respond with json data: %v", err)

		p.writeJsonError(w, errors.New("internal server error"), http.StatusInternalServerError)
		return
	}
}

func (p *Processor) writeJsonDataWithMeta(w http.ResponseWriter, data, meta any) {
	err := p.doWriteJson(w, map[string]any{"data": data, "meta": meta}, http.StatusOK)
	if err != nil {
		p.logger.Errorf("failed to respond with json data: %v", err)
		p.writeJsonError(w, errors.New("internal server error"), http.StatusInternalServerError)
	}
}

func (p *Processor) listJobs(r *http.Request) (any, bool, int, error) {
	jobType, err := validJobType(r.PathValue("jobType"))
	if err != nil {
		return nil, false, http.StatusBadRequest, err
	}

	query, err := parseJobQuery(r)
	if err != nil {
		return nil, false, http.StatusBadRequest, err
	}
	if jobType == JobTypeEnqueued && query.Queue == "" && !query.AllQueues {
		query.Queue = DefaultQueue
	}

	if adapter, ok := p.storage.(JobQueryAdapter); ok {
		jobs, hasMore, err := adapter.ListJobs(jobType, query)
		if err != nil {
			p.logger.WithError(err).WithField("job_type", jobType).Error("failed to list jobs")
			return nil, false, http.StatusInternalServerError, errors.New("failed to list jobs")
		}
		return jobs, hasMore, http.StatusOK, nil
	}

	if query.Query != "" {
		return nil, false, http.StatusBadRequest, errors.New("job search is not supported by this storage adapter")
	}
	if query.Queue != "" && jobType != JobTypeEnqueued {
		return nil, false, http.StatusBadRequest, errors.New("queue filtering is not supported by this storage adapter")
	}

	jobs, hasMore, err := p.listLegacyJobs(jobType, query)
	if err != nil {
		p.logger.WithError(err).WithField("job_type", jobType).Error("failed to list jobs")
		return nil, false, http.StatusInternalServerError, errors.New("failed to list jobs")
	}
	return jobs, hasMore, http.StatusOK, nil
}

func (p *Processor) listLegacyJobs(jobType JobType, query JobQuery) (any, bool, error) {
	limitWithLookahead := query.Limit + 1
	switch jobType {
	case JobTypeScheduled:
		jobs, err := p.storage.ListScheduled(limitWithLookahead, query.Offset)
		page, hasMore := trimLookahead(jobs, query.Limit)
		return page, hasMore, err
	case JobTypeEnqueued:
		queue := query.Queue
		if queue == "" {
			queue = DefaultQueue
		}
		jobs, err := p.storage.ListEnqueued(queue, limitWithLookahead, query.Offset)
		page, hasMore := trimLookahead(jobs, query.Limit)
		return page, hasMore, err
	case JobTypeProcessing:
		jobs, err := p.storage.ListProcessing(limitWithLookahead, query.Offset)
		page, hasMore := trimLookahead(jobs, query.Limit)
		return page, hasMore, err
	case JobTypeProcessed:
		jobs, err := p.storage.ListProcessed(limitWithLookahead, query.Offset)
		page, hasMore := trimLookahead(jobs, query.Limit)
		return page, hasMore, err
	case JobTypeFailed:
		jobs, err := p.storage.ListFailed(limitWithLookahead, query.Offset)
		page, hasMore := trimLookahead(jobs, query.Limit)
		return page, hasMore, err
	default:
		return nil, false, fmt.Errorf("unsupported job type %q", jobType)
	}
}

func validJobType(raw string) (JobType, error) {
	jobType := JobType(raw)
	switch jobType {
	case JobTypeScheduled, JobTypeEnqueued, JobTypeProcessing, JobTypeProcessed, JobTypeFailed:
		return jobType, nil
	default:
		return "", fmt.Errorf("unsupported job type %q", raw)
	}
}

func parseJobQuery(r *http.Request) (JobQuery, error) {
	limit, err := parseUintQuery(r, "limit", 25)
	if err != nil || limit == 0 || limit > 100 {
		return JobQuery{}, errors.New("limit must be between 1 and 100")
	}
	offset, err := parseUintQuery(r, "offset", 0)
	if err != nil {
		return JobQuery{}, errors.New("offset must be a non-negative integer")
	}
	allQueues, err := parseBoolQuery(r, "allQueues", false)
	if err != nil {
		return JobQuery{}, errors.New("allQueues must be a boolean")
	}

	return JobQuery{
		Query:     strings.TrimSpace(r.URL.Query().Get("q")),
		Queue:     r.URL.Query().Get("queue"),
		AllQueues: allQueues,
		Limit:     limit,
		Offset:    offset,
	}, nil
}

func parseUintQuery(r *http.Request, key string, fallback uint) (uint, error) {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		return fallback, nil
	}

	parsed, err := strconv.ParseUint(raw, 10, 32)
	if err != nil {
		return 0, err
	}
	return uint(parsed), nil
}

func parseBoolQuery(r *http.Request, key string, fallback bool) (bool, error) {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		return fallback, nil
	}
	return strconv.ParseBool(raw)
}

func trimLookahead[T any](jobs []T, limit uint) ([]T, bool) {
	if uint(len(jobs)) <= limit {
		return jobs, false
	}
	return jobs[:limit], true
}

var (
	errFailedJobNotFound         = errors.New("failed job not found")
	errFailedJobClaimUnsupported = errors.New("failed job claim is not supported")
)

func (p *Processor) retryFailedJob(id uuid.UUID) (*EnqueuedJob, error) {
	p.webRetryMu.Lock()
	defer p.webRetryMu.Unlock()

	var enqueuedJob *EnqueuedJob
	err := p.storage.Transact(func(storage StorageAdapter) error {
		claimer, ok := storage.(FailedJobClaimer)
		if !ok {
			return errFailedJobClaimUnsupported
		}

		failedJob, err := claimer.ClaimFailed(id)
		if err != nil {
			return err
		}
		if failedJob == nil {
			return errFailedJobNotFound
		}

		enqueuedJob = &EnqueuedJob{
			ID:          failedJob.ID,
			Name:        failedJob.Name,
			Queue:       failedJob.Queue,
			Payload:     failedJob.Payload,
			RetryCount:  failedJob.RetryCount,
			EnqueuedAt:  time.Now(),
			ScheduledAt: failedJob.ScheduledAt,
		}

		return storage.InsertToQueue(enqueuedJob)
	})
	if err != nil {
		return nil, err
	}
	return enqueuedJob, nil
}

func hasJSONContentType(r *http.Request) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		return false
	}
	return mediaType == "application/json" || strings.HasSuffix(mediaType, "+json")
}

func hasValidJSONObjectBody(r *http.Request) bool {
	var body map[string]json.RawMessage
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	if err := decoder.Decode(&body); err != nil || body == nil {
		return false
	}
	return decoder.Decode(&struct{}{}) == io.EOF
}

func isSameOriginBrowserRequest(r *http.Request) bool {
	switch strings.ToLower(strings.TrimSpace(r.Header.Get("Sec-Fetch-Site"))) {
	case "cross-site", "same-site":
		return false
	}

	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	originURL, err := url.Parse(origin)
	if err != nil || originURL.Host == "" || (originURL.Scheme != "http" && originURL.Scheme != "https") {
		return false
	}
	if !strings.EqualFold(originURL.Host, r.Host) {
		return false
	}

	requestScheme := "http"
	if r.TLS != nil {
		requestScheme = "https"
	}
	if forwardedProto := strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-Proto"), ",")[0]); forwardedProto != "" {
		requestScheme = strings.ToLower(forwardedProto)
	}
	return strings.EqualFold(originURL.Scheme, requestScheme)
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
