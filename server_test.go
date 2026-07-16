package xwork_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/mathiashsteffensen/xwork/v2"
	"github.com/mathiashsteffensen/xwork/v2/storage"
)

func TestServeMuxWorksBehindPathPrefix(t *testing.T) {
	processor, err := xwork.NewProcessor(storage.NewMemory())
	if err != nil {
		t.Fatal(err)
	}

	xworkMux, err := processor.ServeMux()
	if err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.Handle("/xwork/", http.StripPrefix("/xwork", xworkMux))

	t.Run("serves prefixed static assets", func(t *testing.T) {
		res := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xwork/index.js", nil)

		mux.ServeHTTP(res, req)

		if res.Code != http.StatusOK {
			t.Fatalf("expected status %d, got %d", http.StatusOK, res.Code)
		}
		assertDashboardFrameProtection(t, res)
		if body := res.Body.String(); strings.Contains(body, "fetch(`/") || strings.Contains(body, `fetch("/`) {
			t.Fatal("expected JavaScript fetch calls to use relative URLs")
		}
	})

	t.Run("serves prefixed third-party assets", func(t *testing.T) {
		res := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xwork/third_party/bootstrap-5.3.8/css/bootstrap.min.css", nil)

		mux.ServeHTTP(res, req)

		if res.Code != http.StatusOK {
			t.Fatalf("expected status %d, got %d", http.StatusOK, res.Code)
		}
	})

	t.Run("uses relative browser URLs", func(t *testing.T) {
		res := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xwork/", nil)

		mux.ServeHTTP(res, req)

		if res.Code != http.StatusOK {
			t.Fatalf("expected status %d, got %d", http.StatusOK, res.Code)
		}
		assertDashboardFrameProtection(t, res)

		body := res.Body.String()
		for _, absolutePath := range []string{`href="/`, `src="/`} {
			if strings.Contains(body, absolutePath) {
				t.Fatalf("expected no root-relative %s references in response body", absolutePath)
			}
		}
	})

	t.Run("serves prefixed API routes", func(t *testing.T) {
		res := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xwork/api/count/enqueued", nil)

		mux.ServeHTTP(res, req)

		if res.Code != http.StatusOK {
			t.Fatalf("expected status %d, got %d", http.StatusOK, res.Code)
		}
		if body := res.Body.String(); !strings.Contains(body, `"data":0`) {
			t.Fatalf("expected count response, got %q", body)
		}
	})
}

func TestWebCapabilitiesReflectStorageAndOptIn(t *testing.T) {
	processor, err := xwork.NewProcessor(storage.NewMemory())
	if err != nil {
		t.Fatal(err)
	}
	mux, err := processor.ServeMux()
	if err != nil {
		t.Fatal(err)
	}

	assertCapabilities := func(readOnly, jobQuery, retryFailed bool) {
		t.Helper()
		res := httptest.NewRecorder()
		mux.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/api/capabilities", nil))
		if res.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", res.Code)
		}

		var body struct {
			Data struct {
				ReadOnly    bool `json:"readOnly"`
				JobQuery    bool `json:"jobQuery"`
				RetryFailed bool `json:"retryFailed"`
			} `json:"data"`
		}
		if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if body.Data.ReadOnly != readOnly || body.Data.JobQuery != jobQuery || body.Data.RetryFailed != retryFailed {
			t.Fatalf("unexpected capabilities: %+v", body.Data)
		}
	}

	assertCapabilities(true, true, false)
	processor.SetWebActionsEnabled(true)
	assertCapabilities(false, true, true)
}

func TestWebRetryRequiresAtomicFailedJobClaimer(t *testing.T) {
	store := &lookupOnlyStore{StorageAdapter: storage.NewMemory()}
	processor, err := xwork.NewProcessor(store)
	if err != nil {
		t.Fatal(err)
	}
	processor.SetWebActionsEnabled(true)
	mux, err := processor.ServeMux()
	if err != nil {
		t.Fatal(err)
	}

	res := httptest.NewRecorder()
	mux.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/api/capabilities", nil))
	if res.Code != http.StatusOK {
		t.Fatalf("expected capabilities status 200, got %d", res.Code)
	}
	var capabilities struct {
		Data struct {
			ReadOnly    bool `json:"readOnly"`
			RetryFailed bool `json:"retryFailed"`
		} `json:"data"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &capabilities); err != nil {
		t.Fatal(err)
	}
	if !capabilities.Data.ReadOnly || capabilities.Data.RetryFailed {
		t.Fatalf("expected lookup-only storage to remain read-only, got %+v", capabilities.Data)
	}
}

func TestListJobsSupportsFilteringAndPagination(t *testing.T) {
	store := storage.NewMemory()
	now := time.Now()
	jobs := []*xwork.EnqueuedJob{
		{ID: newServerTestUUID(t), Name: "alpha.first", Queue: "critical", EnqueuedAt: now, ScheduledAt: now},
		{ID: newServerTestUUID(t), Name: "alpha.second", Queue: "critical", EnqueuedAt: now.Add(time.Minute), ScheduledAt: now},
		{ID: newServerTestUUID(t), Name: "alpha.other", Queue: "default", EnqueuedAt: now.Add(-time.Minute), ScheduledAt: now},
	}
	for _, job := range jobs {
		if err := store.InsertToQueue(job); err != nil {
			t.Fatal(err)
		}
	}

	processor, err := xwork.NewProcessor(store)
	if err != nil {
		t.Fatal(err)
	}
	mux, err := processor.ServeMux()
	if err != nil {
		t.Fatal(err)
	}

	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/jobs/enqueued?q=ALPHA&queue=critical&limit=1&offset=0", nil)
	mux.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", res.Code, res.Body.String())
	}

	type jobsResponse struct {
		Data []xwork.EnqueuedJob `json:"data"`
		Meta struct {
			HasMore bool `json:"hasMore"`
		} `json:"meta"`
	}
	var body jobsResponse
	if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Data) != 1 || body.Data[0].ID != jobs[0].ID {
		t.Fatalf("expected oldest matching critical job, got %+v", body.Data)
	}
	if !body.Meta.HasMore {
		t.Fatal("expected pagination lookahead to report another result")
	}

	res = httptest.NewRecorder()
	mux.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/api/jobs/enqueued?limit=25", nil))
	if res.Code != http.StatusOK {
		t.Fatalf("expected default queue request to return 200, got %d: %s", res.Code, res.Body.String())
	}
	var defaultQueueBody jobsResponse
	if err := json.Unmarshal(res.Body.Bytes(), &defaultQueueBody); err != nil {
		t.Fatal(err)
	}
	if len(defaultQueueBody.Data) != 1 || defaultQueueBody.Data[0].ID != jobs[2].ID {
		t.Fatalf("expected an omitted queue to preserve the default-queue filter, got %+v", defaultQueueBody.Data)
	}

	res = httptest.NewRecorder()
	mux.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/api/jobs/enqueued?allQueues=true&limit=25", nil))
	if res.Code != http.StatusOK {
		t.Fatalf("expected all-queues request to return 200, got %d: %s", res.Code, res.Body.String())
	}
	var allQueuesBody jobsResponse
	if err := json.Unmarshal(res.Body.Bytes(), &allQueuesBody); err != nil {
		t.Fatal(err)
	}
	if len(allQueuesBody.Data) != len(jobs) {
		t.Fatalf("expected all queues, got %+v", allQueuesBody.Data)
	}

	for _, path := range []string{
		"/api/jobs/enqueued?limit=0",
		"/api/jobs/enqueued?limit=101",
		"/api/jobs/enqueued?offset=-1",
		"/api/jobs/enqueued?allQueues=sometimes",
		"/api/jobs/unknown",
	} {
		res := httptest.NewRecorder()
		mux.ServeHTTP(res, httptest.NewRequest(http.MethodGet, path, nil))
		if res.Code != http.StatusBadRequest {
			t.Fatalf("expected %s to return 400, got %d", path, res.Code)
		}
	}
}

func TestRetryFailedJobRequiresOptInJSONAndSameOrigin(t *testing.T) {
	store := storage.NewMemory()
	failed := &xwork.FailedJob{
		ID:          newServerTestUUID(t),
		Name:        "mail.deliver",
		Queue:       "critical",
		Payload:     xwork.JobPayload{"recipient": "user@example.com"},
		Error:       `{"message":"smtp unavailable"}`,
		RetryCount:  4,
		LastRetryAt: time.Now(),
		NextRetryAt: time.Now().Add(time.Hour),
		ScheduledAt: time.Now().Add(-time.Hour),
	}
	if err := store.InsertToFailed(failed); err != nil {
		t.Fatal(err)
	}

	processor, err := xwork.NewProcessor(store)
	if err != nil {
		t.Fatal(err)
	}
	mux, err := processor.ServeMux()
	if err != nil {
		t.Fatal(err)
	}
	path := "/api/jobs/failed/" + failed.ID.String() + "/retry"

	request := func(origin, contentType string) *httptest.ResponseRecorder {
		t.Helper()
		res := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "http://example.com"+path, strings.NewReader(`{}`))
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		mux.ServeHTTP(res, req)
		return res
	}

	if res := request("http://example.com", "application/json"); res.Code != http.StatusForbidden {
		t.Fatalf("expected disabled action to return 403, got %d", res.Code)
	}
	processor.SetWebActionsEnabled(true)
	if res := request("http://evil.example", "application/json"); res.Code != http.StatusForbidden {
		t.Fatalf("expected cross-origin action to return 403, got %d", res.Code)
	}
	if res := request("http://example.com", "text/plain"); res.Code != http.StatusBadRequest {
		t.Fatalf("expected non-JSON action to return 400, got %d", res.Code)
	}
	res := httptest.NewRecorder()
	malformed := httptest.NewRequest(http.MethodPost, "http://example.com"+path, strings.NewReader(`{"unfinished":`))
	malformed.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(res, malformed)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected malformed JSON to return 400, got %d", res.Code)
	}

	before := time.Now()
	res = request("http://example.com", "application/json; charset=utf-8")
	after := time.Now()
	if res.Code != http.StatusOK {
		t.Fatalf("expected retry to succeed, got %d: %s", res.Code, res.Body.String())
	}
	var body struct {
		Data struct {
			Job xwork.EnqueuedJob `json:"job"`
		} `json:"data"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	retried := body.Data.Job
	if retried.ID != failed.ID || retried.Name != failed.Name || retried.Queue != failed.Queue || retried.RetryCount != failed.RetryCount || !retried.ScheduledAt.Equal(failed.ScheduledAt) {
		t.Fatalf("retry did not preserve failed job fields: %+v", retried)
	}
	if retried.Payload["recipient"] != failed.Payload["recipient"] {
		t.Fatalf("retry did not preserve payload: %+v", retried.Payload)
	}
	if retried.EnqueuedAt.Before(before) || retried.EnqueuedAt.After(after) {
		t.Fatalf("expected a fresh enqueue time, got %s", retried.EnqueuedAt)
	}

	if found, err := store.GetFailed(failed.ID); err != nil || found != nil {
		t.Fatalf("expected failed job to be removed, got job=%v err=%v", found, err)
	}
	enqueued, err := store.GetFromQueue(failed.Queue)
	if err != nil || enqueued == nil || enqueued.ID != failed.ID {
		t.Fatalf("expected failed job in its original queue, got job=%v err=%v", enqueued, err)
	}
	if res := request("http://example.com", "application/json"); res.Code != http.StatusNotFound {
		t.Fatalf("expected repeated retry to return 404, got %d", res.Code)
	}
}

func TestRetryFailedJobRollsBackStorageFailure(t *testing.T) {
	store := &failingRetryStore{Memory: storage.NewMemory()}
	failed := &xwork.FailedJob{
		ID: newServerTestUUID(t), Name: "job", Queue: "default", Payload: xwork.JobPayload{},
		NextRetryAt: time.Now().Add(time.Hour), ScheduledAt: time.Now(),
	}
	if err := store.InsertToFailed(failed); err != nil {
		t.Fatal(err)
	}

	processor, err := xwork.NewProcessor(store)
	if err != nil {
		t.Fatal(err)
	}
	processor.SetWebActionsEnabled(true)
	mux, err := processor.ServeMux()
	if err != nil {
		t.Fatal(err)
	}

	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/jobs/failed/"+failed.ID.String()+"/retry", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(res, req)
	if res.Code != http.StatusInternalServerError {
		t.Fatalf("expected storage failure to return 500, got %d: %s", res.Code, res.Body.String())
	}
	if found, err := store.GetFailed(failed.ID); err != nil || found == nil {
		t.Fatalf("expected failed job rollback, got job=%v err=%v", found, err)
	}
	if enqueued, err := store.GetFromQueue(failed.Queue); err != nil || enqueued != nil {
		t.Fatalf("expected no queued job after rollback, got job=%v err=%v", enqueued, err)
	}
}

func TestRetryFailedJobRejectsInvalidID(t *testing.T) {
	processor, err := xwork.NewProcessor(storage.NewMemory())
	if err != nil {
		t.Fatal(err)
	}
	processor.SetWebActionsEnabled(true)
	mux, err := processor.ServeMux()
	if err != nil {
		t.Fatal(err)
	}

	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/jobs/failed/not-a-uuid/retry", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(res, req)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected invalid ID to return 400, got %d", res.Code)
	}
}

func newServerTestUUID(t *testing.T) uuid.UUID {
	t.Helper()
	id, err := uuid.NewV4()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func assertDashboardFrameProtection(t *testing.T, res *httptest.ResponseRecorder) {
	t.Helper()
	if got := res.Header().Get("Content-Security-Policy"); got != "frame-ancestors 'none'" {
		t.Fatalf("expected frame-ancestors CSP, got %q", got)
	}
	if got := res.Header().Get("X-Frame-Options"); got != "DENY" {
		t.Fatalf("expected X-Frame-Options DENY, got %q", got)
	}
}

type failingRetryStore struct {
	*storage.Memory
}

type lookupOnlyStore struct {
	xwork.StorageAdapter
}

func (*lookupOnlyStore) GetFailed(uuid.UUID) (*xwork.FailedJob, error) {
	return nil, nil
}

func (s *failingRetryStore) Transact(f func(xwork.StorageAdapter) error) error {
	return s.Memory.Transact(func(adapter xwork.StorageAdapter) error {
		memory, ok := adapter.(*storage.Memory)
		if !ok {
			return errors.New("unexpected transaction adapter")
		}
		return f(&failingRetryStore{Memory: memory})
	})
}

func (s *failingRetryStore) InsertToQueue(*xwork.EnqueuedJob) error {
	return errors.New("queue unavailable")
}
