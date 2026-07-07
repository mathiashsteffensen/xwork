package xwork_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
		if body := res.Body.String(); strings.Contains(body, "fetch(`/") || strings.Contains(body, `fetch("/`) {
			t.Fatal("expected JavaScript fetch calls to use relative URLs")
		}
	})

	t.Run("uses relative browser URLs", func(t *testing.T) {
		res := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xwork/", nil)

		mux.ServeHTTP(res, req)

		if res.Code != http.StatusOK {
			t.Fatalf("expected status %d, got %d", http.StatusOK, res.Code)
		}

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
