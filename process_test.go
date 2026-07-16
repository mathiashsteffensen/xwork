package xwork

import (
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/alitto/pond"
	"github.com/gofrs/uuid"
	"github.com/sirupsen/logrus"
)

func TestShutdownDoesNotBlockSendingQuitSignals(t *testing.T) {
	p := &Processor{
		logger:         logrus.New(),
		killTimeout:    2 * RequeueTimeout,
		pool:           pond.New(1, 0),
		processingJobs: NewAtomicMap[*Job](),
		quit:           make(chan struct{}),
	}

	done := make(chan struct{})
	go func() {
		p.Shutdown(syscall.SIGINT)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown blocked while notifying managed goroutines")
	}

	// Repeated shutdown calls must not panic by closing the channel again.
	p.Shutdown(syscall.SIGINT)
}

func TestCleanStackTraceExcludesLibraryFrames(t *testing.T) {
	stack := []byte(`goroutine 1 [running]:
runtime/debug.Stack()
	/usr/local/go/src/runtime/debug/stack.go:26 +0x5e
github.com/mathiashsteffensen/xwork/v2.(*Processor).processJob.func1()
	/Users/mathias/code/xwork/process.go:354 +0x45
panic({0x102, 0x203})
	/usr/local/go/src/runtime/panic.go:783 +0x132
github.com/example/app.runJob()
	/Users/mathias/code/app/jobs.go:42 +0x12
github.com/mathiashsteffensen/xwork/v2.(*Processor).processJob(0x1, 0x2)
	/Users/mathias/code/xwork/process.go:367 +0x3a
github.com/alitto/pond.(*WorkerPool).executeTask()
	/Users/mathias/go/pkg/mod/github.com/alitto/pond/pool.go:123 +0x4
runtime.goexit()
	/usr/local/go/src/runtime/asm_arm64.s:1268 +0x1
`)

	cleaned := cleanStackTrace(stack)

	if !strings.Contains(cleaned, "github.com/example/app.runJob()") {
		t.Fatalf("expected user frame, got:\n%s", cleaned)
	}

	for _, unwanted := range []string{
		"runtime/debug.Stack",
		"github.com/mathiashsteffensen/xwork/v2.(*Processor).",
		"github.com/alitto/pond.",
		"runtime.goexit",
		"panic(",
	} {
		if strings.Contains(cleaned, unwanted) {
			t.Fatalf("expected cleaned stack to exclude %q, got:\n%s", unwanted, cleaned)
		}
	}
}

func TestAutomaticRetryUsesAtomicClaim(t *testing.T) {
	id, err := uuid.NewV4()
	if err != nil {
		t.Fatal(err)
	}
	claimed := &FailedJob{
		ID: id, Name: "authoritative", Queue: "critical", Payload: JobPayload{"source": "claim"},
		RetryCount: 3, ScheduledAt: time.Now().Add(-time.Hour),
	}
	store := &automaticClaimStorage{claimed: claimed}
	processor := &Processor{storage: store}

	stale := &FailedJob{ID: id, Name: "stale", Queue: "default", Payload: JobPayload{"source": "stale"}}
	if err := processor.retry(stale); err != nil {
		t.Fatal(err)
	}
	if store.claimCalls != 1 || store.deleteCalls != 0 {
		t.Fatalf("expected atomic claim without legacy delete, claim=%d delete=%d", store.claimCalls, store.deleteCalls)
	}
	if store.enqueued == nil || store.enqueued.Name != claimed.Name || store.enqueued.Queue != claimed.Queue || store.enqueued.RetryCount != claimed.RetryCount {
		t.Fatalf("expected claimed job to be enqueued, got %+v", store.enqueued)
	}

	store.enqueued = nil
	if err := processor.retry(stale); err != nil {
		t.Fatal(err)
	}
	if store.enqueued != nil {
		t.Fatalf("expected a stale retry to stop after a lost claim, got %+v", store.enqueued)
	}
}

func TestAutomaticRetryDropsFailedJobAlreadyInQueue(t *testing.T) {
	id, err := uuid.NewV4()
	if err != nil {
		t.Fatal(err)
	}
	failed := &FailedJob{ID: id, Name: "job", Queue: "default", Payload: JobPayload{}}
	store := &duplicateQueueStorage{
		automaticClaimStorage: automaticClaimStorage{claimed: failed},
	}
	processor := &Processor{storage: store}

	if err := processor.retry(failed); err != nil {
		t.Fatal(err)
	}
	if store.claimed != nil {
		t.Fatal("expected failed job to be removed after the claim")
	}
	if store.insertCalls != 1 || store.enqueued != nil {
		t.Fatalf("expected duplicate queue insert to be ignored, calls=%d enqueued=%+v", store.insertCalls, store.enqueued)
	}
}

type automaticClaimStorage struct {
	StorageAdapter
	claimed     *FailedJob
	enqueued    *EnqueuedJob
	claimCalls  int
	deleteCalls int
}

func (s *automaticClaimStorage) Transact(f func(StorageAdapter) error) error {
	return f(s)
}

func (s *automaticClaimStorage) ClaimFailed(uuid.UUID) (*FailedJob, error) {
	s.claimCalls++
	claimed := s.claimed
	s.claimed = nil
	return claimed, nil
}

func (s *automaticClaimStorage) DeleteFromFailed(uuid.UUID) error {
	s.deleteCalls++
	return nil
}

func (s *automaticClaimStorage) InsertToQueue(job *EnqueuedJob) error {
	s.enqueued = job
	return nil
}

type duplicateQueueStorage struct {
	automaticClaimStorage
	insertCalls int
}

func (s *duplicateQueueStorage) Transact(f func(StorageAdapter) error) error {
	return f(s)
}

func (s *duplicateQueueStorage) InsertToQueue(*EnqueuedJob) error {
	s.insertCalls++
	return ErrAlreadyEnqueued
}
