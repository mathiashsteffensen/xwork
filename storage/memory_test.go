package storage

import (
	"errors"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/mathiashsteffensen/xwork/v2"
)

func TestMemoryGetFromQueueReturnsOldestJobForQueue(t *testing.T) {
	store := NewMemory()
	now := time.Now()

	firstID := newTestUUID(t)
	secondID := newTestUUID(t)

	err := store.InsertToQueue(&xwork.EnqueuedJob{
		ID:          secondID,
		Name:        "newer",
		Queue:       "default",
		EnqueuedAt:  now.Add(time.Minute),
		ScheduledAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.InsertToQueue(&xwork.EnqueuedJob{
		ID:          firstID,
		Name:        "older",
		Queue:       "default",
		EnqueuedAt:  now,
		ScheduledAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}

	job, err := store.GetFromQueue("default")
	if err != nil {
		t.Fatal(err)
	}
	if job == nil {
		t.Fatal("expected job")
	}
	if job.ID != firstID {
		t.Fatalf("expected oldest job %s, got %s", firstID, job.ID)
	}
}

func TestMemoryTransactRollsBackOnError(t *testing.T) {
	store := NewMemory()
	id := newTestUUID(t)
	expectedErr := errors.New("rollback")

	err := store.Transact(func(adapter xwork.StorageAdapter) error {
		err := adapter.InsertToQueue(&xwork.EnqueuedJob{
			ID:          id,
			Name:        "job",
			Queue:       "default",
			EnqueuedAt:  time.Now(),
			ScheduledAt: time.Now(),
		})
		if err != nil {
			return err
		}
		return expectedErr
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected rollback error, got %v", err)
	}

	count, err := store.Count(xwork.JobTypeEnqueued)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("expected no enqueued jobs after rollback, got %d", count)
	}
}

func newTestUUID(t *testing.T) uuid.UUID {
	t.Helper()

	id, err := uuid.NewV4()
	if err != nil {
		t.Fatal(err)
	}
	return id
}
