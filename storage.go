package xwork

import (
	"time"

	"github.com/gofrs/uuid"
)

type JobType string

const (
	JobTypeScheduled  JobType = "scheduled"
	JobTypeEnqueued   JobType = "enqueued"
	JobTypeProcessing JobType = "processing"
	JobTypeProcessed  JobType = "processed"
	JobTypeFailed     JobType = "failed"
)

type StorageAdapter interface {
	Transact(func(adapter StorageAdapter) error) error

	InsertToScheduled(job *ScheduledJob) error
	NextFromScheduled() ([]*ScheduledJob, error)
	DeleteFromScheduled(id uuid.UUID) error
	ListScheduled(limit, offset uint) ([]*ScheduledJob, error)

	InsertToQueue(job *EnqueuedJob) error
	GetFromQueue(queue string) (*EnqueuedJob, error)
	DeleteFromQueue(id uuid.UUID) error
	ListEnqueued(queue string, limit, offset uint) ([]*EnqueuedJob, error)

	InsertToProcessing(job *ProcessingJob) error
	EmitHeartbeat(job *ProcessingJob) error
	GetByLastHeartbeatBefore(timestamp time.Time) ([]*ProcessingJob, error)
	DeleteFromProcessing(id uuid.UUID) error
	ListProcessing(limit, offset uint) ([]*ProcessingJob, error)

	InsertToProcessed(job *ProcessedJob) error
	ListProcessed(limit, offset uint) ([]*ProcessedJob, error)

	InsertToFailed(job *FailedJob) error
	NextFromFailed() ([]*FailedJob, error)
	DeleteFromFailed(id uuid.UUID) error
	ListFailed(limit, offset uint) ([]*FailedJob, error)

	Count(jobType JobType) (int64, error)
}

type Initializer interface {
	Initialize() error
}
