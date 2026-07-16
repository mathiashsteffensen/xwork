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

// JobQuery describes the optional filtering and pagination supported by the
// web UI. Query matches a job's name or ID, while Queue is an exact match.
type JobQuery struct {
	Query string
	Queue string
	// AllQueues prevents the web API's legacy default-queue filter for
	// enqueued jobs when no explicit Queue is provided.
	AllQueues bool
	Limit     uint
	Offset    uint
}

// JobQueryAdapter lets storage adapters provide richer web UI queries without
// adding methods to StorageAdapter and breaking existing implementations.
type JobQueryAdapter interface {
	ListJobs(jobType JobType, query JobQuery) (jobs any, hasMore bool, err error)
}

// FailedJobLookup provides an optional failed-job lookup.
type FailedJobLookup interface {
	GetFailed(id uuid.UUID) (*FailedJob, error)
}

// FailedJobClaimer atomically removes and returns a failed job. A nil job
// means another caller already claimed it. Implementations used inside
// Transact must expose this interface too.
type FailedJobClaimer interface {
	ClaimFailed(id uuid.UUID) (*FailedJob, error)
}

type Initializer interface {
	Initialize() error
}
