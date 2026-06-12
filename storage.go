package xwork

import "github.com/gofrs/uuid"

type StorageAdapter interface {
	Transact(func(adapter StorageAdapter) error) error
	InsertToScheduled(job *ScheduledJob) error
	NextFromScheduled() ([]*ScheduledJob, error)
	DeleteFromScheduled(id uuid.UUID) error
	InsertToQueue(job *EnqueuedJob) error
	GetFromQueue(queue string) (*EnqueuedJob, error)
	DeleteFromQueue(id uuid.UUID) error
	InsertToProcessing(job *ProcessingJob) error
	DeleteFromProcessing(id uuid.UUID) error
	InsertToProcessed(job *ProcessedJob) error
	InsertToFailed(job *FailedJob) error
	NextFromFailed() ([]*FailedJob, error)
	DeleteFromFailed(id uuid.UUID) error
}

type Initializer interface {
	Initialize() error
}

var storage StorageAdapter

func SetStorageAdapter(s StorageAdapter) error {
	if s, ok := s.(Initializer); ok {
		err := s.Initialize()
		if err != nil {
			return err
		}
	}

	storage = s

	return nil
}
