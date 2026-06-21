package xwork

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/gofrs/uuid"
)

type JobPayload map[string]any

func (p JobPayload) Value() (any, error) {
	j, err := json.Marshal(p)
	return j, err
}

func (p *JobPayload) Scan(src interface{}) error {
	source, ok := src.([]byte)
	if !ok {
		return errors.New("type assertion .([]byte) failed")
	}

	return json.Unmarshal(source, p)
}

// Bind converts a job payload into a typed value using JSON tags.
func Bind[TPayload any](payload JobPayload) (TPayload, error) {
	var val TPayload

	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		return val, err
	}
	err = json.Unmarshal(jsonBytes, &val)
	return val, err
}

type JobHandler = func(job *ProcessingJob) error

type JobDefinition struct {
	Name    string
	Queue   string
	Handler JobHandler

	/* Internal */
	processor *Processor
}

type JobDefinitions map[string]*JobDefinition

func (defs JobDefinitions) set(name string, definition *JobDefinition) {
	defs[name] = definition
}

func (defs JobDefinitions) get(name string) *JobDefinition {
	return defs[name]
}

func (def JobDefinition) Enqueue(payload JobPayload) error {
	return def.processor.Enqueue(def.Name, payload)
}

func (def JobDefinition) EnqueueIn(duration time.Duration, payload JobPayload) error {
	return def.processor.EnqueueIn(def.Name, duration, payload)
}

func (def JobDefinition) EnqueueAt(enqueueAt time.Time, payload JobPayload) error {
	return def.processor.EnqueueAt(def.Name, enqueueAt, payload)
}

type ScheduledJob struct {
	ID          uuid.UUID  `json:"id"`
	Name        string     `json:"name"`
	Queue       string     `json:"queue"`
	Payload     JobPayload `json:"payload"`
	EnqueueAt   time.Time  `json:"enqueueAt"`
	ScheduledAt time.Time  `json:"scheduledAt"`
}

type EnqueuedJob struct {
	ID          uuid.UUID  `json:"id"`
	Name        string     `json:"name"`
	Queue       string     `json:"queue"`
	Payload     JobPayload `json:"payload"`
	RetryCount  int        `json:"retryCount"`
	EnqueuedAt  time.Time  `json:"enqueuedAt"`
	ScheduledAt time.Time  `json:"scheduledAt"`
}

type ProcessingJob struct {
	ID          uuid.UUID  `json:"id"`
	Name        string     `json:"name"`
	Queue       string     `json:"queue"`
	Payload     JobPayload `json:"payload"`
	RetryCount  int        `json:"retryCount"`
	StartedAt   time.Time  `json:"startedAt"`
	EnqueuedAt  time.Time  `json:"enqueuedAt"`
	ScheduledAt time.Time  `json:"scheduledAt"`
}

type Job = ProcessingJob

type ProcessedJob struct {
	ID          uuid.UUID  `json:"id"`
	Name        string     `json:"name"`
	Queue       string     `json:"queue"`
	Payload     JobPayload `json:"payload"`
	StartedAt   time.Time  `json:"startedAt"`
	CompletedAt time.Time  `json:"completedAt"`
	EnqueuedAt  time.Time  `json:"enqueuedAt"`
	ScheduledAt time.Time  `json:"scheduledAt"`
}

type FailedJob struct {
	ID          uuid.UUID  `json:"id"`
	Name        string     `json:"name"`
	Queue       string     `json:"queue"`
	Payload     JobPayload `json:"payload"`
	Error       string     `json:"error"`
	RetryCount  int        `json:"retryCount"`
	LastRetryAt time.Time  `json:"lastRetryAt"`
	NextRetryAt time.Time  `json:"nextRetryAt"`
	ScheduledAt time.Time  `json:"scheduledAt"`
}
