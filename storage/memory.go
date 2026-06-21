package storage

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/gofrs/uuid"
	"github.com/mathiashsteffensen/xwork"
)

type Memory struct {
	mu     *sync.Mutex
	state  *memoryState
	locked bool
}

type memoryState struct {
	scheduled           map[uuid.UUID]*xwork.ScheduledJob
	enqueued            map[uuid.UUID]*xwork.EnqueuedJob
	processing          map[uuid.UUID]*xwork.ProcessingJob
	processingHeartbeat map[uuid.UUID]time.Time
	processed           map[uuid.UUID]*xwork.ProcessedJob
	failed              map[uuid.UUID]*xwork.FailedJob
}

func NewMemory() *Memory {
	return &Memory{
		mu:    &sync.Mutex{},
		state: newMemoryState(),
	}
}

func newMemoryState() *memoryState {
	return &memoryState{
		scheduled:           make(map[uuid.UUID]*xwork.ScheduledJob),
		enqueued:            make(map[uuid.UUID]*xwork.EnqueuedJob),
		processing:          make(map[uuid.UUID]*xwork.ProcessingJob),
		processingHeartbeat: make(map[uuid.UUID]time.Time),
		processed:           make(map[uuid.UUID]*xwork.ProcessedJob),
		failed:              make(map[uuid.UUID]*xwork.FailedJob),
	}
}

func (s *memoryState) clone() *memoryState {
	cloned := newMemoryState()

	for id, job := range s.scheduled {
		cloned.scheduled[id] = cloneScheduledJob(job)
	}
	for id, job := range s.enqueued {
		cloned.enqueued[id] = cloneEnqueuedJob(job)
	}
	for id, job := range s.processing {
		cloned.processing[id] = cloneProcessingJob(job)
	}
	for id, heartbeat := range s.processingHeartbeat {
		cloned.processingHeartbeat[id] = heartbeat
	}
	for id, job := range s.processed {
		cloned.processed[id] = cloneProcessedJob(job)
	}
	for id, job := range s.failed {
		cloned.failed[id] = cloneFailedJob(job)
	}

	return cloned
}

func (m *Memory) Transact(f func(adapter xwork.StorageAdapter) error) error {
	if m.locked {
		tx := &Memory{state: m.state.clone(), locked: true}
		if err := f(tx); err != nil {
			return err
		}
		m.state = tx.state
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	tx := &Memory{state: m.state.clone(), locked: true}
	if err := f(tx); err != nil {
		return err
	}

	m.state = tx.state
	return nil
}

func (m *Memory) InsertToScheduled(job *xwork.ScheduledJob) error {
	return m.write(func(s *memoryState) error {
		if _, ok := s.scheduled[job.ID]; ok {
			return duplicateJobError
		}
		s.scheduled[job.ID] = cloneScheduledJob(job)
		return nil
	})
}

func (m *Memory) NextFromScheduled() ([]*xwork.ScheduledJob, error) {
	var jobs []*xwork.ScheduledJob
	err := m.read(func(s *memoryState) error {
		now := time.Now()
		for _, job := range s.scheduled {
			if !job.EnqueueAt.After(now) {
				jobs = append(jobs, cloneScheduledJob(job))
			}
		}
		sort.Slice(jobs, func(i, j int) bool {
			return jobs[i].EnqueueAt.Before(jobs[j].EnqueueAt)
		})
		return nil
	})
	return jobs, err
}

func (m *Memory) DeleteFromScheduled(id uuid.UUID) error {
	return m.write(func(s *memoryState) error {
		delete(s.scheduled, id)
		return nil
	})
}

func (m *Memory) ListScheduled(limit, offset uint) ([]*xwork.ScheduledJob, error) {
	var jobs []*xwork.ScheduledJob
	err := m.read(func(s *memoryState) error {
		for _, job := range s.scheduled {
			jobs = append(jobs, cloneScheduledJob(job))
		}
		sort.Slice(jobs, func(i, j int) bool {
			return jobs[i].EnqueueAt.After(jobs[j].EnqueueAt)
		})
		jobs = paginate(jobs, limit, offset)
		return nil
	})
	return jobs, err
}

func (m *Memory) InsertToQueue(job *xwork.EnqueuedJob) error {
	return m.write(func(s *memoryState) error {
		if _, ok := s.enqueued[job.ID]; ok {
			return duplicateJobError
		}
		s.enqueued[job.ID] = cloneEnqueuedJob(job)
		return nil
	})
}

func (m *Memory) GetFromQueue(queue string) (*xwork.EnqueuedJob, error) {
	var next *xwork.EnqueuedJob
	err := m.read(func(s *memoryState) error {
		for _, job := range s.enqueued {
			if job.Queue != queue {
				continue
			}
			if next == nil || job.EnqueuedAt.Before(next.EnqueuedAt) {
				next = cloneEnqueuedJob(job)
			}
		}
		return nil
	})
	return next, err
}

func (m *Memory) DeleteFromQueue(id uuid.UUID) error {
	return m.write(func(s *memoryState) error {
		delete(s.enqueued, id)
		return nil
	})
}

func (m *Memory) ListEnqueued(queue string, limit, offset uint) ([]*xwork.EnqueuedJob, error) {
	var jobs []*xwork.EnqueuedJob
	err := m.read(func(s *memoryState) error {
		for _, job := range s.enqueued {
			if job.Queue == queue {
				jobs = append(jobs, cloneEnqueuedJob(job))
			}
		}
		sort.Slice(jobs, func(i, j int) bool {
			return jobs[i].EnqueuedAt.Before(jobs[j].EnqueuedAt)
		})
		jobs = paginate(jobs, limit, offset)
		return nil
	})
	return jobs, err
}

func (m *Memory) InsertToProcessing(job *xwork.ProcessingJob) error {
	return m.write(func(s *memoryState) error {
		if _, ok := s.processing[job.ID]; ok {
			return duplicateJobError
		}
		s.processing[job.ID] = cloneProcessingJob(job)
		s.processingHeartbeat[job.ID] = job.StartedAt
		return nil
	})
}

func (m *Memory) EmitHeartbeat(job *xwork.ProcessingJob) error {
	return m.write(func(s *memoryState) error {
		s.processingHeartbeat[job.ID] = time.Now()
		return nil
	})
}

func (m *Memory) GetByLastHeartbeatBefore(timestamp time.Time) ([]*xwork.ProcessingJob, error) {
	var jobs []*xwork.ProcessingJob
	err := m.read(func(s *memoryState) error {
		for id, heartbeat := range s.processingHeartbeat {
			if heartbeat.Before(timestamp) {
				if job, ok := s.processing[id]; ok {
					jobs = append(jobs, cloneProcessingJob(job))
				}
			}
		}
		return nil
	})
	return jobs, err
}

func (m *Memory) DeleteFromProcessing(id uuid.UUID) error {
	return m.write(func(s *memoryState) error {
		delete(s.processing, id)
		delete(s.processingHeartbeat, id)
		return nil
	})
}

func (m *Memory) ListProcessing(limit, offset uint) ([]*xwork.ProcessingJob, error) {
	var jobs []*xwork.ProcessingJob
	err := m.read(func(s *memoryState) error {
		for _, job := range s.processing {
			jobs = append(jobs, cloneProcessingJob(job))
		}
		sort.Slice(jobs, func(i, j int) bool {
			return jobs[i].EnqueuedAt.After(jobs[j].EnqueuedAt)
		})
		jobs = paginate(jobs, limit, offset)
		return nil
	})
	return jobs, err
}

func (m *Memory) InsertToProcessed(job *xwork.ProcessedJob) error {
	return m.write(func(s *memoryState) error {
		if _, ok := s.processed[job.ID]; ok {
			return duplicateJobError
		}
		s.processed[job.ID] = cloneProcessedJob(job)
		return nil
	})
}

func (m *Memory) ListProcessed(limit, offset uint) ([]*xwork.ProcessedJob, error) {
	var jobs []*xwork.ProcessedJob
	err := m.read(func(s *memoryState) error {
		for _, job := range s.processed {
			jobs = append(jobs, cloneProcessedJob(job))
		}
		sort.Slice(jobs, func(i, j int) bool {
			return jobs[i].EnqueuedAt.Before(jobs[j].EnqueuedAt)
		})
		jobs = paginate(jobs, limit, offset)
		return nil
	})
	return jobs, err
}

func (m *Memory) InsertToFailed(job *xwork.FailedJob) error {
	return m.write(func(s *memoryState) error {
		if _, ok := s.failed[job.ID]; ok {
			return duplicateJobError
		}
		s.failed[job.ID] = cloneFailedJob(job)
		return nil
	})
}

func (m *Memory) NextFromFailed() ([]*xwork.FailedJob, error) {
	var jobs []*xwork.FailedJob
	err := m.read(func(s *memoryState) error {
		now := time.Now()
		for _, job := range s.failed {
			if !job.NextRetryAt.After(now) {
				jobs = append(jobs, cloneFailedJob(job))
			}
		}
		sort.Slice(jobs, func(i, j int) bool {
			return jobs[i].NextRetryAt.Before(jobs[j].NextRetryAt)
		})
		return nil
	})
	return jobs, err
}

func (m *Memory) DeleteFromFailed(id uuid.UUID) error {
	return m.write(func(s *memoryState) error {
		delete(s.failed, id)
		return nil
	})
}

func (m *Memory) ListFailed(limit, offset uint) ([]*xwork.FailedJob, error) {
	var jobs []*xwork.FailedJob
	err := m.read(func(s *memoryState) error {
		for _, job := range s.failed {
			jobs = append(jobs, cloneFailedJob(job))
		}
		sort.Slice(jobs, func(i, j int) bool {
			return jobs[i].NextRetryAt.After(jobs[j].NextRetryAt)
		})
		jobs = paginate(jobs, limit, offset)
		return nil
	})
	return jobs, err
}

func (m *Memory) Count(jobType xwork.JobType) (int64, error) {
	var count int64
	err := m.read(func(s *memoryState) error {
		switch jobType {
		case xwork.JobTypeScheduled:
			count = int64(len(s.scheduled))
		case xwork.JobTypeEnqueued:
			count = int64(len(s.enqueued))
		case xwork.JobTypeProcessing:
			count = int64(len(s.processing))
		case xwork.JobTypeProcessed:
			count = int64(len(s.processed))
		case xwork.JobTypeFailed:
			count = int64(len(s.failed))
		default:
			return errors.New("unknown job type")
		}
		return nil
	})
	return count, err
}

func (m *Memory) read(f func(*memoryState) error) error {
	if m.locked {
		return f(m.state)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	return f(m.state)
}

func (m *Memory) write(f func(*memoryState) error) error {
	return m.read(f)
}

var duplicateJobError = errors.New("duplicate job id")

func cloneScheduledJob(job *xwork.ScheduledJob) *xwork.ScheduledJob {
	cloned := *job
	cloned.Payload = clonePayload(job.Payload)
	return &cloned
}

func cloneEnqueuedJob(job *xwork.EnqueuedJob) *xwork.EnqueuedJob {
	cloned := *job
	cloned.Payload = clonePayload(job.Payload)
	return &cloned
}

func cloneProcessingJob(job *xwork.ProcessingJob) *xwork.ProcessingJob {
	cloned := *job
	cloned.Payload = clonePayload(job.Payload)
	return &cloned
}

func cloneProcessedJob(job *xwork.ProcessedJob) *xwork.ProcessedJob {
	cloned := *job
	cloned.Payload = clonePayload(job.Payload)
	return &cloned
}

func cloneFailedJob(job *xwork.FailedJob) *xwork.FailedJob {
	cloned := *job
	cloned.Payload = clonePayload(job.Payload)
	return &cloned
}

func clonePayload(payload xwork.JobPayload) xwork.JobPayload {
	if payload == nil {
		return nil
	}

	cloned := make(xwork.JobPayload, len(payload))
	for key, value := range payload {
		cloned[key] = value
	}
	return cloned
}

func paginate[T any](items []T, limit, offset uint) []T {
	if limit == 0 {
		limit = 100
	}
	if offset >= uint(len(items)) {
		return items[:0]
	}

	start := int(offset)
	end := start + int(limit)
	if end > len(items) {
		end = len(items)
	}

	return items[start:end]
}
