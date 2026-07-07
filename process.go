package xwork

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	"github.com/alitto/pond"
	"github.com/gofrs/uuid"
	"github.com/sirupsen/logrus"
)

const (
	DefaultConcurrency = 5
	DefaultQueue       = "default"
)

type Processor struct {
	/* Configurable values */
	logger      logrus.FieldLogger
	storage     StorageAdapter
	concurrency int
	killTimeout time.Duration

	/* Internal values */
	registeredQueues []string
	jobDefinitions   JobDefinitions

	pool *pond.WorkerPool

	// These are the jobs currently in memory in this process
	// They are also stored in whatever StorageAdapter is being used,
	// but we keep them in memory and try to push them back if they don't finish when the process is shutting down
	processingJobs AtomicMap[*Job]

	quit                 chan struct{}
	numManagedGoRoutines int32
}

func NewProcessor(storage StorageAdapter) (*Processor, error) {
	if s, ok := storage.(Initializer); ok {
		err := s.Initialize()
		if err != nil {
			return nil, err
		}
	}

	return &Processor{
		logger:      logrus.New().WithField("package", "xwork"),
		storage:     storage,
		concurrency: DefaultConcurrency,
		killTimeout: 15 * time.Second,

		registeredQueues:     make([]string, 0),
		jobDefinitions:       make(JobDefinitions),
		processingJobs:       NewAtomicMap[*Job](),
		quit:                 make(chan struct{}),
		numManagedGoRoutines: 0,
	}, nil
}

func (p *Processor) SetLogger(fieldLogger logrus.FieldLogger) {
	p.logger = fieldLogger
}

func (p *Processor) SetConcurrency(concurrency int) {
	p.concurrency = concurrency
}

func (p *Processor) SetKillTimeout(killTimeout time.Duration) {
	p.killTimeout = killTimeout
}

func (p *Processor) DefineJob(queue string, name string, handler JobHandler) *JobDefinition {
	def := &JobDefinition{
		Name:      name,
		Handler:   handler,
		Queue:     queue,
		processor: p,
	}

	var hasQueue bool
	for _, registeredQueue := range p.registeredQueues {
		if registeredQueue == queue {
			hasQueue = true
		}
	}

	if !hasQueue {
		p.registeredQueues = append(p.registeredQueues, queue)
	}

	p.jobDefinitions.set(name, def)

	return def
}

func (p *Processor) Enqueue(name string, payload JobPayload) error {
	return p.EnqueueAt(name, time.Now(), payload)
}

func (p *Processor) EnqueueIn(name string, duration time.Duration, payload JobPayload) error {
	return p.EnqueueAt(name, time.Now().Add(duration), payload)
}

func (p *Processor) EnqueueAt(name string, enqueueAt time.Time, payload JobPayload) error {
	def := p.jobDefinitions.get(name)
	if def == nil {
		return errors.New("job definition not found")
	}

	id, err := uuid.NewV4()
	if err != nil {
		return err
	}

	p.logger.Infof("Enqueueing job '%s:%s' at %s", def.Queue, def.Name, enqueueAt)

	return p.enqueue(&ScheduledJob{
		ID:          id,
		Name:        def.Name,
		Queue:       def.Queue,
		EnqueueAt:   enqueueAt,
		ScheduledAt: time.Now(),
		Payload:     payload,
	})
}

func (p *Processor) Process(queues ...string) {
	if len(queues) == 0 {
		queues = p.registeredQueues
	}

	if len(queues) == 0 {
		queues = []string{DefaultQueue}
	}

	p.logger.
		WithField("concurrency", p.concurrency).
		WithField("queues", strings.Join(queues, ", ")).
		Info("Starting background work processor")

	p.pool = pond.New(p.concurrency, 0)

	go p.enqueueScheduledJobs()
	go p.requeueFailedJobs()
	go p.emitHeartbeats()
	go p.checkForOrphanedJobs()

	atomic.AddInt32(&p.numManagedGoRoutines, 4)

	for _, queue := range queues {
		go p.processQueue(queue)
		atomic.AddInt32(&p.numManagedGoRoutines, 1)
	}

	p.WaitForShutdown()
}

func (p *Processor) WaitForShutdown() {
	s := <-newShutdownChannel()
	p.Shutdown(s)
}

const RequeueTimeout = time.Second

func (p *Processor) Shutdown(signal os.Signal) {
	p.logger.WithField("signal", signal).Warn("Shutting down background work processor")

	for i := 0; int32(i) < p.numManagedGoRoutines+1; i++ {
		// Gracefully shutdown all managed goroutines
		p.quit <- struct{}{}
	}

	atomic.StoreInt32(&p.numManagedGoRoutines, 0)

	p.logger.WithField("timeout", p.killTimeout).Info("Allowing jobs to finish processing")
	p.pool.StopAndWaitFor(p.killTimeout - RequeueTimeout)

	// Try to reschedule jobs that didn't finish processing
	p.processingJobs.Each(func(_ string, job *Job) {
		p.logger.Warnf("Attempting to requeue interrupted job %s", job.ID)

		err := p.requeue(job)
		if err != nil {
			p.logger.WithError(err).WithField("id", job.ID).WithField("payload", job.Payload).Error("failed to requeue job")
		}
	})

	time.Sleep(RequeueTimeout)
}

const PollTickerTimeout = 10 * time.Millisecond
const HeartbeatTickerTimeout = 3 * time.Second

var ErrOrphanJob = errors.New("orphan job")

func (p *Processor) checkForOrphanedJobs() {
	p.logger.Debug("Checking for orphaned jobs")

	ticker := time.NewTicker(PollTickerTimeout)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				orphanedJobs, err := p.storage.GetByLastHeartbeatBefore(time.Now().Add(-HeartbeatTickerTimeout * 3))
				if err != nil {
					p.logger.WithError(err).Warn("Failed to get orphaned jobs")
				}

				if len(orphanedJobs) > 0 {
					jobIds := make([]string, len(orphanedJobs))
					for i, job := range orphanedJobs {
						jobIds[i] = job.ID.String()
					}
					p.logger.Debugf("Found orphaned jobs: %v", jobIds)
				}

				for _, job := range orphanedJobs {
					err := p.fail(job, ErrOrphanJob)
					if err != nil {
						p.logger.WithError(err).WithField("job_id", job.ID).Warn("Failed to fail job")
						return
					}
				}
			case <-p.quit:
				p.logger.Debug("Shutting down checking for orphaned jobs")
				return
			}
		}
	}()
}

func (p *Processor) emitHeartbeats() {
	p.logger.Debug("Starting heartbeats emitter")

	ticker := time.NewTicker(HeartbeatTickerTimeout)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				p.processingJobs.Each(func(_ string, job *Job) {
					err := p.storage.EmitHeartbeat(job)
					if err != nil {
						p.logger.WithError(err).Error("failed to emit heartbeat")
						return
					}
				})
			case <-p.quit:
				p.logger.Debug("Shutting down heartbeats emitter")
				return
			}
		}
	}()
}

func (p *Processor) enqueueScheduledJobs() {
	p.logger.Debug("Enqueueing scheduled jobs")

	ticker := time.NewTicker(PollTickerTimeout)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				err := p.enqueueReadyScheduledJobs()
				if err != nil {
					p.logger.WithError(err).Error("failed to enqueue scheduled jobs")
					continue
				}
			case <-p.quit:
				p.logger.Debug("Shutting down enqueueing scheduled jobs")
				return
			}
		}
	}()

}

func (p *Processor) requeueFailedJobs() {
	p.logger.Debug("Requeueing failed jobs")

	ticker := time.NewTicker(PollTickerTimeout)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				err := p.enqueueReadyFailedJobs()
				if err != nil {
					p.logger.WithError(err).Error("failed to requeue failed jobs")
					continue
				}
			case <-p.quit:
				p.logger.Debug("Shutting down requeueing failed jobs")
				return
			}
		}
	}()
}

func (p *Processor) processQueue(queue string) {
	p.logger.Infof("Processing queue %q", queue)

	ticker := time.NewTicker(PollTickerTimeout)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				p.pool.TrySubmit(func() {
					job, err := p.dequeue(queue)
					if err != nil {
						p.logger.WithError(err).Error("failed to dequeue job")
						return
					}

					if job == nil {
						return
					}

					p.processJob(job)
				})
			case <-p.quit:
				p.logger.Infof("Stopping queue %q", queue)
				return
			}
		}
	}()
}

func (p *Processor) processJob(job *Job) {
	p.processingJobs.Set(job.ID.String(), job)
	defer p.processingJobs.Delete(job.ID.String())

	jobDefinition := p.jobDefinitions.get(job.Name)

	l := p.logger.WithFields(map[string]any{
		"id":    job.ID,
		"queue": job.Queue,
		"name":  job.Name,
	})

	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			l.WithField("panic", r).Error("PANIC")
			err := p.failWithStack(job, fmt.Errorf("panic: %v", r), stack)
			if err != nil {
				l.WithError(err).Error("failed to send job to failed queue")
			}
		}
	}()

	l.Info("START")

	start := time.Now()

	err := jobDefinition.Handler(job)

	l = l.WithField("duration", time.Since(start).String())

	if err != nil {
		l.WithError(err).Error("FAILED")

		err = p.fail(job, err)
		if err != nil {
			l.WithError(err).Error("failed to send job to failed queue")
		}

		return
	}

	l.Info("COMPLETED")

	err = p.complete(job)
	if err != nil {
		l.WithError(err).Error("failed to complete job")

		err = p.fail(job, err)
		if err != nil {
			l.WithError(err).Error("failed to send job to failed queue")
		}

		return
	}
}

func (p *Processor) enqueue(job *ScheduledJob) error {
	if job.EnqueueAt.Before(time.Now()) {
		return p.storage.InsertToQueue(&EnqueuedJob{
			ID:          job.ID,
			Name:        job.Name,
			Queue:       job.Queue,
			Payload:     job.Payload,
			EnqueuedAt:  time.Now(),
			ScheduledAt: job.ScheduledAt,
		})
	}

	return p.storage.InsertToScheduled(job)
}

func (p *Processor) dequeue(queue string) (*ProcessingJob, error) {
	var job *ProcessingJob
	err := p.storage.Transact(func(storage StorageAdapter) error {
		enqueuedJob, err := storage.GetFromQueue(queue)
		if err != nil {
			p.logger.Debugf("failed to get job from queue %q: %v", queue, err)
			return err
		}

		if enqueuedJob == nil {
			return nil
		}

		err = storage.DeleteFromQueue(enqueuedJob.ID)
		if err != nil {
			return err
		}

		job = &ProcessingJob{
			ID:          enqueuedJob.ID,
			Name:        enqueuedJob.Name,
			Queue:       enqueuedJob.Queue,
			Payload:     enqueuedJob.Payload,
			RetryCount:  enqueuedJob.RetryCount,
			StartedAt:   time.Now(),
			EnqueuedAt:  enqueuedJob.EnqueuedAt,
			ScheduledAt: enqueuedJob.ScheduledAt,
		}

		return storage.InsertToProcessing(job)
	})
	if err != nil {
		return nil, err
	}

	return job, nil
}

func (p *Processor) complete(job *Job) error {
	return p.storage.Transact(func(storage StorageAdapter) error {
		processedJob := &ProcessedJob{
			ID:          job.ID,
			Name:        job.Name,
			Queue:       job.Queue,
			Payload:     job.Payload,
			StartedAt:   job.StartedAt,
			CompletedAt: time.Now(),
			EnqueuedAt:  job.EnqueuedAt,
			ScheduledAt: job.ScheduledAt,
		}

		err := storage.InsertToProcessed(processedJob)
		if err != nil {
			return err
		}

		return storage.DeleteFromProcessing(job.ID)
	})
}

func (p *Processor) requeue(job *Job) error {
	return p.storage.Transact(func(storage StorageAdapter) error {
		err := storage.InsertToQueue(&EnqueuedJob{
			ID:          job.ID,
			Name:        job.Name,
			Queue:       job.Queue,
			Payload:     job.Payload,
			EnqueuedAt:  time.Now(),
			ScheduledAt: job.ScheduledAt,
		})
		if err != nil {
			return err
		}

		return storage.DeleteFromProcessing(job.ID)
	})
}

func (p *Processor) fail(job *Job, jobErr error) error {
	return p.failWithStack(job, jobErr, nil)
}

func (p *Processor) failWithStack(job *Job, jobErr error, stack []byte) error {
	return p.storage.Transact(func(storage StorageAdapter) error {
		err := storage.DeleteFromProcessing(job.ID)
		if err != nil {
			return err
		}

		retryCount := job.RetryCount + 1

		if retryCount > DefaultMaxRetryCount {
			fmt.Printf(
				"WARNING: Job(%s) of type %s:%s is being discarded after %d retries",
				job.ID,
				job.Queue,
				job.Name,
				job.RetryCount,
			)

			return nil
		}

		now := time.Now()

		jobErrData := map[string]any{"message": jobErr.Error()}
		if stacktrace := cleanStackTrace(stack); stacktrace != "" {
			jobErrData["stacktrace"] = stacktrace
		}

		jobErrJson, err := json.Marshal(jobErrData)
		if err != nil {
			jobErrJson = []byte(fmt.Sprintf(`{"message":"%s"}`, jobErr.Error()))
			err = nil
		}

		return storage.InsertToFailed(&FailedJob{
			ID:          job.ID,
			Name:        job.Name,
			Queue:       job.Queue,
			Payload:     job.Payload,
			Error:       string(jobErrJson),
			LastRetryAt: now,
			NextRetryAt: now.Add(exponentialBackoff(retryCount)),
			RetryCount:  retryCount,
			ScheduledAt: job.ScheduledAt,
		})
	})
}

func cleanStackTrace(stack []byte) string {
	if len(stack) == 0 {
		return ""
	}

	lines := strings.Split(strings.TrimRight(string(stack), "\n"), "\n")
	if len(lines) == 0 {
		return ""
	}

	filtered := []string{lines[0]}
	for i := 1; i < len(lines); i += 2 {
		funcLine := lines[i]
		fileLine := ""
		if i+1 < len(lines) {
			fileLine = lines[i+1]
		}

		if isLibraryStackFrame(funcLine) {
			continue
		}

		filtered = append(filtered, funcLine)
		if fileLine != "" {
			filtered = append(filtered, fileLine)
		}
	}

	if len(filtered) == 1 {
		return ""
	}

	return strings.Join(filtered, "\n")
}

func isLibraryStackFrame(funcLine string) bool {
	libraryPrefixes := []string{
		"runtime/debug.Stack",
		"panic(",
		"github.com/mathiashsteffensen/xwork/v2.(*Processor).",
		"github.com/alitto/pond.",
		"runtime.goexit",
	}

	for _, prefix := range libraryPrefixes {
		if strings.HasPrefix(funcLine, prefix) {
			return true
		}
	}

	return false
}

func (p *Processor) retry(job *FailedJob) error {
	return p.storage.Transact(func(storage StorageAdapter) error {
		err := storage.DeleteFromFailed(job.ID)
		if err != nil {
			return err
		}

		return storage.InsertToQueue(&EnqueuedJob{
			ID:          job.ID,
			Name:        job.Name,
			Queue:       job.Queue,
			Payload:     job.Payload,
			RetryCount:  job.RetryCount,
			EnqueuedAt:  time.Now(),
			ScheduledAt: job.ScheduledAt,
		})
	})
}

func (p *Processor) enqueueReadyFailedJobs() error {
	jobs, err := p.storage.NextFromFailed()
	if err != nil {
		return err
	}

	for _, job := range jobs {
		err = p.retry(job)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Processor) enqueueScheduled(job *ScheduledJob) error {
	return p.storage.Transact(func(storage StorageAdapter) error {
		err := storage.DeleteFromScheduled(job.ID)
		if err != nil {
			return err
		}

		return storage.InsertToQueue(&EnqueuedJob{
			ID:          job.ID,
			Name:        job.Name,
			Queue:       job.Queue,
			Payload:     job.Payload,
			EnqueuedAt:  time.Now(),
			ScheduledAt: job.ScheduledAt,
		})
	})
}

func (p *Processor) enqueueReadyScheduledJobs() error {
	jobs, err := p.storage.NextFromScheduled()
	if err != nil {
		return err
	}

	for _, job := range jobs {
		if err := p.enqueueScheduled(job); err != nil {
			return err
		}
	}

	return nil
}

const DefaultMaxRetryCount = 19

// Retry 1: 1 minute
// Retry 2: 8 minutes - 9 minutes total
// Retry 3: 27 minutes - 36 minutes total
// Retry 4: 64 minutes - 100 minutes total
// Retry 5: 125 minutes - 225 minutes total
// Retry 6: 216 minutes - 441 minutes total
// Retry 7: 343 minutes - 784 minutes total
// Retry 8: 512 minutes - 1296 minutes total
// Retry 9: 729 minutes - 2025 minutes total
// Retry 10: 1000 minutes - 3025 minutes total
// Retry 11: 1331 minutes - 4356 minutes total
// Retry 12: 1728 minutes - 6084 minutes total
// Retry 13: 2197 minutes - 8281 minutes total
// Retry 14: 2744 minutes - 11025 minutes total
// Retry 15: 3375 minutes - 14400 minutes total
// Retry 16: 4096 minutes - 18496 minutes total
// Retry 17: 4913 minutes - 23409 minutes total
// Retry 18: 5832 minutes - 29241 minutes total
// Retry 19: 6859 minutes - 36100 minutes total
//
// Calculates the exponential backoff time for a given retry count
// The formula is: retryCount^3 minutes
// The last retry will occur 25 days, 1 hour and 40 minutes after the first failed attempt.
// For a total of 20 tries over the time period.
func exponentialBackoff(retryCount int) time.Duration {
	return time.Duration(math.Pow(float64(retryCount), 3)) * time.Minute
}
