package xwork

import (
	"fmt"
	"math"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/alitto/pond"
	"github.com/sirupsen/logrus"
)

var (
	logger logrus.FieldLogger = logrus.New().WithField("package", "xwork")

	pool *pond.WorkerPool

	// These are the jobs currently in memory in this process
	// They are also stored in whatever StorageAdapter is being used,
	// but we keep them in memory and try to push them back if they don't finish when the process is shutting down
	processingJobs = NewAtomicMap[*Job]()

	quit                       = make(chan struct{})
	numManagedGoRoutines int32 = 0
)

func SetLogger(fieldLogger logrus.FieldLogger) {
	logger = fieldLogger
}

func Process(concurrency int, queues ...string) {
	logger.
		WithField("concurrency", concurrency).
		WithField("queues", strings.Join(queues, ", ")).
		Info("Starting background work processor")

	pool = pond.New(concurrency, 0)

	go enqueueScheduledJobs(quit)
	go requeueFailedJobs(quit)

	atomic.AddInt32(&numManagedGoRoutines, 2)

	for _, queue := range queues {
		go processQueue(pool, queue, quit)
		atomic.AddInt32(&numManagedGoRoutines, 1)
	}

	go WaitForShutdown()
}

func WaitForShutdown() {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGHUP, os.Interrupt, os.Kill)
	<-sigc
	Shutdown()
}

func Shutdown() {
	for i := 0; int32(i) < numManagedGoRoutines; i++ {
		// Gracefully shutdown all managed goroutines
		quit <- struct{}{}
		atomic.AddInt32(&numManagedGoRoutines, -1)
	}

	pool.StopAndWaitFor(30 * time.Second)

	// Try to reschedule jobs that didn't finish processing
	processingJobs.Each(func(_ string, job *Job) {
		logger.Warnf("Attempting to requeue interupted job %s", job.ID)

		err := requeue(job)
		if err != nil {
			logger.WithError(err).WithField("id", job.ID).WithField("payload", job.Payload).Error("failed to requeue job")
		}
	})

	time.Sleep(250 * time.Millisecond)
}

func enqueueScheduledJobs(quit chan struct{}) {
	logger.Debugf("Enqueueing scheduled jobs")

	ticker := time.NewTicker(10 * time.Millisecond)
	go func() {
		for {
			select {
			case <-ticker.C:
				err := enqueueReadyScheduledJobs()
				if err != nil {
					fmt.Printf("failed to enqueue scheduled jobs: %v\n", err)
					continue
				}
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()

}

func requeueFailedJobs(quit chan struct{}) {
	logger.Debugf("Requeueing failed jobs")

	ticker := time.NewTicker(10 * time.Millisecond)
	go func() {
		for {
			select {
			case <-ticker.C:
				err := enqueueReadyFailedJobs()
				if err != nil {
					fmt.Printf("failed to requeue failed jobs: %v\n", err)
					continue
				}
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
}

func processQueue(pool *pond.WorkerPool, queue string, quit chan struct{}) {
	logger.Infof("Processing queue %q", queue)

	ticker := time.NewTicker(10 * time.Millisecond)
	go func() {
		for {
			select {
			case <-ticker.C:
				pool.Submit(func() {
					job, err := dequeue(queue)
					if err != nil {
						logger.WithError(err).Error("failed to dequeue job")
						return
					}

					if job == nil {
						return
					}

					processJob(job)
				})
			case <-quit:
				logger.Infof("Stopping queue %q", queue)
				ticker.Stop()
				return
			}
		}
	}()
}

func processJob(job *Job) error {
	processingJobs.Set(job.ID.String(), job)
	defer processingJobs.Delete(job.ID.String())

	jobDefinition := jobDefinitions.get(job.Queue, job.Name)

	l := logger.WithFields(map[string]any{
		"id":    job.ID,
		"queue": job.Queue,
		"name":  job.Name,
	})

	l.Info("START")

	start := time.Now()

	defer func() {
		if r := recover(); r != nil {
			l.WithField("panic", r).Error("PANIC")
			err := fail(job, fmt.Errorf("panic: %v", r))
			if err != nil {
				l.WithError(err).Error("failed to send job to failed queue")
			}
		}
	}()

	err := jobDefinition.Handler(job)

	l = l.WithField("duration", time.Since(start).String())

	if err != nil {
		l.WithError(err).Error("FAILED")

		err = fail(job, err)
		if err != nil {
			l.WithError(err).Error("failed to send job to failed queue")
		}

		return err
	}

	l.Info("COMPLETED")

	err = complete(job)
	if err != nil {
		l.WithError(err).Error("failed to complete job")
		return err
	}

	return nil
}

func enqueue(job *ScheduledJob) error {
	if job.EnqueueAt.Before(time.Now()) {
		return storage.InsertToQueue(&EnqueuedJob{
			ID:          job.ID,
			Name:        job.Name,
			Queue:       job.Queue,
			Payload:     job.Payload,
			EnqueuedAt:  time.Now(),
			ScheduledAt: job.ScheduledAt,
		})
	} else {
		return storage.InsertToScheduled(job)
	}
}

func dequeue(queue string) (*ProcessingJob, error) {
	var job *ProcessingJob
	err := storage.Transact(func(storage StorageAdapter) error {
		enqueuedJob, err := storage.GetFromQueue(queue)
		if err != nil {
			logger.Debugf("failed to get job from queue %q: %v", queue, err)
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

func complete(job *Job) error {
	return storage.Transact(func(storage StorageAdapter) error {
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

func requeue(job *Job) error {
	return storage.Transact(func(storage StorageAdapter) error {
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

func fail(job *Job, jobErr error) error {
	return storage.Transact(func(storage StorageAdapter) error {
		err := storage.DeleteFromProcessing(job.ID)
		if err != nil {
			return err
		}

		retryCount := job.RetryCount + 1

		if retryCount > DEFAULT_MAX_RETRY_COUNT {
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

		return storage.InsertToFailed(&FailedJob{
			ID:          job.ID,
			Name:        job.Name,
			Queue:       job.Queue,
			Payload:     job.Payload,
			Error:       jobErr.Error(),
			LastRetryAt: now,
			NextRetryAt: now.Add(exponentialBackoff(retryCount)),
			RetryCount:  retryCount,
			ScheduledAt: job.ScheduledAt,
		})
	})
}

func retry(job *FailedJob) error {
	return storage.Transact(func(storage StorageAdapter) error {
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

func enqueueReadyFailedJobs() error {
	jobs, err := storage.NextFromFailed()
	if err != nil {
		return err
	}

	for _, job := range jobs {
		err = retry(job)
		if err != nil {
			return err
		}
	}

	return nil
}

func enqueueScheduled(job *ScheduledJob) error {
	return storage.Transact(func(storage StorageAdapter) error {
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

func enqueueReadyScheduledJobs() error {
	jobs, err := storage.NextFromScheduled()
	if err != nil {
		return err
	}

	for _, job := range jobs {
		if err := enqueueScheduled(job); err != nil {
			return err
		}
	}

	return nil
}

const DEFAULT_MAX_RETRY_COUNT = 19

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
