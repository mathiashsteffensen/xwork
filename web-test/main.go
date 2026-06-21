package main

import (
	"database/sql"
	"errors"
	"time"

	"github.com/mathiashsteffensen/xwork/v2"
	"github.com/mathiashsteffensen/xwork/v2/storage"
	_ "github.com/mattn/go-sqlite3"
	"github.com/sirupsen/logrus"
)

var logger = logrus.New()

func main() {
	db, err := sql.Open("sqlite3", "./sqlite.db?_busy_timeout=5000&_journal_mode=WAL&_foreign_keys=on")
	if err != nil {
		logger.Fatal(err)
	}

	storageAdapter := storage.NewSQL(db)
	processor, err := xwork.NewProcessor(storageAdapter)
	if err != nil {
		logger.Fatal(err)
	}

	logger.SetLevel(logrus.InfoLevel)
	processor.SetLogger(logger)
	processor.SetConcurrency(20)
	processor.SetKillTimeout(5 * time.Second)

	processSomeJobs(processor)

	go processor.Serve()
	processor.Process()
}

func processSomeJobs(processor *xwork.Processor) {
	shortRecurringJob := processor.DefineJob("default", "short_job", func(job *xwork.ProcessingJob) error {
		time.Sleep(30 * time.Second)

		return processor.EnqueueIn("short_job", 2*time.Minute, xwork.JobPayload{})
	})
	err := shortRecurringJob.EnqueueIn(5*time.Second, xwork.JobPayload{})
	if err != nil {
		logger.Fatal(err)
	}

	longJob := processor.DefineJob("default", "long_job", func(job *xwork.Job) error {
		time.Sleep(5 * time.Minute)
		return nil
	})
	err = longJob.Enqueue(xwork.JobPayload{})
	if err != nil {
		logger.Fatal(err)
	}

	failingJob := processor.DefineJob("default", "failing_job", func(job *xwork.Job) error {
		time.Sleep(5 * time.Second)

		return errors.New("something bad happened")
	})
	err = failingJob.Enqueue(xwork.JobPayload{})
	if err != nil {
		logger.Fatal(err)
	}

	otherQueueLongJob := processor.DefineJob("other", "other_long_job", func(job *xwork.Job) error {
		time.Sleep(5 * time.Minute)
		return nil
	})
	err = otherQueueLongJob.Enqueue(xwork.JobPayload{})
	if err != nil {
		logger.Fatal(err)
	}
}
