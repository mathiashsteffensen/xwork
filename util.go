package xwork

import (
	"os"
	"os/signal"
	"syscall"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return fallback
}

func newShutdownChannel() chan os.Signal {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGHUP, os.Interrupt)
	return sigc
}
