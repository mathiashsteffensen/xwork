package xwork

import (
	"strings"
	"testing"
)

func TestCleanStackTraceExcludesLibraryFrames(t *testing.T) {
	stack := []byte(`goroutine 1 [running]:
runtime/debug.Stack()
	/usr/local/go/src/runtime/debug/stack.go:26 +0x5e
github.com/mathiashsteffensen/xwork/v2.(*Processor).processJob.func1()
	/Users/mathias/code/xwork/process.go:354 +0x45
panic({0x102, 0x203})
	/usr/local/go/src/runtime/panic.go:783 +0x132
github.com/example/app.runJob()
	/Users/mathias/code/app/jobs.go:42 +0x12
github.com/mathiashsteffensen/xwork/v2.(*Processor).processJob(0x1, 0x2)
	/Users/mathias/code/xwork/process.go:367 +0x3a
github.com/alitto/pond.(*WorkerPool).executeTask()
	/Users/mathias/go/pkg/mod/github.com/alitto/pond/pool.go:123 +0x4
runtime.goexit()
	/usr/local/go/src/runtime/asm_arm64.s:1268 +0x1
`)

	cleaned := cleanStackTrace(stack)

	if !strings.Contains(cleaned, "github.com/example/app.runJob()") {
		t.Fatalf("expected user frame, got:\n%s", cleaned)
	}

	for _, unwanted := range []string{
		"runtime/debug.Stack",
		"github.com/mathiashsteffensen/xwork/v2.(*Processor).",
		"github.com/alitto/pond.",
		"runtime.goexit",
		"panic(",
	} {
		if strings.Contains(cleaned, unwanted) {
			t.Fatalf("expected cleaned stack to exclude %q, got:\n%s", unwanted, cleaned)
		}
	}
}
