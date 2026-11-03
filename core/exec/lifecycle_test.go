//go:build !windows
// +build !windows

package exec

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/alajmo/mani/core"
	"github.com/alajmo/mani/core/dao"
)

// ---- fixtures ----------------------------------------------------------------

type fixture struct {
	root  string
	dirs  []string
	execE *Exec
}

func newFixture(t *testing.T, n int, spec dao.Spec, cmds []string) *fixture {
	t.Helper()

	root := t.TempDir()

	var projects []dao.Project
	var tasks []dao.Task
	var clients []Client
	var dirs []string

	for i := 0; i < n; i++ {
		dir := filepath.Join(root, fmt.Sprintf("p%d", i))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		dirs = append(dirs, dir)

		name := fmt.Sprintf("p%d", i)
		projects = append(projects, dao.Project{Name: name, Path: dir})
		clients = append(clients, Client{Name: name, Path: dir})

		cmd := cmds[i%len(cmds)]
		tasks = append(tasks, dao.Task{
			Name:         name,
			Cmd:          cmd,
			Shell:        "sh -c",
			ShellProgram: "/bin/sh",
			CmdArg:       []string{"-c", cmd},
			SpecData:     spec,
		})
	}

	return &fixture{
		root:  root,
		dirs:  dirs,
		execE: &Exec{Projects: projects, Tasks: tasks, Clients: clients, Config: dao.Config{Path: root}},
	}
}

func (f *fixture) runStream(t *testing.T, b *Batch) error {
	t.Helper()
	b.watch()
	// stream/table surface the same failure finish() canonicalizes; the
	// assertion on the finish result is authoritative.
	_ = f.execE.stream(b, false, io.Discard, io.Discard)
	return b.finish()
}

// livePgids snapshots the process group ids (== leader pids via Setpgid)
// currently tracked by the batch.
func livePgids(b *Batch) []int {
	var pgids []int
	for _, c := range b.snapshotLive() {
		if c.cmd != nil && c.cmd.Process != nil {
			pgids = append(pgids, c.cmd.Process.Pid)
		}
	}
	return pgids
}

// processGroupsGone fails if any process in one of pgids still exists after
// the deadline. Orphaned descendants keep their original pgid, so this also
// catches reparented grandchildren of the reaped tree.
func assertGroupsGone(t *testing.T, pgids []int, deadline time.Duration) {
	t.Helper()

	deadlineAt := time.Now().Add(deadline)
	for time.Now().Before(deadlineAt) {
		out, err := exec.Command("ps", "-e", "-o", "pgid=").Output()
		if err != nil {
			t.Fatalf("ps: %v", err)
		}

		alive := map[int]bool{}
		for _, field := range strings.Fields(string(out)) {
			if pgid, err := strconv.Atoi(field); err == nil {
				alive[pgid] = true
			}
		}

		found := false
		for _, pgid := range pgids {
			if alive[pgid] {
				found = true
				break
			}
		}
		if !found {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("process groups still alive after %s: %v", deadline, pgids)
}

func countNamed(t *testing.T, dirs []string, name string) int {
	t.Helper()
	count := 0
	for _, dir := range dirs {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			count++
		}
	}
	return count
}

func countMatching(t *testing.T, dirs []string, prefix string) int {
	t.Helper()
	count := 0
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), prefix) {
				count++
			}
		}
	}
	return count
}

// interruptAfter snapshots live groups after delay and then cancels the
// batch. The snapshot is delivered through a channel so callers receive it
// with a proper happens-before edge.
func interruptAfter(b *Batch, delay time.Duration) <-chan []int {
	ch := make(chan []int, 1)
	go func() {
		time.Sleep(delay)
		ch <- livePgids(b)
		b.Interrupt()
	}()
	return ch
}

func snapshotAfter(b *Batch, delay time.Duration) <-chan []int {
	ch := make(chan []int, 1)
	go func() {
		time.Sleep(delay)
		ch <- livePgids(b)
	}()
	return ch
}

// ---- tests -------------------------------------------------------------------

func TestStreamCompletesNormally(t *testing.T) {
	f := newFixture(t, 3, dao.Spec{Forks: 3}, []string{"touch done"})
	b := NewBatch(context.Background(), 0, time.Second)

	if err := f.runStream(t, b); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if got := countNamed(t, f.dirs, "done"); got != 3 {
		t.Fatalf("expected 3 done markers, got %d", got)
	}
}

func TestInterruptStopsParallelDispatch(t *testing.T) {
	const forks = 3
	f := newFixture(t, 8, dao.Spec{Forks: forks, Parallel: true}, []string{"touch started; sleep 60"})
	b := NewBatch(context.Background(), 0, time.Second)

	pgidsCh := interruptAfter(b, 400*time.Millisecond)

	start := time.Now()
	err := f.runStream(t, b)
	elapsed := time.Since(start)
	pgids := <-pgidsCh

	if !errors.Is(err, ErrInterrupted) {
		t.Fatalf("expected ErrInterrupted, got %v", err)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("cancellation took %s, expected prompt stop", elapsed)
	}
	// Only the 3 projects that held a forks slot may have started; queued
	// projects must never be launched after cancellation.
	if got := countNamed(t, f.dirs, "started"); got != forks {
		t.Fatalf("expected %d started projects, got %d", forks, got)
	}
	if len(pgids) != forks {
		t.Fatalf("expected %d live groups at interrupt, got %d", forks, len(pgids))
	}
	assertGroupsGone(t, pgids, 2*time.Second)
}

func TestInterruptStopsSequentialDispatch(t *testing.T) {
	f := newFixture(t, 5, dao.Spec{Forks: 4}, []string{"touch started; sleep 60"})
	b := NewBatch(context.Background(), 0, time.Second)

	pgidsCh := interruptAfter(b, 400*time.Millisecond)

	err := f.runStream(t, b)
	pgids := <-pgidsCh

	if !errors.Is(err, ErrInterrupted) {
		t.Fatalf("expected ErrInterrupted, got %v", err)
	}
	// Sequential run: only the first project may have started.
	if got := countNamed(t, f.dirs, "started"); got != 1 {
		t.Fatalf("expected 1 started project, got %d", got)
	}
	assertGroupsGone(t, pgids, 2*time.Second)
}

func TestTimeoutReapsBatch(t *testing.T) {
	f := newFixture(t, 2, dao.Spec{Forks: 2, Parallel: true}, []string{"sleep 60"})
	b := NewBatch(context.Background(), 300*time.Millisecond, time.Second)

	pgidsCh := snapshotAfter(b, 150*time.Millisecond)

	start := time.Now()
	err := f.runStream(t, b)
	elapsed := time.Since(start)
	pgids := <-pgidsCh

	var timeoutErr *TimeoutError
	if !errors.As(err, &timeoutErr) {
		t.Fatalf("expected *TimeoutError, got %v", err)
	}
	if timeoutErr.Timeout != 300*time.Millisecond {
		t.Fatalf("expected 300ms timeout, got %s", timeoutErr.Timeout)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("timeout batch took %s", elapsed)
	}
	assertGroupsGone(t, pgids, 2*time.Second)
}

func TestGracePeriodThenForceKill(t *testing.T) {
	// Ignore SIGINT in the shell (inherited by sleep); only SIGKILL after
	// the grace period can reclaim this tree.
	f := newFixture(t, 1, dao.Spec{Forks: 1}, []string{"trap '' INT; sleep 60"})
	grace := 400 * time.Millisecond
	b := NewBatch(context.Background(), 0, grace)

	pgidsCh := interruptAfter(b, 200*time.Millisecond)

	start := time.Now()
	err := f.runStream(t, b)
	elapsed := time.Since(start)
	pgids := <-pgidsCh

	if !errors.Is(err, ErrInterrupted) {
		t.Fatalf("expected ErrInterrupted, got %v", err)
	}
	if elapsed < grace-100*time.Millisecond {
		t.Fatalf("batch returned before grace period elapsed (%s)", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("force kill did not happen after grace (took %s)", elapsed)
	}
	assertGroupsGone(t, pgids, 2*time.Second)
}

func TestLaterCommandsNotStartedAfterCancel(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "p0")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	task := dao.Task{
		Name:         "p0",
		Shell:        "sh -c",
		ShellProgram: "/bin/sh",
		SpecData: dao.Spec{
			Forks:        1,
			IgnoreErrors: true, // must NOT apply to cancellation
		},
		Commands: []dao.Command{
			{Cmd: "sleep 60", Shell: "sh -c", ShellProgram: "/bin/sh", CmdArg: []string{"-c", "sleep 60"}},
			{Cmd: "touch second", Shell: "sh -c", ShellProgram: "/bin/sh", CmdArg: []string{"-c", "touch second"}},
		},
	}

	execE := &Exec{
		Projects: []dao.Project{{Name: "p0", Path: dir}},
		Tasks:    []dao.Task{task},
		Clients:  []Client{{Name: "p0", Path: dir}},
		Config:   dao.Config{Path: root},
	}

	b := NewBatch(context.Background(), 0, 300*time.Millisecond)
	b.watch()
	go func() {
		time.Sleep(300 * time.Millisecond)
		b.Interrupt()
	}()

	err := execE.stream(b, false, io.Discard, io.Discard)
	ferr := b.finish()
	if !errors.Is(ferr, ErrInterrupted) && !errors.Is(err, ErrInterrupted) {
		t.Fatalf("expected ErrInterrupted (stream=%v finish=%v)", err, ferr)
	}
	if _, err := os.Stat(filepath.Join(dir, "second")); !os.IsNotExist(err) {
		t.Fatal("later command ran after cancellation")
	}
}

func TestIgnoreErrorsStillContinuesOnFailure(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "p0")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	task := dao.Task{
		Name:         "p0",
		Shell:        "sh -c",
		ShellProgram: "/bin/sh",
		SpecData:     dao.Spec{Forks: 1, IgnoreErrors: true},
		Commands: []dao.Command{
			{Cmd: "exit 1", Shell: "sh -c", ShellProgram: "/bin/sh", CmdArg: []string{"-c", "exit 1"}},
			{Cmd: "touch ok", Shell: "sh -c", ShellProgram: "/bin/sh", CmdArg: []string{"-c", "touch ok"}},
		},
	}

	execE := &Exec{
		Projects: []dao.Project{{Name: "p0", Path: dir}},
		Tasks:    []dao.Task{task},
		Clients:  []Client{{Name: "p0", Path: dir}},
		Config:   dao.Config{Path: root},
	}

	b := NewBatch(context.Background(), 0, time.Second)
	b.watch()
	if err := execE.stream(b, false, io.Discard, io.Discard); err != nil {
		t.Fatalf("ordinary failure should be ignored, got %v", err)
	}
	if err := b.finish(); err != nil {
		t.Fatalf("batch should complete, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "ok")); err != nil {
		t.Fatalf("ignore_errors should let later commands run: %v", err)
	}
}

func TestForksLimitAndParallelStillEnforced(t *testing.T) {
	const n, forks = 9, 3

	cmds := []string{
		"touch started; mkdir slot.$$; sleep 0.25; rmdir slot.$$",
	}
	f := newFixture(t, n, dao.Spec{Forks: forks, Parallel: true}, cmds)

	var maxSeen int
	var mu sync.Mutex
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				seen := countMatching(t, f.dirs, "slot.")
				mu.Lock()
				if seen > maxSeen {
					maxSeen = seen
				}
				mu.Unlock()
			}
		}
	}()

	b := NewBatch(context.Background(), 0, time.Second)
	if err := f.runStream(t, b); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	close(stop)

	if got := countNamed(t, f.dirs, "started"); got != n {
		t.Fatalf("expected all %d projects to run, got %d", n, got)
	}
	mu.Lock()
	defer mu.Unlock()
	if maxSeen > forks {
		t.Fatalf("observed %d concurrent processes, forks limit is %d", maxSeen, forks)
	}
	if maxSeen < forks {
		t.Fatalf("never reached forks limit, max concurrent was %d", maxSeen)
	}
}

func TestMultipleBatchesLeaveNoLeaks(t *testing.T) {
	baseline := runtime.NumGoroutine()

	var allPgids []int
	for i := 0; i < 3; i++ {
		// Spawn a grandchild in the same process group that ignores SIGINT;
		// only the post-grace group SIGKILL can reclaim this tree.
		f := newFixture(t, 4, dao.Spec{Forks: 4, Parallel: true}, []string{"trap '' INT; sleep 60 & wait"})
		b := NewBatch(context.Background(), 0, 300*time.Millisecond)

		pgidsCh := interruptAfter(b, 200*time.Millisecond)

		if err := f.runStream(t, b); !errors.Is(err, ErrInterrupted) {
			t.Fatalf("batch %d: expected ErrInterrupted, got %v", i, err)
		}
		allPgids = append(allPgids, <-pgidsCh...)
	}

	assertGroupsGone(t, allPgids, 2*time.Second)

	// No lingering goroutines (watchers, output copiers, tickers).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= baseline+2 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("goroutine leak: baseline=%d now=%d", baseline, runtime.NumGoroutine())
}

func TestInterruptTableOutput(t *testing.T) {
	f := newFixture(t, 6, dao.Spec{Forks: 2, Parallel: true}, []string{"sleep 60"})
	b := NewBatch(context.Background(), 0, time.Second)

	pgidsCh := interruptAfter(b, 300*time.Millisecond)

	b.watch()
	_ = f.execE.table(b, &core.RunFlags{Silent: true})
	err := b.finish()
	pgids := <-pgidsCh

	if !errors.Is(err, ErrInterrupted) {
		t.Fatalf("expected ErrInterrupted from table batch, got %v", err)
	}
	assertGroupsGone(t, pgids, 2*time.Second)
}

// ---- CLI entry point (Exec.Run: real signal listener) -------------------------

func newCLIExec(t *testing.T, n int, spec dao.Spec, cmd string) *Exec {
	t.Helper()
	root := t.TempDir()

	var projects []dao.Project
	var tasks []dao.Task
	for i := 0; i < n; i++ {
		dir := filepath.Join(root, fmt.Sprintf("p%d", i))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		name := fmt.Sprintf("p%d", i)
		projects = append(projects, dao.Project{Name: name, Path: dir})
		tasks = append(tasks, dao.Task{
			Name:         name,
			Shell:        "sh -c",
			ShellProgram: "/bin/sh",
			Cmd:          cmd,
			CmdArg:       []string{"-c", cmd},
			SpecData:     spec,
		})
	}

	return &Exec{Projects: projects, Tasks: tasks, Config: dao.Config{Path: root}}
}

func TestCLIRunInterruptReportedAsError(t *testing.T) {
	e := newCLIExec(t, 2, dao.Spec{Forks: 2, Parallel: true}, "sleep 60")

	runFlags := &core.RunFlags{}
	setRunFlags := &core.SetRunFlags{}

	done := make(chan error, 1)
	go func() {
		done <- e.Run(context.Background(), []string{}, runFlags, setRunFlags)
	}()

	select {
	case err := <-done:
		t.Fatalf("batch finished before interrupt (err=%v)", err)
	case <-time.After(500 * time.Millisecond):
	}

	if err := syscall.Kill(syscall.Getpid(), syscall.SIGINT); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, ErrInterrupted) {
			t.Fatalf("interrupt must not be reported as success, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("CLI run did not return after interrupt")
	}
}

func TestCLIRunSecondInterruptSkipsGrace(t *testing.T) {
	// Children ignore SIGINT; without the force path the 3s default grace
	// applies, with it the batch returns almost immediately.
	e := newCLIExec(t, 2, dao.Spec{Forks: 2, Parallel: true}, "trap '' INT; sleep 60")

	runFlags := &core.RunFlags{}
	setRunFlags := &core.SetRunFlags{}

	done := make(chan error, 1)
	go func() {
		done <- e.Run(context.Background(), []string{}, runFlags, setRunFlags)
	}()

	time.Sleep(500 * time.Millisecond)
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGINT); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	select {
	case err := <-done:
		if !errors.Is(err, ErrInterrupted) {
			t.Fatalf("expected ErrInterrupted, got %v", err)
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Fatalf("second interrupt did not skip grace, took %s", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CLI run did not return after force interrupt")
	}
}

func TestCLIRunTimeout(t *testing.T) {
	e := newCLIExec(t, 2, dao.Spec{Forks: 2, Parallel: true}, "sleep 60")

	runFlags := &core.RunFlags{Timeout: 300 * time.Millisecond}
	setRunFlags := &core.SetRunFlags{}

	done := make(chan error, 1)
	go func() {
		done <- e.Run(context.Background(), []string{}, runFlags, setRunFlags)
	}()

	select {
	case err := <-done:
		var timeoutErr *TimeoutError
		if !errors.As(err, &timeoutErr) {
			t.Fatalf("timeout must be reported as error, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("CLI run did not return after timeout")
	}
}
