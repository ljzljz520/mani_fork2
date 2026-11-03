package exec

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// killGracePeriod is the time started processes are given to exit after the
// polite notification signal before their whole process tree is reaped.
const killGracePeriod = 3 * time.Second

// ErrInterrupted is returned when a batch is cancelled by an interrupt
// (CLI signal or explicit cancellation). It must never be swallowed by
// ignore-errors, which only applies to ordinary command failures.
var ErrInterrupted = errors.New("interrupted")

// TimeoutError is returned when a batch exceeds its configured timeout.
type TimeoutError struct {
	Timeout time.Duration
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("timed out after %s", e.Timeout)
}

// Batch owns the cancellation/timeout lifecycle of a single execution
// batch (one `mani run`/`mani exec` invocation or one TUI run).
//
// Lifecycle guarantees:
//   - once the context is done, no new projects and no further commands of
//     already started projects are launched;
//   - already started processes are notified first and their process trees
//     reaped after the grace period;
//   - finish() does not return until every worker and every output-copying
//     goroutine has exited, so no stale output callbacks outlive the batch.
type Batch struct {
	ctx    context.Context
	cancel context.CancelFunc

	timeout time.Duration
	grace   time.Duration

	// live holds the clients whose process is currently started (one
	// command at a time per client). Protected by mu.
	mu   sync.Mutex
	live map[*Client]struct{}

	// workers tracks one goroutine per started project.
	workers sync.WaitGroup

	doneCh chan struct{}
	result error

	// forceCh short-circuits the grace period (second interrupt).
	forceCh        chan struct{}
	forceCloseOnce sync.Once

	watchOnce    sync.Once
	terminateOnce sync.Once
	finishOnce   sync.Once
}

// NewBatch creates a batch derived from parent. A timeout > 0 aborts the
// batch automatically after it. grace is the polite-signal to force-kill
// delay (defaults to killGracePeriod).
func NewBatch(parent context.Context, timeout, grace time.Duration) *Batch {
	if grace <= 0 {
		grace = killGracePeriod
	}

	b := &Batch{
		timeout: timeout,
		grace:   grace,
		live:    map[*Client]struct{}{},
		doneCh:  make(chan struct{}),
		forceCh: make(chan struct{}),
	}

	if timeout > 0 {
		b.ctx, b.cancel = context.WithTimeout(parent, timeout)
	} else {
		b.ctx, b.cancel = context.WithCancel(parent)
	}

	return b
}

// Ctx exposes the batch context. Dispatch loops and command loops gate on it.
func (b *Batch) Ctx() context.Context {
	return b.ctx
}

// watch starts the shutdown watcher. Called once, right before dispatch.
func (b *Batch) watch() {
	b.watchOnce.Do(func() {
		go func() {
			<-b.ctx.Done()
			b.terminate()
		}()
	})
}

// Interrupt cancels the batch with an "interrupted" cause (user signal,
// TUI cancel, or superseded batch). Idempotent.
func (b *Batch) Interrupt() {
	b.cancel()
}

// Force skips the grace period: still-alive process trees are reaped
// immediately (used when the user interrupts a second time).
func (b *Batch) Force() {
	b.forceCloseOnce.Do(func() {
		close(b.forceCh)
	})
}

// Done is closed once the batch has fully settled (all workers and output
// callbacks gone, all surviving processes reaped).
func (b *Batch) Done() <-chan struct{} {
	return b.doneCh
}

// Err returns the batch result after Done has closed: nil on normal
// completion, ErrInterrupted or *TimeoutError otherwise.
func (b *Batch) Err() error {
	return b.result
}

// addWorker reserves a slot in the project worker group. Must be called
// from the dispatch goroutine before the worker starts.
func (b *Batch) addWorker() {
	b.workers.Add(1)
}

func (b *Batch) workerDone() {
	b.workers.Done()
}

// track registers a client whose process just started.
func (b *Batch) track(c *Client) {
	b.mu.Lock()
	b.live[c] = struct{}{}
	b.mu.Unlock()
}

// untrack removes a client whose process has been waited on.
func (b *Batch) untrack(c *Client) {
	b.mu.Lock()
	delete(b.live, c)
	b.mu.Unlock()
}

func (b *Batch) snapshotLive() []*Client {
	b.mu.Lock()
	defer b.mu.Unlock()

	clients := make([]*Client, 0, len(b.live))
	for c := range b.live {
		clients = append(clients, c)
	}
	return clients
}

// failureErr maps a done context to the corresponding batch error. It
// returns nil while the context is still alive.
func (b *Batch) failureErr() error {
	if err := b.ctx.Err(); err != nil {
		return b.mapCtxErr(err)
	}
	return nil
}

func (b *Batch) mapCtxErr(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return &TimeoutError{Timeout: b.timeout}
	}
	return ErrInterrupted
}

// terminate implements the graceful-notification -> grace-period -> reap
// sequence. It is safe to call multiple times and from both the context
// watcher and finish().
func (b *Batch) terminate() {
	b.terminateOnce.Do(func() {
		// 1. Politely notify every process already started.
		for _, c := range b.snapshotLive() {
			c.notify()
		}

		// 2. Wait for workers to drain, the grace period to elapse, or an
		//    explicit force request.
		workersDone := make(chan struct{})
		go func() {
			b.workers.Wait()
			close(workersDone)
		}()

		select {
		case <-workersDone:
			return
		case <-time.After(b.grace):
		case <-b.forceCh:
		}

		// 3. Reap whatever is still alive, whole process trees.
		for _, c := range b.snapshotLive() {
			c.killTree()
		}
		<-workersDone
	})
}

// finish is called by the runner after every dispatched project worker has
// returned. It drives terminate() when the context is done and closes Done.
func (b *Batch) finish() error {
	b.finishOnce.Do(func() {
		if b.ctx.Err() != nil {
			b.terminate()
		}

		b.workers.Wait()
		b.result = b.failureErr()

		// Release context resources when nothing cancelled them.
		b.cancel()
		close(b.doneCh)
	})
	return b.result
}
