package misc

import (
	"sync"

	"github.com/alajmo/mani/core/exec"
)

// Active batch registry shared by the run and exec pages. At most one batch
// runs at a time; a new launch first lets any previous batch fully settle so
// processes, signal listeners and output callbacks never leak across batches.
var (
	batchMu     sync.Mutex
	activeBatch *exec.Batch
)

// SetActiveBatch registers the currently running batch.
func SetActiveBatch(b *exec.Batch) {
	batchMu.Lock()
	activeBatch = b
	batchMu.Unlock()
}

// ActiveBatch returns the currently running batch, if any.
func ActiveBatch() *exec.Batch {
	batchMu.Lock()
	defer batchMu.Unlock()
	return activeBatch
}

// ClearActiveBatch unregisters b only if it is still the active batch.
func ClearActiveBatch(b *exec.Batch) {
	batchMu.Lock()
	defer batchMu.Unlock()
	if activeBatch == b {
		activeBatch = nil
	}
}

// BatchRunning reports whether a batch is currently in flight.
func BatchRunning() bool {
	batchMu.Lock()
	defer batchMu.Unlock()
	return activeBatch != nil
}

// CancelActiveBatch interrupts the running batch, if any. Returns true when
// a cancellation was requested so key handlers can swallow the event.
func CancelActiveBatch() bool {
	batchMu.Lock()
	b := activeBatch
	batchMu.Unlock()

	if b != nil {
		b.Interrupt()
		return true
	}
	return false
}
