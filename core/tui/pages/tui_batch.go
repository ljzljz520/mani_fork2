package pages

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/rivo/tview"

	"github.com/alajmo/mani/core/exec"
	"github.com/alajmo/mani/core/tui/misc"
)

// batchLauncher serializes batch launches on a TUI page. A new launch
// interrupts any in-flight batch and waits for it to fully settle before
// starting, so processes and output callbacks from an old batch can never
// leak into a new one. Generations ensure stale completion callbacks never
// touch the view after they have been superseded.
type batchLauncher struct {
	gen atomic.Uint64
}

// buildFunc parses the current selections and starts a batch. It runs off
// the UI goroutine so the interface stays responsive.
type buildFunc func(ctx context.Context) (*exec.Batch, error)

func (l *batchLauncher) launch(
	streamView *tview.TextView,
	out *misc.ThreadSafeWriter,
	build buildFunc,
) {
	gen := l.gen.Add(1)

	go func() {
		// Let an in-flight batch (possibly started from another page) fully
		// settle before starting the new one.
		if prev := misc.ActiveBatch(); prev != nil {
			prev.Interrupt()
			<-prev.Done()
		}

		// A newer launch happened while we waited: this one is obsolete and
		// must not start anything or write to the view.
		if l.gen.Load() != gen {
			return
		}

		b, err := build(context.Background())
		if err != nil {
			if l.gen.Load() == gen {
				misc.App.QueueUpdateDraw(func() {
					fmt.Fprintf(out, "\x1b[31m%s\x1b[0m\n", err)
					streamView.ScrollToEnd()
				})
			}
			return
		}

		misc.SetActiveBatch(b)

		// TextView.Write is goroutine-safe, but screen redraws have to be
		// driven from the UI goroutine. Stop the ticker exactly once when the
		// batch settles so no timer leaks across batches.
		ticker := time.NewTicker(100 * time.Millisecond)
		go func() {
			for {
				select {
				case <-b.Done():
					ticker.Stop()
					return
				case <-ticker.C:
					misc.App.QueueUpdateDraw(func() {})
				}
			}
		}()

		<-b.Done()
		misc.ClearActiveBatch(b)

		// Never let an obsolete batch update the view.
		if l.gen.Load() != gen {
			return
		}

		misc.App.QueueUpdateDraw(func() {
			if err := b.Err(); err != nil {
				fmt.Fprintf(out, "\x1b[31m%s\x1b[0m\n", err)
			}
			streamView.ScrollToEnd()
		})
	}()
}
