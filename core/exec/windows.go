//go:build windows
// +build windows

package exec

import (
	"os"
	"os/exec"
	"strconv"
)

func ExecTTY(cmd string, envs []string) error {
	return nil
}

// configureProcess is a no-op on Windows: tree termination walks the
// process tree via taskkill /T instead of process groups.
func configureProcess(cmd *exec.Cmd) {}

// processPid returns the started process pid.
func (c *Client) processPid() (int, bool) {
	if c.cmd == nil || c.cmd.Process == nil || c.cmd.Process.Pid <= 0 {
		return 0, false
	}
	return c.cmd.Process.Pid, true
}

// notify asks the process to stop. Console processes on Windows cannot be
// interrupted with a signal, so this is best-effort; the grace-period kill
// with taskkill /T is the reliable path.
func (c *Client) notify() {
	pid, ok := c.processPid()
	if !ok {
		return
	}

	_ = exec.Command("taskkill", "/PID", strconv.Itoa(pid)).Run()
}

// killTree force-reaps the process and every descendant.
func (c *Client) killTree() {
	pid, ok := c.processPid()
	if !ok {
		return
	}

	_ = exec.Command("taskkill", "/F", "/T", "/PID", strconv.Itoa(pid)).Run()
}

// interruptSignals are the OS signals that cancel a CLI batch.
func interruptSignals() []os.Signal {
	return []os.Signal{os.Interrupt}
}
