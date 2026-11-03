//go:build !windows
// +build !windows

package exec

import (
	"os"
	"os/exec"
	"syscall"

	"golang.org/x/sys/unix"
)

func ExecTTY(cmd string, envs []string) error {
	shell := "bash"
	foundShell, found := os.LookupEnv("SHELL")
	if found {
		shell = foundShell
	}

	execBin, err := exec.LookPath(shell)
	if err != nil {
		return err
	}

	userEnv := append(os.Environ(), envs...)
	err = unix.Exec(execBin, []string{shell, "-c", cmd}, userEnv)
	if err != nil {
		return err
	}

	return nil
}

// configureProcess starts the child as the leader of a fresh process group
// so it can be signalled together with every descendant it spawns.
func configureProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// processPid returns the started process pid. cmd and Process are immutable
// after Start, so this is safe to read concurrently with Wait; membership in
// the batch's live set decides whether the result is acted upon.
func (c *Client) processPid() (int, bool) {
	if c.cmd == nil || c.cmd.Process == nil || c.cmd.Process.Pid <= 1 {
		return 0, false
	}
	return c.cmd.Process.Pid, true
}

// notify politely asks the whole process group to stop. The child became a
// group leader through Setpgid, so its group id equals its pid and signalling
// -pid reaches the shell and all of its children.
func (c *Client) notify() {
	pid, ok := c.processPid()
	if !ok {
		return
	}

	_ = syscall.Kill(-pid, syscall.SIGINT)
}

// killTree force-reaps the whole process group.
func (c *Client) killTree() {
	pid, ok := c.processPid()
	if !ok {
		return
	}

	_ = syscall.Kill(-pid, syscall.SIGKILL)
}

// interruptSignals are the OS signals that cancel a CLI batch.
func interruptSignals() []os.Signal {
	return []os.Signal{syscall.SIGINT, syscall.SIGTERM}
}
