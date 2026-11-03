package exec

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
)

// Client is a wrapper over the SSH connection/sessions.
type Client struct {
	Name string
	Path string
	Env  []string

	cmd     *exec.Cmd
	stdout  io.Reader
	stderr  io.Reader
	running bool
}

func (c *Client) Run(ctx context.Context, shell string, env []string, cmdStr []string) error {
	var err error
	if c.running {
		return fmt.Errorf("command already running")
	}

	// Do not start a new process once the batch is done.
	if err := ctx.Err(); err != nil {
		return err
	}

	cmd := exec.Command(shell, cmdStr...)

	cmd.Dir = c.Path
	cmd.Env = append(os.Environ(), env...)

	// Put the child in its own process group/tree so notifications and
	// forced reaping reach the shell *and* all of its descendants.
	configureProcess(cmd)

	c.cmd = cmd

	c.stdout, err = cmd.StdoutPipe()
	if err != nil {
		return err
	}

	c.stderr, err = cmd.StderrPipe()
	if err != nil {
		return err
	}

	if err := c.cmd.Start(); err != nil {
		return err
	}

	c.running = true

	return nil
}

func (c *Client) Wait() error {
	if !c.running {
		return fmt.Errorf("trying to wait on stopped command")
	}

	err := c.cmd.Wait()
	c.running = false

	return err
}

func (c *Client) Close() error {
	return nil
}

func (c *Client) Stderr() io.Reader {
	return c.stderr
}

func (c *Client) Stdout() io.Reader {
	return c.stdout
}

func (c *Client) Prefix() string {
	return c.Name
}
