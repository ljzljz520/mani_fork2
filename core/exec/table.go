package exec

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/theckman/yacspin"

	"github.com/alajmo/mani/core"
	"github.com/alajmo/mani/core/dao"
)

func (exec *Exec) table(b *Batch, runFlags *core.RunFlags) dao.TableOutput {
	task := exec.Tasks[0]
	clients := exec.Clients
	projects := exec.Projects

	var spinner *yacspin.Spinner
	var spinnerErr error
	go func() {
		if !runFlags.Silent {
			time.Sleep(500 * time.Millisecond)
			spinner, spinnerErr = initSpinner()
		}
	}()

	var data dao.TableOutput
	var dataMutex = sync.RWMutex{}

	/**
	** Headers
	**/
	data.Headers = append(data.Headers, "project")

	// Append Command names if set
	for _, subTask := range task.Commands {
		if subTask.Name != "" {
			data.Headers = append(data.Headers, subTask.Name)
		} else {
			data.Headers = append(data.Headers, "output")
		}
	}

	// Append Command name if set
	if task.Cmd != "" {
		if task.Name != "" {
			data.Headers = append(data.Headers, task.Name)
		} else {
			data.Headers = append(data.Headers, "output")
		}
	}

	// Populate the rows (project name is first cell, then commands and cmd output is set to empty string)
	for i, p := range projects {
		data.Rows = append(data.Rows, dao.Row{Columns: []string{p.Name}})

		for range task.Commands {
			data.Rows[i].Columns = append(data.Rows[i].Columns, "")
		}

		if task.Cmd != "" {
			data.Rows[i].Columns = append(data.Rows[i].Columns, "")
		}
	}

	wg := core.NewSizedWaitGroup(task.SpecData.Forks)
	/**
	** Values
	**/
	for i := range clients {
		// Stop launching new projects once the batch is done.
		if err := wg.AddWithContext(b.Ctx()); err != nil {
			break
		}
		// A slot may free in the same instant the batch is cancelled: never
		// start the process in that race window.
		if b.Ctx().Err() != nil {
			wg.Done()
			break
		}

		b.addWorker()
		if task.SpecData.Parallel {
			go func(i int) {
				defer wg.Done()
				defer b.workerDone()
				_ = exec.TableWork(b, i, runFlags.DryRun, data, &dataMutex)
			}(i)
		} else {
			func(i int) {
				defer wg.Done()
				defer b.workerDone()
				_ = exec.TableWork(b, i, runFlags.DryRun, data, &dataMutex)
			}(i)

			if b.Ctx().Err() != nil {
				break
			}
		}
	}
	wg.Wait()

	if !runFlags.Silent && spinner != nil && spinnerErr == nil {
		_ = spinner.Stop()
	}

	return data
}

func (exec *Exec) TableWork(b *Batch, rIndex int, dryRun bool, data dao.TableOutput, dataMutex *sync.RWMutex) error {
	client := exec.Clients[rIndex]
	task := exec.Tasks[rIndex]
	var wg sync.WaitGroup

	for j, cmd := range task.Commands {
		// Cancellation stops later commands even with ignore_errors set.
		if err := b.Ctx().Err(); err != nil {
			return b.mapCtxErr(err)
		}

		args := TableCmd{
			rIndex: rIndex,
			cIndex: j + 1,
			client: client,
			dryRun: dryRun,
			shell:  cmd.ShellProgram,
			env:    cmd.EnvList,
			cmd:    cmd.Cmd,
			cmdArr: cmd.CmdArg,
		}

		if cmd.TTY {
			return ExecTTY(cmd.Cmd, cmd.EnvList)
		}

		err := RunTableCmd(b, args, data, dataMutex, &wg)
		if err != nil {
			if errors.Is(err, ErrInterrupted) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			if !task.SpecData.IgnoreErrors {
				return err
			}
		}
	}

	if task.Cmd != "" {
		if err := b.Ctx().Err(); err != nil {
			return b.mapCtxErr(err)
		}

		args := TableCmd{
			rIndex: rIndex,
			cIndex: len(task.Commands) + 1,
			client: client,
			dryRun: dryRun,
			shell:  task.ShellProgram,
			env:    task.EnvList,
			cmd:    task.Cmd,
			cmdArr: task.CmdArg,
		}

		if task.TTY {
			return ExecTTY(task.Cmd, task.EnvList)
		}

		err := RunTableCmd(b, args, data, dataMutex, &wg)
		if err != nil {
			if errors.Is(err, ErrInterrupted) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			if !task.SpecData.IgnoreErrors {
				return err
			}
		}
	}

	wg.Wait()

	return nil
}

func RunTableCmd(b *Batch, t TableCmd, data dao.TableOutput, dataMutex *sync.RWMutex, wg *sync.WaitGroup) error {
	combinedEnvs := dao.MergeEnvs(t.client.Env, t.env)

	if t.dryRun {
		if err := b.Ctx().Err(); err != nil {
			return b.mapCtxErr(err)
		}
		data.Rows[t.rIndex].Columns[t.cIndex] = t.cmd
		return nil
	}

	err := t.client.Run(b.Ctx(), t.shell, combinedEnvs, t.cmdArr)
	if err != nil {
		if b.Ctx().Err() != nil {
			return b.mapCtxErr(b.Ctx().Err())
		}
		return err
	}

	b.track(&t.client)

	// Copy over commands STDOUT.
	var stdoutHandler = func(client Client) {
		defer wg.Done()
		dataMutex.Lock()
		out, err := io.ReadAll(client.Stdout())
		data.Rows[t.rIndex].Columns[t.cIndex] = fmt.Sprintf("%s%s", data.Rows[t.rIndex].Columns[t.cIndex], strings.TrimSuffix(string(out), "\n"))
		dataMutex.Unlock()

		if err != nil && err != io.EOF {
			fmt.Fprintf(os.Stderr, "%v", err)
		}
	}
	wg.Add(1)
	go stdoutHandler(t.client)

	// Copy over tasks's STDERR.
	var stderrHandler = func(client Client) {
		defer wg.Done()
		dataMutex.Lock()
		out, err := io.ReadAll(client.Stderr())
		data.Rows[t.rIndex].Columns[t.cIndex] = fmt.Sprintf("%s%s", data.Rows[t.rIndex].Columns[t.cIndex], strings.TrimSuffix(string(out), "\n"))
		dataMutex.Unlock()
		if err != nil && err != io.EOF {
			fmt.Fprintf(os.Stderr, "%v", err)
		}
	}
	wg.Add(1)
	go stderrHandler(t.client)

	wg.Wait()

	waitErr := t.client.Wait()
	b.untrack(&t.client)

	// An interrupt is never an ordinary (ignorable) command failure.
	if b.Ctx().Err() != nil {
		return b.mapCtxErr(b.Ctx().Err())
	}

	if waitErr != nil {
		// Suppress Go's *exec.ExitError ("exit status N") — it's a stdlib
		// stringification of the process exit code that adds no information
		// the command's own stderr (already captured above) doesn't convey,
		// and it breaks --omit-empty-rows for commands that intentionally
		// exit non-zero (e.g. `grep -v` with no matches). Real mani-level
		// errors (I/O, plumbing) still surface in the cell.
		var exitErr *exec.ExitError
		if !errors.As(waitErr, &exitErr) {
			data.Rows[t.rIndex].Columns[t.cIndex] = fmt.Sprintf("%s\n%s", data.Rows[t.rIndex].Columns[t.cIndex], waitErr.Error())
		}
		return waitErr
	}

	return nil
}

func initSpinner() (*yacspin.Spinner, error) {
	spinner, err := dao.TaskSpinner()
	if err != nil {
		return &spinner, err
	}

	err = spinner.Start()
	if err != nil {
		return &spinner, err
	}

	spinner.Message(" Running")

	return &spinner, nil
}
