package exec

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"

	"github.com/gookit/color"

	"github.com/alajmo/mani/core"
	"github.com/alajmo/mani/core/dao"
	"github.com/alajmo/mani/core/print"
)

type Exec struct {
	Clients  []Client
	Projects []dao.Project
	Tasks    []dao.Task
	Config   dao.Config
}

type TableCmd struct {
	rIndex int
	cIndex int
	client Client
	dryRun bool

	desc     string
	name     string
	shell    string
	env      []string
	cmd      string
	cmdArr   []string
	numTasks int
}

// Run executes a CLI batch. Interrupt signals (SIGINT/SIGTERM) and the
// optional timeout share one lifecycle: they stop dispatching new projects,
// notify already started processes and reap their trees after the grace
// period. Interruption is always returned as an error so it is never
// reported as success. The signal listener is removed before returning.
func (exec *Exec) Run(
	ctx context.Context,
	userArgs []string,
	runFlags *core.RunFlags,
	setRunFlags *core.SetRunFlags,
) error {
	err := exec.ParseTask(userArgs, runFlags, setRunFlags)
	if err != nil {
		return err
	}

	clientCh := make(chan Client, len(exec.Projects))
	errCh := make(chan error, len(exec.Projects))
	err = exec.SetClients(clientCh, errCh)
	if err != nil {
		return err
	}

	// Describe task
	if runFlags.Describe {
		out := print.PrintTaskBlock([]dao.Task{exec.Tasks[0]}, false, exec.Tasks[0].ThemeData.Block, print.GookitFormatter{})
		fmt.Print(out)
	}

	exec.CheckTaskNoColor()

	batch := NewBatch(ctx, runFlags.Timeout, 0)
	batch.watch()

	// CLI-only signal handling. The TUI owns the terminal (raw mode) and
	// cancels batches via key events, so it must never install a listener
	// that would kill the whole application.
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, interruptSignals()...)
	listenerDone := make(chan struct{})
	go func() {
		select {
		case <-sigCh:
			// First interrupt: polite cancellation.
			batch.Interrupt()
		case <-batch.Done():
		}

		select {
		case <-sigCh:
			// Second interrupt: skip the grace period.
			batch.Force()
		case <-batch.Done():
		}

		// Detach the listener so no signal handler outlives the batch.
		signal.Stop(sigCh)
		close(listenerDone)
	}()

	runErr := exec.executeBatch(batch, runFlags, os.Stdout, os.Stderr, true)
	batchErr := batch.finish()

	<-listenerDone

	if batchErr != nil {
		return batchErr
	}
	return runErr
}

// RunTUI prepares and starts a batch asynchronously, returning its handle
// so the TUI stays responsive and can cancel it. All output-copying
// goroutines have exited by the time the returned batch's Done channel
// closes, preventing stale writes into later batches.
func (exec *Exec) RunTUI(
	ctx context.Context,
	userArgs []string,
	runFlags *core.RunFlags,
	setRunFlags *core.SetRunFlags,
	output string,
	outWriter io.Writer,
	errWriter io.Writer,
) (*Batch, error) {
	err := exec.ParseTask(userArgs, runFlags, setRunFlags)
	if err != nil {
		return nil, err
	}

	clientCh := make(chan Client, len(exec.Projects))
	errCh := make(chan error, len(exec.Projects))
	err = exec.SetClients(clientCh, errCh)
	if err != nil {
		return nil, err
	}

	batch := NewBatch(ctx, runFlags.Timeout, 0)
	batch.watch()

	go func() {
		_ = exec.executeBatch(batch, runFlags, outWriter, errWriter, false)
		_ = batch.finish()
	}()

	return batch, nil
}

func (exec *Exec) executeBatch(
	batch *Batch,
	runFlags *core.RunFlags,
	stdout io.Writer,
	stderr io.Writer,
	autoWrap bool,
) error {
	switch exec.Tasks[0].SpecData.Output {
	case "table", "html", "markdown":
		data := exec.table(batch, runFlags)
		if err := batch.failureErr(); err != nil {
			return err
		}

		options := print.PrintTableOptions{
			Theme:            exec.Tasks[0].ThemeData,
			Output:           exec.Tasks[0].SpecData.Output,
			Color:            *exec.Tasks[0].ThemeData.Color,
			AutoWrap:         autoWrap,
			OmitEmptyRows:    exec.Tasks[0].SpecData.OmitEmptyRows,
			OmitEmptyColumns: exec.Tasks[0].SpecData.OmitEmptyColumns,
		}
		print.PrintTable(data.Rows, options, data.Headers[0:1], data.Headers[1:], stdout)
		fmt.Fprintln(stdout)
	default:
		return exec.stream(batch, runFlags.DryRun, stdout, stderr)
	}

	return nil
}

func (exec *Exec) SetClients(
	clientCh chan Client,
	errCh chan error,
) error {
	config := exec.Config
	ignoreNonExisting := exec.Tasks[0].SpecData.IgnoreNonExisting
	projects := exec.Projects

	var clients []Client
	for i, project := range projects {
		func(i int, project dao.Project) {
			projectPath, err := core.GetAbsolutePath(config.Path, project.Path, project.Name)
			if err != nil {
				errCh <- &core.FailedToParsePath{Name: projectPath}
				return
			}
			if _, err := os.Stat(projectPath); os.IsNotExist(err) && !ignoreNonExisting {
				errCh <- &core.PathDoesNotExist{Path: projectPath}
				return
			}

			client := Client{Path: projectPath, Name: project.Name, Env: project.EnvList}
			clientCh <- client

			clients = append(clients, client)
		}(i, project)
	}

	close(clientCh)
	close(errCh)

	// Return if there's any errors
	for err := range errCh {
		return err
	}

	exec.Clients = clients

	return nil
}

// ParseTask processes and updates task configurations based on runtime flags and user arguments.
// It handles theme, specification, environment variables, and execution settings for each task.
//
// The function performs these operations for each task:
// 1. Evaluates configuration environment variables
// 2. Updates theme if specified
// 3. Updates spec settings if provided
// 4. Applies runtime execution flags
// 5. Processes environment variables for the task and its commands
//
// Environment variable processing order:
// 1. Configuration level variables
// 2. Task level variables
// 3. Command level variables
// 4. User provided arguments
func (exec *Exec) ParseTask(userArgs []string, runFlags *core.RunFlags, setRunFlags *core.SetRunFlags) error {
	configEnv, err := dao.EvaluateEnv(exec.Config.EnvList)
	if err != nil {
		return err
	}

	for i := range exec.Tasks {
		// Update theme property if user flag is provided
		if runFlags.Theme != "" {
			theme, err := exec.Config.GetTheme(runFlags.Theme)
			if err != nil {
				return err
			}

			exec.Tasks[i].ThemeData = *theme
		}

		if runFlags.Spec != "" {
			spec, err := exec.Config.GetSpec(runFlags.Spec)
			if err != nil {
				return err
			}
			exec.Tasks[i].SpecData = *spec
		}

		// Update output property if user flag is provided
		if runFlags.Output != "" {
			exec.Tasks[i].SpecData.Output = runFlags.Output
		}

		// TTY
		if setRunFlags.TTY {
			exec.Tasks[i].TTY = runFlags.TTY
		}

		// Omit rows which provide empty output
		if setRunFlags.OmitEmptyRows {
			exec.Tasks[i].SpecData.OmitEmptyRows = runFlags.OmitEmptyRows
		}

		// Omit columns which provide empty output
		if setRunFlags.OmitEmptyColumns {
			exec.Tasks[i].SpecData.OmitEmptyColumns = runFlags.OmitEmptyColumns
		}

		if setRunFlags.IgnoreErrors {
			exec.Tasks[i].SpecData.IgnoreErrors = runFlags.IgnoreErrors
		}

		if setRunFlags.IgnoreNonExisting {
			exec.Tasks[i].SpecData.IgnoreNonExisting = runFlags.IgnoreNonExisting
		}

		// If parallel flag is set to true, then update task specs
		if setRunFlags.Parallel {
			exec.Tasks[i].SpecData.Parallel = runFlags.Parallel
		}

		if setRunFlags.Forks {
			exec.Tasks[i].SpecData.Forks = runFlags.Forks
		}

		// Parse env here instead of config since we're only interested in tasks run, and not all tasks.
		// Also, userArgs is not present in the config.
		envs, err := dao.ParseTaskEnv(exec.Tasks[i].Env, userArgs, []string{}, configEnv)
		if err != nil {
			return err
		}
		exec.Tasks[i].EnvList = envs

		// Set environment variables for sub-commands
		for j := range exec.Tasks[i].Commands {
			envs, err := dao.ParseTaskEnv(exec.Tasks[i].Commands[j].Env, userArgs, exec.Tasks[i].EnvList, configEnv)
			if err != nil {
				return err
			}
			exec.Tasks[i].Commands[j].EnvList = envs
		}
	}

	return nil
}

func (exec *Exec) CheckTaskNoColor() {
	task := exec.Tasks[0]

	for _, env := range task.EnvList {
		name := strings.Split(env, "=")[0]
		if name == "NO_COLOR" {
			color.Disable()
			break
		}
	}
}
