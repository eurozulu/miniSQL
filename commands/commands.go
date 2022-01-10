package commands

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"eurozulu/tinydb/tinydb"
	"github.com/eurozulu/commandline"
)

const historyLocation = "$HOME/.tinydb_history"

var Database *tinydb.TinyDB
var Prompt = ">"
var exitError = fmt.Errorf("exiting")

// RunCommands reads the available commands from the std input and executes them.
// If stdinput already contains data (pipped in from cmdline) that is executed first, then
// the applications own command line is started.  Commands can then be entered into this command line until "EXIT" is entered
func RunCommands(ctx context.Context, out io.Writer, done chan bool) {
	defer close(done)
	err := readStdInput(ctx, out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return
	}
	err = readCommandLine(ctx, out)
}

// readStdInput flushes any available data in stdin and parse it as lines of commands
func readStdInput(ctx context.Context, out io.Writer) error {
	for stdInSize() > 0 {
		s := bufio.NewScanner(os.Stdin)
		for s.Scan() {
			if err := parseCommand(ctx, out, strings.Split(s.Text(), " ")...); err != nil {
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// readCommandLine awaits user input and parses each line as a command
func readCommandLine(ctx context.Context, out io.Writer) error {
	cli := commandline.NewCommandLine()
	if err := cli.LoadHistory(historyLocation); err != nil {
		return fmt.Errorf("cli history load failed  %w", err)
	}
	defer func(cli *commandline.CommandLine) {
		if err := cli.SaveHistory(historyLocation); err != nil {
			log.Println(err)
		}
	}(cli)

	for {
		_, _ = out.Write([]byte(Prompt))
		ln, err := cli.ReadCommand()
		if err != nil {
			return fmt.Errorf("failed to read command line %w", err)
		}
		fmt.Println()
		err = parseCommand(ctx, out, strings.Split(ln, " ")...)
		if err != nil {
			if err == exitError {
				return err
			}
			if _, err = fmt.Fprintf(os.Stderr, "%s\n", err); err != nil {
				return fmt.Errorf("Failed to write to stdout  %w", err)
			}
		}
	}
}

func parseCommand(ctx context.Context, out io.Writer, args ...string) error {
	if len(args) == 0 {
		args = []string{""}
	}
	var err error
	switch strings.ToUpper(args[0]) {
	case "":
		err = nil //nop
	case "EXIT", "X", "QUIT":
		return exitError
	case "SELECT", "INSERT", "DELETE", "UPDATE":
		err = queryCommand(ctx, strings.Join(args, " "), out)
	case "CREATE":
		err = createCommand(strings.Join(args[1:], " "), out)

	case "DROP":
		err = dropCommand(strings.Join(args[1:], " "), out)

	case "RESTORE":
		err = RestoreCommand(strings.Join(args[1:], ""), out)

	case "DUMP":
		err = DumpCommand(strings.Join(args[1:], ""), out)

	case "DESC", "DESCRIBE":
		err = DescribeCommand(strings.Join(args[1:], ""), out)

	case "TABLES":
		err = TablesCommand("", out)

	case "HELP":
		err = HelpCommand(strings.Join(args, " "), out)
	default:
		err = fmt.Errorf("%q is an unknown command", args[0])
	}
	return err
}

func stdInSize() int64 {
	fi, err := os.Stdin.Stat()
	if err != nil {
		if _, err := fmt.Fprintf(os.Stderr, "%v", err); err != nil {
			log.Fatalln(err)
		}
		return 0
	}
	return fi.Size()
}

func HelpCommand(_ string, out io.Writer) error {
	_, _ = fmt.Fprintln(out, queryHelp)
	_, _ = fmt.Fprintln(out, structueHelp)
	_, _ = fmt.Fprintln(out, metadataHelp)
	_, _ = fmt.Fprintln(out, dumpHelp)
	return nil
}
