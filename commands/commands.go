package commands

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"eurozulu/tinydb/db"
	"github.com/eurozulu/commandline"
)

const historyLocation = "$HOME/.tinydb_history"

var Database *db.TinyDB
var Prompt = ">"
var exitError = fmt.Errorf("exiting")

func ReadCommands(ctx context.Context, out io.Writer, done chan bool) {
	defer close(done)
	err := readStdInput(ctx, out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return
	}
	err = readCommandLine(ctx, out)
}

func readStdInput(ctx context.Context, out io.Writer) error {
	for stdInSize() > 0 {
		s := bufio.NewScanner(os.Stdin)
		for s.Scan() {
			args := strings.Split(s.Text(), " ")
			if err := parseCommand(ctx, out, args...); err != nil {
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

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
			fmt.Fprintf(os.Stderr, "%s\n", err)
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
		if len(args) < 2 {
			err = fmt.Errorf("no create parameter")
		} else {
			err = createCommand(strings.Join(args[1:], " "), out)
		}

	case "DROP":
		err = dropCommand(strings.Join(args[1:], " "), out)

	case "RESTORE":
		if len(args) > 1 {
			err = RestoreCommand(strings.TrimSpace(args[1]), out)
		} else {
			err = fmt.Errorf("no RESTORE parameter")
		}

	case "DUMP":
		if len(args) > 1 {
			err = DumpCommand(strings.TrimSpace(args[1]), out)
		} else {
			err = fmt.Errorf("no DUMP parameter")
		}

	case "DESC", "DESCRIBE":
		if len(args) > 1 {
			err = DescribeCommand(strings.TrimSpace(args[1]), out)
		} else {
			err = fmt.Errorf("no DESCRIBE parameter")
		}

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
		fmt.Fprintf(os.Stderr, "%v", err)
		return 0
	}
	return fi.Size()
}

func HelpCommand(cmd string, out io.Writer) error {
	_, _ = fmt.Fprintln(out, queryHelp)
	_, _ = fmt.Fprintln(out, structueHelp)
	_, _ = fmt.Fprintln(out, metadataHelp)
	_, _ = fmt.Fprintln(out, dumpHelp)
	return nil
}
