package commands

import (
	"eurozulu/miniSQL/minisql"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
)

var dumpHelp = "Dump and restore the whole database with DUMP and RESTORE\n" +
	"\tDUMP <filename to write to>\n" +
	"\tRESTORE <filename to read from\n"

func DumpCommand(cmd string, out io.Writer) error {
	if cmd == "" {
		return fmt.Errorf("must specifiy the file path to write to")
	}
	if path.Ext(cmd) == "" {
		cmd = strings.Join([]string{cmd, "json"}, ".")
	}
	if err := minisql.Dump(cmd, Database); err != nil {
		return err
	}
	Prompt = dbName(cmd) + ">"
	_, err := fmt.Fprintf(out, "dumped %d tables to %s\n", len(Database.TableNames()), cmd)
	return err
}

func RestoreCommand(cmd string, out io.Writer) error {
	if cmd == "" {
		return fmt.Errorf("must specifiy the file path to restore from")
	}

	tc := len(Database.TableNames())
	if err := minisql.Restore(cmd, Database); err != nil {
		if path.Ext(cmd) == "" && os.IsNotExist(err) {
			// not exists without extentions, try again with json extension
			err = minisql.Restore(strings.Join([]string{cmd, "json"}, "."), Database)
		}
		if err != nil {
			return err
		}
	}
	tc = len(Database.TableNames()) - tc
	var ts string
	if tc != 1 {
		ts = "s"
	}
	Prompt = dbName(cmd) + ">"
	_, err := fmt.Fprintf(out, "restored %d new table%s from %s\n", tc, ts, cmd)
	return err
}

func dbName(s string) string {
	n := path.Base(s)
	return n[:len(n)-len(path.Ext(s))]
}
