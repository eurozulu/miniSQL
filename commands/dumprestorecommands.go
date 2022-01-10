package commands

import (
	"eurozulu/tinydb/tinydb"
	"fmt"
	"io"
)

var dumpHelp = "Dump and restore the whole database with DUMP and RESTORE\n" +
	"\tDUMP <filename to write to>\n" +
	"\tRESTORE <filename to read from\n"

func DumpCommand(cmd string, out io.Writer) error {
	if cmd == "" {
		return fmt.Errorf("must specifiy the file path to write to")
	}
	if err := tinydb.Dump(cmd, Database); err != nil {
		return err
	}
	_, err := fmt.Fprintf(out, "dumped %d tables to %s\n", len(Database.TableNames()), cmd)
	return err
}

func RestoreCommand(cmd string, out io.Writer) error {
	if cmd == "" {
		return fmt.Errorf("must specifiy the file path to restore from")
	}
	if err := tinydb.Restore(cmd, Database); err != nil {
		return err
	}

	tc := len(Database.TableNames())
	var ts string
	if tc != 1 {
		ts = "s"
	}
	_, err := fmt.Fprintf(out, "restored %d table%s from %s\n", tc, ts, cmd)
	return err
}
