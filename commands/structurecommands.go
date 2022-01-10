package commands

import (
	"eurozulu/tinydb/db"
	"eurozulu/tinydb/queryparse"
	"fmt"
	"io"
	"strings"
)

func createCommand(cmd string, out io.Writer) error {
	cmds := strings.SplitN(cmd, " ", 2)
	switch strings.ToUpper(cmds[0]) {
	case "TABLE":
		return createTable(cmds[1])
	case "COLUMN", "COL":
		return createColumn(cmds[1])
	default:
		return fmt.Errorf("unknown CREATE type, must be TABLE or COLUMN")
	}
}
func dropCommand(cmd string, out io.Writer) error {
	cmds := strings.SplitN(cmd, " ", 2)
	switch strings.ToUpper(cmds[0]) {
	case "TABLE":
		return dropTable(cmds[1])
	case "COLUMN", "COL":
		return dropColumn(cmds[1])
	default:
		return fmt.Errorf("unknown DROP type, must be TABLE or COLUMN")
	}

}

func createTable(cmd string) error {
	sc, err := createSchema(cmd)
	if err != nil {
		return err
	}
	Database.AlterDatabase(sc)
	return nil
}

func createColumn(cmd string) error {
	sc, err := createSchema(cmd)
	if err != nil {
		return err
	}
	var tn string
	for k := range sc {
		tn = k
		break
	}
	if !Database.ContainsTable(tn) {
		return fmt.Errorf("%s is not a known table")
	}
	Database.AlterDatabase(sc)
	return nil
}
func dropTable(cmd string) error {
	Database.AlterDatabase(db.Schema{cmd: nil})
	return nil
}

func dropColumn(cmd string) error {
	sc, err := createSchema(cmd)
	if err != nil {
		return err
	}
	var tn string
	for k := range sc {
		tn = k
		break
	}
	if !Database.ContainsTable(tn) {
		return fmt.Errorf("%s is not a known table")
	}
	for k := range sc[tn] {
		sc[tn][k] = false
	}
	Database.AlterDatabase(sc)
	return nil
}

func createSchema(cmd string) (db.Schema, error) {
	cmds := strings.SplitN(cmd, " ", 2)
	if len(cmds) < 2 {
		return nil, fmt.Errorf("no columns stated for table %q", cmds[0])
	}
	_, c, err := queryparse.ParseList(strings.TrimSpace(cmds[1]))
	if err != nil {
		return nil, err
	}
	cols := map[string]bool{}
	for _, col := range c {
		cols[col] = true
	}
	return db.Schema{cmds[0]: cols}, nil
}
