package commands

import (
	"eurozulu/tinydb/db"
	"eurozulu/tinydb/queryparse"
	"fmt"
	"io"
	"strings"
)

var structueHelp = "Supports CREATE and DROP to structure the database tables and columns\n" +
	"\tCREATE TABLE | COLUMN <table> (<column> [,<column>...] )\n" +
	"\t\te.g. CREATE TABLE mytable (col1, col2, col3)\n" +
	"\tDROP TABLE | COLUMN <table> (<column> [,<column>...] )\n" +
	"\t\te.g. DROP COLUMN mytable (col3)\n" +
	"\t\t     DROP TABLE mytable\n"

func createCommand(cmd string, out io.Writer) error {
	cmds := strings.SplitN(cmd, " ", 2)
	if len(cmds) < 2 {
		return fmt.Errorf("invalid CREATE command. Must state TABLE or COLUMN and a table name")
	}
	switch strings.ToUpper(cmds[0]) {
	case "TABLE":
		return createTable(cmds[1])
	case "COLUMN", "COL":
		return createColumn(strings.Join(cmds[1:], " "))
	default:
		return fmt.Errorf("%s is an unknown CREATE type, must be TABLE or COLUMN", strings.ToUpper(cmds[0]))
	}
}
func dropCommand(cmd string, out io.Writer) error {
	cmds := strings.SplitN(strings.TrimSpace(cmd), " ", 2)
	dt := strings.TrimSpace(strings.ToUpper(cmds[0]))
	if dt == "" {
		return fmt.Errorf("missing DROP type. must use TABLE or COLUMN")
	}
	if len(cmds) < 2 || cmds[1] == "" {
		return fmt.Errorf("DROP %s missing table name", dt)
	}
	cmd = strings.TrimSpace(cmds[1])
	switch dt {
	case "TABLE":
		return dropTable(cmd, out)
	case "COLUMN", "COL":
		return dropColumn(cmd, out)
	default:
		return fmt.Errorf("DROP %s, is not a known drop type, must be TABLE or COLUMN", dt)
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
		return fmt.Errorf("%q is not a known table", tn)
	}
	Database.AlterDatabase(sc)
	return nil
}
func dropTable(tableName string, out io.Writer) error {
	if !Database.ContainsTable(tableName) {
		return fmt.Errorf("%q is not a known table", tableName)
	}
	Database.AlterDatabase(db.Schema{tableName: nil})
	_, err := fmt.Fprintf(out, "table %s dropped\n", tableName)
	return err
}

func dropColumn(cmd string, out io.Writer) error {
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
		return fmt.Errorf("%s is not a known table", tn)
	}
	cols, err := Database.Describe(tn)
	if err != nil {
		return err
	}
	var colNames []string
	for k := range sc[tn] {
		if containsString(k, cols) < 0 {
			return fmt.Errorf("%q is not a known column in table %s", k, tn)
		}
		colNames = append(colNames, k)
		sc[tn][k] = false
	}
	Database.AlterDatabase(sc)
	var cs string
	if len(colNames) != 1 {
		cs = "s"
	}
	_, err = fmt.Fprintf(out, "dropped column%s %s, in table %s\n", cs, strings.Join(colNames, ", "), tn)
	return err
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

func containsString(s string, ss []string) int {
	for i, sz := range ss {
		if sz == s {
			return i
		}
	}
	return -1
}
