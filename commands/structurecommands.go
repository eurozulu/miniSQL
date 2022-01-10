package commands

import (
	"eurozulu/tinydb/queryparser"
	"eurozulu/tinydb/stringutil"
	"eurozulu/tinydb/tinydb"
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
	ct := strings.ToUpper(strings.TrimSpace(cmds[0]))
	if ct == "" {
		return fmt.Errorf("invalid CREATE command. Must state TABLE or COLUMN and a table name")
	}
	cmd = strings.Join(cmds[1:], " ")

	switch strings.ToUpper(cmds[0]) {
	case "TABLE":
		return createTable(cmd, out)
	case "COLUMN", "COL":
		return createColumn(cmd, out)
	default:
		return fmt.Errorf("%s is an unknown CREATE type, must be TABLE or COLUMN", strings.ToUpper(cmds[0]))
	}
}
func dropCommand(cmd string, out io.Writer) error {
	cmds := strings.SplitN(strings.TrimSpace(cmd), " ", 2)
	dt := strings.TrimSpace(strings.ToUpper(cmds[0]))
	if dt == "" {
		return fmt.Errorf("missing DROP type. must use DATABASE, TABLE or COLUMN")
	}
	cmd = strings.TrimSpace(strings.Join(cmds[1:], " "))
	switch dt {
	case "TABLE":
		return dropTable(cmd, out)
	case "COLUMN", "COL":
		return dropColumn(cmd, out)
	case "DATABASE":
		return dropDatabase(cmd, out)
	default:
		return fmt.Errorf("DROP %s, is not a known drop type, must be TABLE or COLUMN", dt)
	}

}

func createTable(cmd string, out io.Writer) error {
	sc, err := createSchema(cmd)
	if err != nil {
		return err
	}
	Database.AlterDatabase(sc)
	_, err = fmt.Fprintf(out, "created table %v\n", sc)
	return err
}

func createColumn(cmd string, out io.Writer) error {
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
	_, err = fmt.Fprintf(out, "created columns %v\n", sc)
	return err
}
func dropTable(tableName string, out io.Writer) error {
	if tableName == "" {
		return fmt.Errorf("no table name given")
	}
	if !Database.ContainsTable(tableName) {
		return fmt.Errorf("%q is not a known table", tableName)
	}
	Database.AlterDatabase(tinydb.Schema{tableName: nil})
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
		if stringutil.ContainsString(k, cols) < 0 {
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

func dropDatabase(cmd string, out io.Writer) error {
	sc, err := createCurrentSchema(cmd)
	if err != nil {
		return err
	}
	// remove col defs from scheam to force drop of table
	for tn := range sc {
		sc[tn] = nil
	}
	var s string
	if len(sc) != 1 {
		s = "s"
	}
	Database.AlterDatabase(sc)
	_, err = fmt.Fprintf(out, "dropped %d table%s\n", len(sc), s)
	Prompt = ">"
	return err
}

func createCurrentSchema(cmd string) (tinydb.Schema, error) {
	if cmd == "" {
		cmd = strings.Join(Database.TableNames(), ",")
	}
	tns := strings.Split(cmd, ",")
	sc := tinydb.Schema{}
	for _, tn := range tns {
		t, err := Database.Table(tn)
		if err != nil {
			return nil, err
		}
		cols := map[string]bool{}
		for _, cn := range t.ColumnNames() {
			cols[cn] = true
		}
		sc[tn] = cols
	}
	return sc, nil
}

func createSchema(cmd string) (tinydb.Schema, error) {
	if cmd == "" {
		return nil, fmt.Errorf("no table name found")
	}
	cmds := strings.SplitN(cmd, " ", 2)
	if len(cmds) < 2 {
		return nil, fmt.Errorf("no columns stated for table %q", cmds[0])
	}
	_, c, err := queryparser.ParseList(strings.TrimSpace(cmds[1]))
	if err != nil {
		return nil, err
	}
	cols := map[string]bool{}
	for _, col := range c {
		cols[col] = true
	}
	return tinydb.Schema{cmds[0]: cols}, nil
}
