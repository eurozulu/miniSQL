package commands

import (
	"eurozulu/miniSQL/minisql"
	"eurozulu/miniSQL/stringutil"
	"fmt"
	"io"
	"strings"
)

var structueHelp = "Supports CREATE and DROP to structure the database tables and columns\n" +
	"\tCREATE TABLE | COLUMN <table> (<column> [,<column>...] )\n" +
	"\t\te.g. CREATE TABLE mytable (col1, col2, col3)\n" +
	"\tDROP TABLE | COLUMN <table> (<column> [,<column>...] )\n" +
	"\t\te.g. DROP COLUMN mytable (col1, col3)\n" +
	"\t\t     DROP TABLE mytable\n" +
	"\tDROP DATABASE\tDrops entire database (all tables)\n"

func createCommand(cmd string, out io.Writer) error {
	ct, rest := stringutil.FirstWord(cmd)
	if ct == "" {
		return fmt.Errorf("invalid CREATE command. Must state TABLE or COLUMN and a table name")
	}
	ct = strings.ToUpper(ct)
	switch ct {
	case "TABLE":
		return createTable(rest, out)
	case "COLUMN", "COL":
		return createColumn(rest, out)
	default:
		return fmt.Errorf("%s is an unknown CREATE type, must be TABLE or COLUMN", ct)
	}
}
func dropCommand(cmd string, out io.Writer) error {
	dt, rest := stringutil.FirstWord(cmd)
	if dt == "" {
		return fmt.Errorf("missing DROP type. must use DATABASE, TABLE or COLUMN")
	}
	dt = strings.ToUpper(dt)

	switch dt {
	case "TABLE":
		return dropTable(rest, out)
	case "COLUMN", "COL":
		return dropColumn(rest, out)
	case "DATABASE":
		return dropDatabase(rest, out)
	default:
		return fmt.Errorf("DROP %s, is not a known drop type, must be TABLE or COLUMN", dt)
	}

}

func createTable(cmd string, out io.Writer) error {
	sc, err := minisql.NewSchema(cmd)
	if err != nil {
		return err
	}
	Database.AlterDatabase(sc)
	_, err = fmt.Fprintf(out, "created table %v\n", sc)
	return err
}

func createColumn(cmd string, out io.Writer) error {
	sc, err := minisql.NewSchema(cmd)
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
	Database.AlterDatabase(minisql.Schema{tableName: nil})
	_, err := fmt.Fprintf(out, "table %s dropped\n", tableName)
	return err
}

func dropColumn(cmd string, out io.Writer) error {
	sc, err := minisql.NewSchema(cmd)
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
		if !stringutil.Contains(k, cols) {
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
	tbs := strings.Split(cmd, ",")
	if cmd == "" || tbs[0] == "" {
		tbs = Database.TableNames()
	}
	sc, err := minisql.NewSchemaFromTables(Database, tbs...)
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
