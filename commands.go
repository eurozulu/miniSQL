package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"tinydb/db"
	"tinydb/queryparse"
)

func readCommands(ctx context.Context, out io.Writer, done chan bool) {
	defer close(done)

	scn := bufio.NewScanner(os.Stdin)
	out.Write([]byte(">"))
	for scn.Scan() {
		cmd := strings.TrimSpace(scn.Text())
		args := strings.SplitN(cmd, " ", 2)
		var err error
		switch args[0] {
		case "EXIT", "X", "QUIT":
			return
		case "SELECT", "INSERT", "DELETE", "UPDATE":
			err = queryCommand(ctx, cmd, out)
		case "CREATE":
			err = createCommand(strings.TrimSpace(args[1]), out)
		case "DROP":
			err = dropCommand(strings.TrimSpace(args[1]), out)
		case "RESTORE":
			err = restoreCommand(strings.TrimSpace(args[1]), out)
		case "DUMP":
			err = dumpCommand(strings.TrimSpace(args[1]), out)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "\n%s\n", err)
		}
		out.Write([]byte(">"))
	}
}

func queryCommand(ctx context.Context, cmd string, out io.Writer) error {
	qp := &queryparse.QueryParser{}
	q, err := qp.Parse(cmd)
	if err != nil {
		return err
	}
	rCh, err := q.Execute(ctx, tdb)
	if err != nil {
		return err
	}
	result := map[string][]db.Values{}
	for r := range rCh {
		result[r.TableName()] = append(result[r.TableName()], r.Values())
	}
	if len(result) == 0 {
		fmt.Fprintf(out, "no results\n")
		return nil
	}

	for t, v := range result {
		fmt.Fprintf(out, "Table: %s\n", t)
		cols := orderedColumnNames(v[0])
		fmt.Fprintf(out, "%s\n", strings.Join(cols, "\t"))
		orderedColumnValues(cols, out, v)
	}
	return nil
}

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
func dumpCommand(cmd string, out io.Writer) error {
	if cmd == "" {
		return fmt.Errorf("must specifiy the file path to write to")
	}
	return db.Dump(cmd, tdb)
}
func restoreCommand(cmd string, out io.Writer) error {
	if cmd == "" {
		return fmt.Errorf("must specifiy the file path to restore from")
	}
	return db.Restore(cmd, tdb)
}

func createTable(cmd string) error {
	sc, err := createSchema(cmd)
	if err != nil {
		return err
	}
	tdb.AlterDatabase(sc)
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
	if !tdb.ContainsTable(tn) {
		return fmt.Errorf("%s is not a known table")
	}
	tdb.AlterDatabase(sc)
	return nil
}
func dropTable(cmd string) error {
	tdb.AlterDatabase(db.Schema{cmd: nil})
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
	if !tdb.ContainsTable(tn) {
		return fmt.Errorf("%s is not a known table")
	}
	for k := range sc[tn] {
		sc[tn][k] = false
	}
	tdb.AlterDatabase(sc)
	return nil
}

func orderedColumnNames(values db.Values) []string {
	keys := make([]string, len(values))
	var i int
	for k := range values {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	return keys
}

func orderedColumnValues(cols []string, out io.Writer, values []db.Values) {
	for _, v := range values {
		for _, c := range cols {
			fmt.Fprintf(out, "%s\t", valueString(v[c]))
		}
		fmt.Fprintln(out)
	}
}

func createSchema(cmd string) (db.Schema, error) {
	cmds := strings.SplitN(cmd, " ", 2)
	var cols map[string]bool
	if len(cmds) > 1 {
		_, c, err := queryparse.ParseList(strings.TrimSpace(cmds[1]))
		if err != nil {
			return nil, err
		}
		cols = map[string]bool{}
		for _, col := range c {
			cols[col] = true
		}
	}
	return db.Schema{cmds[0]: cols}, nil
}

func valueString(v *string) string {
	if v != nil {
		return *v
	}
	return db.NULL
}
