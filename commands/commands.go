package commands

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"

	"eurozulu/tinydb/db"
	"eurozulu/tinydb/queryparse"
	"github.com/eurozulu/commandline"
)

var Database *db.TinyDB

const historyLocation = "$HOME/.tinydb_history"

func ReadCommands(ctx context.Context, out io.Writer, done chan bool) {
	defer close(done)

	cli := commandline.NewCommandLine()
	if err := cli.LoadHistory(historyLocation); err != nil {
		log.Println(err)
	} else {
		defer cli.SaveHistory(historyLocation)
	}

	out.Write([]byte(">"))
	for {
		ln, err := cli.ReadCommand()
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Println()

		args := strings.SplitN(strings.TrimSpace(ln), " ", 2)
		switch strings.ToUpper(args[0]) {
		case "":
			err = nil //nop
		case "EXIT", "X", "QUIT":
			return
		case "SELECT", "INSERT", "DELETE", "UPDATE":
			err = queryCommand(ctx, strings.TrimSpace(ln), out)
		case "CREATE":
			if len(args) > 1 {
				err = createCommand(strings.TrimSpace(args[1]), out)
			} else {
				err = fmt.Errorf("no create parameter")
			}

		case "DROP":
			if len(args) > 1 {
				err = dropCommand(strings.TrimSpace(args[1]), out)
			} else {
				err = fmt.Errorf("no drop parameter")
			}

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
		default:
			err = fmt.Errorf("%q is an unknown command", args[0])
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
		}

		_, _ = out.Write([]byte(">"))
	}
}

func queryCommand(ctx context.Context, cmd string, out io.Writer) error {
	qp := &queryparse.QueryParser{}
	q, err := qp.Parse(cmd)
	if err != nil {
		return err
	}
	rCh, err := q.Execute(ctx, Database)
	if err != nil {
		return err
	}
	result := map[string][]db.Values{}
	for r := range rCh {
		result[r.TableName()] = append(result[r.TableName()], r.Values())
	}
	if len(result) == 0 {
		if _, err := fmt.Fprintf(out, "no results\n"); err != nil {
			return err
		}
		return nil
	}

	for t, v := range result {
		if _, err := fmt.Fprintf(out, "Table: %s\n", t); err != nil {
			return err
		}
		cols := orderedColumnNames(v[0])
		if _, err := fmt.Fprintf(out, "%s\n", strings.Join(cols, "\t")); err != nil {
			return err
		}
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
func DumpCommand(cmd string, out io.Writer) error {
	if cmd == "" {
		return fmt.Errorf("must specifiy the file path to write to")
	}
	if err := db.Dump(cmd, Database); err != nil {
		return err
	}
	_, err := fmt.Fprintf(out, "dumped %d tables to %s\n", len(Database.TableNames()), cmd)
	return err
}

func RestoreCommand(cmd string, out io.Writer) error {
	if cmd == "" {
		return fmt.Errorf("must specifiy the file path to restore from")
	}
	if err := db.Restore(cmd, Database); err != nil {
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

func DescribeCommand(cmd string, out io.Writer) error {
	desc, err := Database.Describe(cmd)
	if err != nil {
		return err
	}
	desc = append([]string{fmt.Sprintf("Table: %s", cmd)}, desc...)
	_, err = fmt.Fprintln(out, strings.Join(desc, "\n"))
	return err
}

func TablesCommand(cmd string, out io.Writer) error {
	_, err := fmt.Fprintln(out, strings.Join(Database.TableNames(), "\n"))
	return err
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

func valueString(v *string) string {
	if v != nil {
		return *v
	}
	return db.NULL
}
