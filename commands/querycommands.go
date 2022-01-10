package commands

import (
	"context"
	"eurozulu/tinydb/db"
	"eurozulu/tinydb/queryparse"
	"fmt"
	"io"
	"sort"
	"strings"
)

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

func valueString(v *string) string {
	if v != nil {
		return *v
	}
	return db.NULL
}
