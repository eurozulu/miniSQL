package commands

import (
	"context"
	"eurozulu/miniSQL/minisql"
	"eurozulu/miniSQL/queries"
	"eurozulu/miniSQL/queries/whereclause"
	"fmt"
	"io"
	"sort"
	"strings"
)

var queryHelp = "Query commands: SELECT, INSERT, UPDATE and DELETE.\n" +
	"\tSELECT <table> [INTO <newtable>] FROM <column>[,<column>...] [WHERE <column>=<value>|NULL [AND <column>=<value>|NULL]...]\n" +
	"\t\t<table> must be an existing table\n" +
	"\t\tINTO is optional, when given with a tablename, inserts the results into that table\n" +
	"\t\t\tIf the INTO table exists, must have matching column names from the result.\n" +
	"\t\t\tIf the INTO table doesn't exists, it is created with the columns of the result\n" +
	"\t\tFROM must be followed by one or more, comma deliminated column names from the named table.\n" +
	"\t\tWHERE optional whereclause clause to filter result.  Current only supports AND and equality\n" +
	"\t\t\te.g. WHERE col1=1 AND col2=thatthing\n" +
	"\t\t\tcolumn can also be tested for NULL using the 'NULL' keyword\n" +
	"\tINSERT INTO <table> (<column> [,<column>...]) VALUES (<value> [,<value>...])\n" +
	"\tUPDATE <table> SET <column>=<value>|NULL [,<column>=<value>|NULL...][ WHERE <column>=<value>|NULL [AND <column>=<value>|NULL]...]\n" +
	"\tDELETE FROM <table> [ WHERE <column>=<value>|NULL [AND <column>=<value>|NULL]...]\n"

func queryCommand(ctx context.Context, cmd string, out io.Writer) error {
	q, err := queries.ParseQuery(cmd)
	if err != nil {
		return err
	}
	rCh, err := q.Execute(ctx, Database)
	if err != nil {
		return err
	}
	result := map[string][]minisql.Values{}
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

func orderedColumnNames(values minisql.Values) []string {
	keys := make([]string, len(values))
	var i int
	for k := range values {
		keys[i] = k
		i++
	}
	sort.Slice(keys, func(i, j int) bool {
		s := []string{keys[i], keys[j]}
		sort.Strings(s)
		return s[0] == keys[i]
	})
	return keys
}

func orderedColumnValues(cols []string, out io.Writer, values []minisql.Values) {
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
	return whereclause.NULL
}
