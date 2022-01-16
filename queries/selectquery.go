package queries

import (
	"context"
	"eurozulu/miniSQL/queries/whereclause"
	"eurozulu/miniSQL/stringutil"
	"fmt"
	"log"
	"strings"

	"eurozulu/miniSQL/minisql"
)

type SelectQuery struct {
	TableName string
	Columns   []string
	Names     []string
	Where     whereclause.WhereClause
	Into      string
	OrderBy   *sortedResult
}

func (q SelectQuery) Execute(ctx context.Context, db *minisql.MiniDB) (<-chan Result, error) {
	t, err := db.Table(q.TableName)
	if err != nil {
		return nil, err
	}

	cols, err := expandColumnNames(t, q.Columns)
	if err != nil {
		return nil, fmt.Errorf("%w in table %s", err, q.TableName)
	}
	q.Columns = cols

	if q.Into != "" && db.ContainsTable(q.Into) {
		return nil, fmt.Errorf("table %q already exists. Use INSERT INTO to insert into existing table", q.Into)
	}

	ch := make(chan Result)
	var chOut <-chan Result = ch
	if q.OrderBy != nil {
		chOut = q.OrderBy.Sort(ctx, ch)
	}
	go func(sq *SelectQuery, results chan<- Result) {
		defer close(results)

		var err error
		if sq.Into != "" {
			err = sq.executeSelectINTO(ctx, db, results)
		} else {
			err = sq.executeSelect(ctx, db, results)
		}
		if err != nil {
			es := err.Error()
			select {
			case <-ctx.Done():
				log.Println(err)
				return
			case results <- NewResult(sq.TableName, minisql.Values{"ERROR": &es}):
			}
		}
	}(&q, ch)
	return chOut, nil
}

func (q SelectQuery) executeSelect(ctx context.Context, db *minisql.MiniDB, results chan<- Result) error {
	t, _ := db.Table(q.TableName)
	keys := q.Where.Keys(ctx, t)
	for {
		select {
		case <-ctx.Done():
			return nil
		case id, ok := <-keys:
			if !ok {
				return nil
			}
			v, err := t.Select(id, q.Columns)
			if err != nil {
				return err
			}
			v = q.nameValues(v)
			select {
			case <-ctx.Done():
				return nil
			case results <- NewResult(q.TableName, v):
			}
		}
	}
}

func (q SelectQuery) executeSelectINTO(ctx context.Context, db *minisql.MiniDB, results chan<- Result) error {
	// create the new table based on the query columns
	cols := removeIDColumn(q.Columns)
	if err := createTable(q.Into, cols, db); err != nil {
		return err
	}

	// flip SELECT INTO, into an INSERT SELECT, removing the SELECT INTO name
	q.Into = ""
	iq := InsertQuery{
		TableName: q.Into,
		Columns:   cols,
		Select:    &q,
	}
	return iq.insertSelect(ctx, db, results)
}

func (q SelectQuery) nameValues(values minisql.Values) minisql.Values {
	vals := minisql.Values{}
	for i, col := range q.Columns {
		vals[q.Names[i]] = values[col]
	}
	return vals
}

func createTable(table string, columns []string, db *minisql.MiniDB) error {
	cols := map[string]bool{}
	for _, c := range columns {
		cols[c] = true
	}
	db.AlterDatabase(minisql.Schema{
		table: cols,
	})
	return nil
}

// expandColumnNames expands the given list of column names and validates the given list as known names.
// columns may contain "*" wild card to indicate all column names.
func expandColumnNames(t minisql.Table, columns []string) ([]string, error) {
	tcols := t.ColumnNames()
	if len(columns) == 0 {
		return tcols, nil
	}
	var cols []string
	for _, c := range columns {
		if c == "*" {
			cols = append(cols, tcols...)
		} else {
			if !stringutil.Contains(c, tcols) {
				return nil, fmt.Errorf("%s is an unknown column", c)
			}
			cols = append(cols, c)
		}
	}
	return cols, nil
}

func removeIDColumn(cols []string) []string {
	i := stringutil.IndexOf("_id", cols)
	if i < 0 {
		return cols
	}
	if i == len(cols)-1 {
		return cols[:i]
	}
	c := make([]string, len(cols)-1)
	copy(c, cols[:i])
	copy(c[i:], cols[i+1:])
	return c
}

func parseColumnNames(q string) ([]string, []string, error) {
	cols := stringutil.SplitTrim(q, ",")
	names := make([]string, len(cols))

	for i, c := range cols {
		n := c
		if strings.Contains(c, " ") {
			col, rest := stringutil.FirstWord(c)
			as, name := stringutil.FirstWord(rest)
			if !strings.EqualFold(as, "AS") {
				return nil, nil, fmt.Errorf("unexpected value found after column %q. Expected ',' or 'AS'", c)
			}
			if name == "" {
				return nil, nil, fmt.Errorf("expected column alias name not found after AS: %q.", c)
			}
			n = name
			cols[i] = col
		}
		if stringutil.Contains(n, names) {
			return nil, nil, fmt.Errorf("column name %q appears more than once", n)
		}
		names[i] = n
	}
	return cols, names, nil
}

// NewSelectQuery creates a SelectQuery from the given string.
// String should contain a valid SELECT query, without the preceeding SELECT statement.
// i.e. it should begin with a comma delimited list of column names.
// e.g. "col1, col2, col3 FROM mytable WHERE col3=NULL"
func NewSelectQuery(query string) (*SelectQuery, error) {
	var into string
	iti := strings.Index(strings.ToUpper(query), "INTO")
	if iti > 0 {
		q := strings.TrimSpace(query[iti+len("INTO"):])
		into, q = stringutil.FirstWord(q)
		if into == "" {
			return nil, fmt.Errorf("missing table name after INTO")
		}
		query = strings.Join([]string{query[:iti], q}, " ")
	}
	fi := strings.Index(strings.ToUpper(query), "FROM")
	if fi < 0 {
		return nil, fmt.Errorf("missing FROM in query")
	}

	cols, names, err := parseColumnNames(strings.TrimSpace(query[:fi]))
	if err != nil {
		return nil, err
	}

	// trim off FROM and read table name
	_, query = stringutil.FirstWord(query[fi:])
	table, rest := stringutil.FirstWord(query)
	if table == "" {
		return nil, fmt.Errorf("no table name given")
	}

	// Check if an ORDER BY is present
	var order *sortedResult
	if i := strings.Index(strings.ToUpper(rest), "ORDER"); i >= 0 {
		order, err = newSortedResult(rest[i:])
		if err != nil {
			return nil, err
		}
		rest = strings.TrimSpace(rest[:i])
	}

	// Check for a WHERE clause (Query always has a where, but can be 'empty' == ALL keys in the table)
	where, err := whereclause.NewWhere(rest)
	if err != nil {
		return nil, err
	}
	return &SelectQuery{
		TableName: table,
		Columns:   cols,
		Names:     names,
		Where:     where,
		Into:      into,
		OrderBy:   order,
	}, nil
}
