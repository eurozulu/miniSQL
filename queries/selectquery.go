package queries

import (
	"context"
	"eurozulu/tinydb/queries/whereclause"
	"eurozulu/tinydb/stringutil"
	"fmt"
	"log"
	"strings"

	"eurozulu/tinydb/tinydb"
)

type SelectQuery struct {
	TableName string
	Columns   []string
	Where     whereclause.WhereClause
	Into      string
}

func (q SelectQuery) Execute(ctx context.Context, db *tinydb.TinyDB) (<-chan Result, error) {
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
			case results <- NewResult(sq.TableName, tinydb.Values{"ERROR": &es}):
			}
		}

	}(&q, ch)
	return ch, nil
}

func (q SelectQuery) executeSelectINTO(ctx context.Context, db *tinydb.TinyDB, results chan<- Result) error {
	// create the new table based on the query columns
	cols := removeIDColumn(q.Columns)
	if err := alterTable(q.Into, cols, db); err != nil {
		return err
	}

	// flip SELECT INTO, into an INSERT SELECT, removing the SELECT INTO name
	iq := InsertQuery{
		TableName: q.Into,
		Columns:   cols,
		Select: &SelectQuery{
			TableName: q.TableName,
			Columns:   q.Columns,
			Where:     q.Where,
			Into:      "",
		},
	}
	return iq.insertSelect(ctx, db, results)
}

func (q SelectQuery) executeSelect(ctx context.Context, db *tinydb.TinyDB, results chan<- Result) error {
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
			select {
			case <-ctx.Done():
				return nil
			case results <- NewResult(q.TableName, v):
			}
		}
	}
}

func alterTable(table string, columns []string, db *tinydb.TinyDB) error {
	cols := map[string]bool{}
	for _, c := range columns {
		cols[c] = true
	}
	db.AlterDatabase(tinydb.Schema{
		table: cols,
	})
	return nil
}

// expandColumnNames expands the given list of column names and validates the given list as known names.
// columns may contain "*" wild card to indicate all column names.
func expandColumnNames(t tinydb.Table, columns []string) ([]string, error) {
	tcols := t.ColumnNames()
	if len(columns) == 0 {
		return tcols, nil
	}
	var cols []string
	for _, c := range columns {
		if c == "*" {
			cols = append(cols, tcols...)
		} else {
			if stringutil.ContainsString(c, tcols) < 0 {
				return nil, fmt.Errorf("%s is an unknown column", c)
			}
			cols = append(cols, c)
		}
	}
	return cols, nil
}

func removeIDColumn(cols []string) []string {
	i := stringutil.ContainsString("_id", cols)
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

	cols := strings.Split(strings.TrimSpace(query[:fi]), ",")
	// trim off FROM
	_, query = stringutil.FirstWord(query[fi:])
	table, rest := stringutil.FirstWord(query)
	if table == "" {
		return nil, fmt.Errorf("no table name given")
	}

	where, err := whereclause.NewWhere(rest)
	if err != nil {
		return nil, err
	}
	return &SelectQuery{
		TableName: table,
		Columns:   cols,
		Where:     where,
		Into:      into,
	}, nil
}
