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

	chResult := make(chan Result)
	chOut := chResult
	if q.Into != "" {
		chOut, err = q.insertInto(ctx, db, chResult)
		if err != nil {
			return nil, fmt.Errorf("problem with target table  %s", err)
		}
	}
	go func(sq SelectQuery, results chan<- Result) {
		defer close(results)
		t, _ := db.Table(q.TableName)
		keys := sq.Where.Keys(ctx, t)
		for {
			select {
			case <-ctx.Done():
				return
			case id, ok := <-keys:
				if !ok {
					return
				}
				v, err := t.Select(id, sq.Columns)
				if err != nil {
					log.Println(err)
					return
				}
				select {
				case <-ctx.Done():
					return
				case results <- NewResult(q.TableName, v):
				}
			}
		}
	}(q, chResult)
	return chOut, nil
}

func (q SelectQuery) insertInto(ctx context.Context, db *tinydb.TinyDB, ch <-chan Result) (chan Result, error) {
	t := q.Into
	if db.ContainsTable(t) {
		return nil, fmt.Errorf("table %q already exists. Use INSERT INTO to insert into existing table")
	}
	cols := removeColumn("_id", q.Columns)
	err := alterTable(t, cols, db)
	if err != nil {
		return nil, err
	}

	chOut := make(chan Result)
	go func(table string, cols []string, chIn <-chan Result, chOut chan<- Result) {
		defer close(chOut)
		for {
			select {
			case <-ctx.Done():
				return
			case r, ok := <-chIn:
				if !ok {
					return
				}
				ir, err := insertResult(ctx, db, table, cols, r.Values())
				if err != nil {
					e := fmt.Sprintf("inserting values  %s", err)
					ir = NewResult(table, tinydb.Values{"ERROR": &e})
				}
				select {
				case <-ctx.Done():
					return
				case chOut <- ir:
				}
				if err != nil {
					return
				}
			}
		}
	}(t, cols, ch, chOut)
	return chOut, nil
}

func insertResult(ctx context.Context, db *tinydb.TinyDB, table string, cols []string, values tinydb.Values) (Result, error) {
	iq := &InsertQuery{
		TableName: table,
		Columns:   cols,
		Values:    values,
	}
	irs, err := iq.Execute(ctx, db)
	if err != nil {
		return nil, err
	}
	ir, ok := <-irs
	if !ok {
		return nil, fmt.Errorf("No result found for insert into %s", table)
	}
	return ir, nil
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

func removeColumn(c string, columns []string) []string {
	i := stringutil.ContainsString(c, columns)
	if i < 0 {
		return columns
	}
	if i == len(columns)-1 {
		return columns[:i]
	}
	return append(columns[:i], columns[i+1:]...)
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
