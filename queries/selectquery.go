package queries

import (
	"context"
	"eurozulu/tinydb/stringutil"
	"fmt"
	"log"
	"strings"

	"eurozulu/tinydb/tinydb"
)

type SelectQuery struct {
	TableName string
	Columns   []string
	Where     WhereClause
	Into      string
}

func (q SelectQuery) Execute(ctx context.Context, db *tinydb.TinyDB) (<-chan Result, error) {
	t, err := db.Table(q.TableName)
	if !db.ContainsTable(q.TableName) {
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
	if !db.ContainsTable(q.Into) {
		err := createNewTable(fmt.Sprintf("%s (%s)", q.Into, strings.Join(q.Columns, ",")), db)
		if err != nil {
			return nil, err
		}
	}

	chOut := make(chan Result)
	go func(q *SelectQuery, chIn <-chan Result, chOut chan<- Result) {
		defer close(chOut)
		for {
			select {
			case <-ctx.Done():
				return
			case r, ok := <-chIn:
				if !ok {
					return
				}
				delete(r.Values(), "_id")
				ir, err := insertResult(ctx, db, q.Into, q.Columns, r.Values())
				if err != nil {
					e := fmt.Sprintf("inserting values  %s", err)
					ir = NewResult(q.Into, tinydb.Values{"ERROR": &e})
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
	}(&q, ch, chOut)
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

func createNewTable(q string, db *tinydb.TinyDB) error {
	sc, err := tinydb.NewSchema(q)
	if err != nil {
		return err
	}
	db.AlterDatabase(sc)
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

// NewSelectQuery creates a SelectQuery from the given string.
// String should contain a valid SELECT query, without the preceeding SELECT statement.
// i.e. it should begin with a comma delimited list of column names.
// e.g. "col1, col2, col3 FROM mytable WHERE col3=NULL"
func NewSelectQuery(query string) (*SelectQuery, error) {
	var into string
	iti := strings.Index(strings.ToUpper(query), "INTO")
	if iti > 0 {
		is := strings.SplitN(strings.TrimSpace(query[iti+len("INTO"):]), " ", 2)
		into = is[0]
		q := strings.TrimSpace(query[:iti])
		is = append([]string{q}, is[1:]...)
		query = strings.Join(is, " ")
	}
	fi := strings.Index(strings.ToUpper(query), "FROM")
	if fi < 0 {
		return nil, fmt.Errorf("missing FROM in query")
	}

	cols := strings.Split(strings.TrimSpace(query[:fi]), ",")
	query = strings.TrimSpace(query[fi+len("FROM)"):])
	cmd := strings.SplitN(query, " ", 2)
	if cmd[0] == "" {
		return nil, fmt.Errorf("no table name given")
	}
	var where WhereClause
	if len(cmd) > 1 {
		w, err := NewWhere(strings.Join(cmd[1:], " "))
		if err != nil {
			return nil, err
		}
		where = w
	}
	return &SelectQuery{
		TableName: cmd[0],
		Columns:   cols,
		Where:     where,
		Into:      into,
	}, nil
}
