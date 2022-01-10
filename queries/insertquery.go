package queries

import (
	"context"
	"eurozulu/tinydb/stringutil"
	"eurozulu/tinydb/tinydb"
	"fmt"
	"strings"
)

type InsertQuery struct {
	TableName string
	Columns   []string
	Values    tinydb.Values
}

func (q InsertQuery) Execute(ctx context.Context, db *tinydb.TinyDB) (<-chan Result, error) {
	t, err := db.Table(q.TableName)
	if !db.ContainsTable(q.TableName) {
		return nil, err
	}
	cols, err := expandColumnNames(t, q.Columns)
	if err != nil {
		return nil, fmt.Errorf("%w in table %s", err, q.TableName)
	}
	q.Columns = cols
	if len(q.Columns) != len(q.Values) {
		return nil, fmt.Errorf("columns / values count mismatch")
	}
	ch := make(chan Result)
	go func(sq *InsertQuery, results chan<- Result) {
		defer close(results)
		t, _ := db.Table(q.TableName)

		var r Result
		if id, err := t.Insert(q.Values); err != nil {
			e := fmt.Sprintf("failed to insert into table %q  %w", q.TableName, err)
			r = NewResult(q.TableName, tinydb.Values{"ERROR": &e})
		} else {
			idp := fmt.Sprintf("%s:%d", q.TableName, id)
			r = NewResult(q.TableName, tinydb.Values{"inserted": &idp})
		}
		select {
		case <-ctx.Done():
			return
		case results <- r:
		}

	}(&q, ch)
	return ch, nil
}

func valuesList(keys []string, vals []string) (tinydb.Values, error) {
	if len(keys) != len(vals) {
		return nil, fmt.Errorf("columns / values count mismatch")
	}
	vm := tinydb.Values{}
	for i, k := range keys {
		vs := strings.Trim(vals[i], "'")
		vm[k] = &vs
	}
	return vm, nil
}

// NewInsertQuery creates a new insert query from the given string
// Query should be a valid insert without the preceeding INSERT.
// i.e it should begin with the INTO keyword.
// e.g. "INTO mytable (col1, col2, col3) VALUES ("one", "two", "three") "
func NewInsertQuery(q string) (*InsertQuery, error) {
	if !strings.HasPrefix(strings.ToUpper(q), "INTO") {
		return nil, fmt.Errorf("missing INTO in query")
	}
	qs := strings.SplitN(strings.TrimSpace(q[len("INTO"):]), " ", 2)
	tn := qs[0]
	if tn == "" {
		return nil, fmt.Errorf("missing table name after INTO")
	}
	q = strings.Join(qs[1:], " ")
	vi := strings.Index(strings.ToUpper(q), "VALUES")
	if vi < 0 {
		return nil, fmt.Errorf("invalid INSERT query.  No VALUES given")
	}
	_, cols, err := stringutil.ParseList(q[:vi])
	if err != nil {
		return nil, fmt.Errorf("invalid columns %s", err)
	}

	q = q[vi+len("VALUES"):]
	_, vals, err := stringutil.ParseList(strings.TrimSpace(q))
	vs, err := valuesList(cols, vals)
	if err != nil {
		return nil, err
	}
	return &InsertQuery{
		TableName: tn,
		Columns:   cols,
		Values:    vs,
	}, nil
}
