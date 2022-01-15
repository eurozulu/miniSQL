package queries

import (
	"context"
	"eurozulu/tinydb/stringutil"
	"eurozulu/tinydb/tinydb"
	"fmt"
	"log"
	"strings"
)

type InsertQuery struct {
	TableName string
	Columns   []string
	Values    tinydb.Values
	Select    *SelectQuery
}

func (q InsertQuery) Execute(ctx context.Context, db *tinydb.TinyDB) (<-chan Result, error) {
	// perform sanity checks on query before starting execution
	t, err := db.Table(q.TableName)
	if err != nil {
		return nil, err
	}
	cols, err := expandColumnNames(t, q.Columns)
	if err != nil {
		return nil, fmt.Errorf("%w in table %s", err, q.TableName)
	}
	q.Columns = cols
	ch := make(chan Result)

	go func(db *tinydb.TinyDB, sq *InsertQuery, results chan<- Result) {
		defer close(results)
		if sq.Values != nil {
			err = sq.insertValues(ctx, db, results, sq.Values)

		} else if sq.Select != nil {
			err = sq.insertSelect(ctx, db, results)

		} else {
			err = fmt.Errorf("Invalid INSERT query, no SELECT or VALUES to insert")
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
	}(db, &q, ch)
	return ch, nil
}

func (q InsertQuery) insertSelect(ctx context.Context, db *tinydb.TinyDB, results chan<- Result) error {
	// use sub context to cancel select if error encountered
	subCtx, cnl := context.WithCancel(ctx)
	defer cnl()

	rs, err := q.Select.Execute(subCtx, db)
	if err != nil {
		return fmt.Errorf("SELECT query of %s failed  %v", q.Select.TableName, err)
	}
	for r := range rs {
		vals := r.Values()
		delete(vals, "_id")
		if err = q.insertValues(ctx, db, results, vals); err != nil {
			return err
		}
	}
	return nil
}

func (q InsertQuery) insertValues(ctx context.Context, db *tinydb.TinyDB, results chan<- Result, values tinydb.Values) error {
	t, _ := db.Table(q.TableName)
	if len(q.Columns) != len(values) {
		return fmt.Errorf("columns / values count mismatch")
	}
	id, err := t.Insert(values)
	if err != nil {
		return fmt.Errorf("failed to insert into table %q  %w", q.TableName, err)
	}
	idp := fmt.Sprintf("%s:%d", q.TableName, id)
	select {
	case <-ctx.Done():
		break
	case results <- NewResult(q.TableName, tinydb.Values{"inserted": &idp}):
	}
	return nil
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

func newInsertSelectQuery(table string, cols []string, query string) (*InsertQuery, error) {
	q, err := NewSelectQuery(query)
	if err != nil {
		return nil, err
	}
	return &InsertQuery{
		TableName: table,
		Columns:   cols,
		Select:    q,
	}, nil
}

func newInsertValuesQuery(table string, cols []string, values string) (*InsertQuery, error) {
	valList, rest := stringutil.BracketedString(values)
	if valList == "" {
		return nil, fmt.Errorf("no values found after VALUES.  Place comma delimited values in brackets. Must be same amount of values as columns")
	}
	if strings.TrimSpace(rest) != "" {
		return nil, fmt.Errorf("unexpected text found %q after vqlues", rest)
	}
	vs, err := valuesList(cols, stringutil.SplitTrim(valList, ","))
	if err != nil {
		return nil, err
	}
	return &InsertQuery{
		TableName: table,
		Columns:   cols,
		Values:    vs,
	}, nil
}

// NewInsertQuery creates a new insert query from the given string
// i.e it should begin with the INTO keyword.
// e.g. "INTO mytable (col1, col2, col3) VALUES ("one", "two", "three") "
func NewInsertQuery(q string) (*InsertQuery, error) {
	// Strip any leading INSERT and INTO commands
	if strings.HasPrefix(strings.ToUpper(q), "INSERT") {
		_, q = stringutil.FirstWord(q)
	}
	if strings.HasPrefix(strings.ToUpper(q), "INTO") {
		_, q = stringutil.FirstWord(q)
	} else {
		return nil, fmt.Errorf("missing INTO in query")
	}
	table, rest := stringutil.FirstWord(q)
	if table == "" {
		return nil, fmt.Errorf("missing table name after INTO")
	}
	var colList string
	colList, rest = stringutil.BracketedString(rest)
	if colList == "" {
		return nil, fmt.Errorf("invalid INSERT query.  No columns found. list columns to insert, inside brackets")
	}
	cols := stringutil.SplitTrim(colList, ",")
	rest = strings.TrimSpace(rest)

	if strings.HasPrefix(strings.ToUpper(rest), "VALUES") {
		return newInsertValuesQuery(table, cols, strings.TrimSpace(rest[len("VALUES"):]))
	}
	if strings.HasPrefix(strings.ToUpper(rest), "SELECT") {
		return newInsertSelectQuery(table, cols, strings.TrimSpace(rest[len("SELECT"):]))
	}
	return nil, fmt.Errorf("invalid INSERT query.  missing VALUES or SELECT keyword")
}
