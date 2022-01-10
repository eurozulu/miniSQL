package db

import (
	"context"
	"fmt"
	"log"
)

type Query interface {
	Execute(ctx context.Context, db *TinyDB) (<-chan Result, error)
}

type SelectQuery struct {
	TableName string
	Columns   []string
	Where     WhereClause
}

func (q SelectQuery) Execute(ctx context.Context, db *TinyDB) (<-chan Result, error) {
	t, ok := db.tables[q.TableName]
	if !ok {
		return nil, fmt.Errorf("%q is not a known table", q.TableName)
	}
	cols, err := expandColumnNames(t, q.Columns)
	if err != nil {
		return nil, fmt.Errorf("%w in table %s", err, q.TableName)
	}
	q.Columns = cols

	ch := make(chan Result)
	go func(sq SelectQuery, results chan<- Result) {
		defer close(results)
		t := db.tables[sq.TableName]
		keys := sq.Where.keys(ctx, t)
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
				case results <- &result{
					tableName: q.TableName,
					values:    v,
				}:
				}
			}
		}
	}(q, ch)
	return ch, nil
}

// expandColumnNames expands the given list of column names and validates the given list as known names.
// columns may contain "*" wild card to indicate all column names.
func expandColumnNames(t Table, columns []string) ([]string, error) {
	tcols := t.ColumnNames()
	if len(columns) == 0 {
		return tcols, nil
	}
	var cols []string
	for _, c := range columns {
		if c == "*" {
			cols = append(cols, tcols...)
		} else {
			if containsString(c, tcols) < 0 {
				return nil, fmt.Errorf("%s is an unknown column", c)
			}
			cols = append(cols, c)
		}
	}
	return cols, nil
}
