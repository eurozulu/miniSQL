package db

import (
	"context"
	"fmt"
	"log"
	"strconv"
)

type Query interface {
	Execute(ctx context.Context, db TinyDB) (<-chan Result, error)
}

type SelectQuery struct {
	TableName string
	Columns   []string
	Where     Where
}

func (q SelectQuery) Execute(ctx context.Context, db TinyDB) (<-chan Result, error) {
	t, ok := db.tables[q.TableName]
	if !ok {
		return nil, fmt.Errorf("%s is not a known table", q.TableName)
	}
	cols, err := expandColumnNames(q.Columns, t.ColumnNames())
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

type InsertValuesQuery struct {
	TableName string
	Values    Values
}

func (q InsertValuesQuery) Execute(ctx context.Context, db TinyDB) (Result, error) {
	t, ok := db.tables[q.TableName]
	if !ok {
		return nil, fmt.Errorf("%s table is not known", q.TableName)
	}
	id, err := t.Insert(q.Values)
	if err != nil {
		return nil, err
	}
	ids := strconv.Itoa(int(id))
	q.Values["_id"] = &ids
	return &result{
		tableName: q.TableName,
		values:    q.Values,
	}, nil
}

type InsertQuery struct {
	TableName string
	Select    *SelectQuery
}

func (q InsertQuery) Execute(ctx context.Context, db TinyDB) (<-chan Result, error) {
	_, ok := db.tables[q.TableName]
	if !ok {
		return nil, fmt.Errorf("%s is not a known table", q.TableName)
	}
	vals, err := q.Select.Execute(ctx, db)
	if err != nil {
		return nil, err
	}

	ch := make(chan Result)
	go func(vIn <-chan Result, vOut chan<- Result) {
		defer close(vOut)
		vq := &InsertValuesQuery{TableName: q.TableName,}
		for {
			select {
			case <-ctx.Done():
				return
			case r, ok := <-vIn:
				if !ok {
					return
				}

				vq.Values = r.Values()
				rr, err := vq.Execute(ctx, db)
				if err != nil {
					log.Println(err)
					return
				}

				select {
				case <-ctx.Done():
					return
				case vOut <- rr:
				}
			}
		}
	}(vals, ch)
	return ch, nil
}

type DeleteQuery struct {
	TableName string
	Where     Where
}

func (q DeleteQuery) Execute(ctx context.Context, db TinyDB) (<-chan Result, error) {
	panic("not implemented")
}

// expandColumnNames expands the given list of column names and validates the given list as known names.
// columns may contain "*" wild card to indicate all column names.
func expandColumnNames(sel []string, columns []string) ([]string, error) {
	if len(sel) == 0 {
		return columns, nil
	}
	var cols []string
	for _, c := range sel {
		if c == "*" {
			cols = append(cols, columns...)
		} else {
			if containsString(c, columns) < 0 {
				return nil, fmt.Errorf("%s is an unknown column", c)
			}
			cols = append(cols, c)
		}
	}
	return cols, nil
}
