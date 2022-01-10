package db

import (
	"context"
	"fmt"
	"strconv"
)

type UpdateQuery struct {
	TableName string
	Values    Values
	Where     WhereClause
}

func (q UpdateQuery) Execute(ctx context.Context, db *TinyDB) (<-chan Result, error) {
	t, ok := db.tables[q.TableName]
	if !ok {
		return nil, fmt.Errorf("%q is not a known table", q.TableName)
	}
	ch := make(chan Result)
	go func(ch chan<- Result) {
		defer close(ch)
		keys := q.Where.keys(ctx, t)
		for {
			select {
			case <-ctx.Done():
				return
			case k, ok := <-keys:
				if !ok {
					return
				}
				r := q.updateRow(k, t)
				select {
				case <-ctx.Done():
					return
				case ch <- r:
				}
			}
		}
	}(ch)
	return ch, nil
}

func (q UpdateQuery) updateRow(k Key, t Table) Result {
	r := &result{tableName: q.TableName}
	err := t.Update(k, q.Values)
	if err != nil {
		errs := err.Error()
		r.values = Values{"ERROR": &errs}
	} else {
		id := strconv.Itoa(int(k))
		r.values = Values{"_id": &id}
	}
	return r
}
