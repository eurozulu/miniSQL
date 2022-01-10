package queries

import (
	"context"
	"eurozulu/tinydb/tinydb"
	"fmt"
	"strconv"
)

type UpdateQuery struct {
	TableName string
	Values    tinydb.Values
	Where     WhereClause
}

func (q UpdateQuery) Execute(ctx context.Context, db *tinydb.TinyDB) (<-chan Result, error) {
	if !db.ContainsTable(q.TableName) {
		return nil, fmt.Errorf("%q is not a known table", q.TableName)
	}
	ch := make(chan Result)
	go func(q *UpdateQuery, ch chan<- Result) {
		defer close(ch)
		t, _ := db.Table(q.TableName)
		keys := q.Where.Keys(ctx, t)
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
	}(&q, ch)
	return ch, nil
}

func (q UpdateQuery) updateRow(k tinydb.Key, t tinydb.Table) Result {
	var v tinydb.Values
	err := t.Update(k, q.Values)
	if err != nil {
		errs := err.Error()
		v = tinydb.Values{"ERROR": &errs}
	} else {
		id := strconv.Itoa(int(k))
		v = tinydb.Values{"_id": &id}
	}
	return NewResult(q.TableName, v)
}
