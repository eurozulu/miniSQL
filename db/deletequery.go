package db

import (
	"context"
	"fmt"
	"strconv"
)

type DeleteQuery struct {
	TableName string
	Where     WhereClause
}

func (q DeleteQuery) Execute(ctx context.Context, db *TinyDB) (<-chan Result, error) {
	t, ok := db.tables[q.TableName]
	if !ok {
		return nil, fmt.Errorf("%q is not a known table", q.TableName)
	}
	ch := make(chan Result)
	go func(ch chan<- Result) {
		defer close(ch)
		var keys []Key
		for k := range q.Where.keys(ctx, t) {
			keys = append(keys, k)
		}
		keys = t.Delete(keys...)
		ks := strconv.Itoa(len(keys))
		select {
		case <-ctx.Done():
			return
		case ch <- &result{
			tableName: q.TableName,
			values:    Values{"deleted": &ks},
		}:
		}
	}(ch)
	return ch, nil
}
