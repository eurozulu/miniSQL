package db

import (
	"context"
	"fmt"
	"strconv"
)

type DeleteQuery struct {
	TableName string
	Where     Where
}

func (q DeleteQuery) Execute(ctx context.Context, db *TinyDB) (<-chan Result, error) {
	t, ok := db.tables[q.TableName]
	if !ok {
		return nil, fmt.Errorf("%q is not a known table", q.TableName)
	}
	ch := make(chan Result)
	go func(ch chan<- Result) {
		defer close(ch)
		keys := q.Where.keys(ctx, t)
		for k := range keys {
			t.Delete(k)

			select {
			case <-ctx.Done():
				return
			case ch <- &result{
				tableName: q.TableName,
				values:    Values{strconv.Itoa(int(k)): nil},
			}:
			}
		}
	}(ch)
	return ch, nil
}
