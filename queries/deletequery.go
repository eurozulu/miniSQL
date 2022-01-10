package queries

import (
	"context"
	"eurozulu/tinydb/tinydb"
	"fmt"
	"strconv"
	"strings"
)

type DeleteQuery struct {
	TableName string
	Where     WhereClause
}

func (q DeleteQuery) Execute(ctx context.Context, db *tinydb.TinyDB) (<-chan Result, error) {
	if !db.ContainsTable(q.TableName) {
		return nil, fmt.Errorf("%q is not a known table", q.TableName)
	}
	ch := make(chan Result)
	go func(q *DeleteQuery, ch chan<- Result) {
		defer close(ch)
		t, _ := db.Table(q.TableName)
		var keys []tinydb.Key
		for k := range q.Where.Keys(ctx, t) {
			keys = append(keys, k)
		}
		keys = t.Delete(keys...)
		ks := strconv.Itoa(len(keys))
		select {
		case <-ctx.Done():
			return
		case ch <- NewResult(q.TableName, tinydb.Values{"deleted": &ks}):
		}
	}(&q, ch)
	return ch, nil
}

func NewDeleteQuery(q string) (*DeleteQuery, error) {
	if !strings.HasPrefix(strings.ToUpper(q), "FROM") {
		return nil, fmt.Errorf("missing FROM in query")
	}
	qs := strings.SplitN(strings.TrimSpace(q[4:]), " ", 2)
	var wh WhereClause
	if len(qs) > 1 {
		w, err := NewWhere(strings.Join(qs[1:], " "))
		if err != nil {
			return nil, err
		}
		wh = w
	}
	return &DeleteQuery{
		TableName: qs[0],
		Where:     wh,
	}, nil
}
