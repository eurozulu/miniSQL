package queries

import (
	"context"
	"eurozulu/tinydb/queries/whereclause"
	"eurozulu/tinydb/stringutil"
	"eurozulu/tinydb/tinydb"
	"fmt"
	"strconv"
	"strings"
)

type DeleteQuery struct {
	TableName string
	Where     whereclause.WhereClause
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

// NewDeleteQuery creates a new delete query from the given string
// Query should be a valid delete without the preceeding DELETE.
// i.e it should begin with the keyword FROM.
// e.g. "FROM mytable WHERE _id=2"
func NewDeleteQuery(q string) (*DeleteQuery, error) {
	if !strings.HasPrefix(strings.ToUpper(q), "FROM ") {
		return nil, fmt.Errorf("missing FROM in query")
	}
	// strip leading FROM
	_, q = stringutil.FirstWord(q)
	table, rest := stringutil.FirstWord(q)
	if table == "" {
		return nil, fmt.Errorf("missing table name for delete")
	}

	var wh whereclause.WhereClause
	if len(rest) > 1 {
		if !strings.HasPrefix(strings.ToUpper(q), "WHERE") {
			return nil, fmt.Errorf("%s is not a recognised WHERE", q)
		}
		// strip leading 'WHERE'
		_, rest = stringutil.FirstWord(rest)
		w, err := whereclause.NewWhere(rest)
		if err != nil {
			return nil, err
		}
		wh = w
	}
	return &DeleteQuery{
		TableName: table,
		Where:     wh,
	}, nil
}
