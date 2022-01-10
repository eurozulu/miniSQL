package queries

import (
	"context"
	"eurozulu/tinydb/stringutil"
	"eurozulu/tinydb/tinydb"
	"fmt"
	"log"
	"strconv"
	"strings"
)

type InsertValuesQuery struct {
	TableName string
	Values    tinydb.Values
}

func (q InsertValuesQuery) Execute(ctx context.Context, db *tinydb.TinyDB) (<-chan Result, error) {
	iq := &InsertQuery{TableName: q.TableName, Values: make(chan tinydb.Values)}
	rCh, err := iq.Execute(ctx, db)
	if err != nil {
		return nil, err
	}
	go func(iq *InsertQuery, v tinydb.Values) {
		defer close(iq.Values)
		select {
		case <-ctx.Done():
			return
		case iq.Values <- v:
		}
	}(iq, q.Values)
	return rCh, nil
}

type InsertSelectQuery struct {
	TableName   string
	SelectQuery *SelectQuery
}

func (q InsertSelectQuery) Execute(ctx context.Context, db *tinydb.TinyDB) (<-chan Result, error) {
	src, err := q.SelectQuery.Execute(ctx, db)
	if err != nil {
		return nil, err
	}

	iq := &InsertQuery{TableName: q.TableName, Values: make(chan tinydb.Values)}
	rCh, err := iq.Execute(ctx, db)
	if err != nil {
		return nil, err
	}

	go func(iq *InsertQuery, src <-chan Result) {
		defer close(iq.Values)
		select {
		case <-ctx.Done():
			return
		case r, ok := <-src:
			if !ok {
				return
			}
			select {
			case <-ctx.Done():
				return
			case iq.Values <- r.Values():
			}
		}
	}(iq, src)
	return rCh, nil
}

type InsertQuery struct {
	TableName   string
	Values      chan tinydb.Values
	selectQuery string
}

func (q InsertQuery) Execute(ctx context.Context, db *tinydb.TinyDB) (<-chan Result, error) {
	if !db.ContainsTable(q.TableName) {
		return nil, fmt.Errorf("%s is not a known table", q.TableName)
	}

	ch := make(chan Result)
	go func(q *InsertQuery, vOut chan<- Result) {
		defer close(vOut)
		t, _ := db.Table(q.TableName)
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-q.Values:
				if !ok {
					return
				}

				id, err := t.Insert(v)
				if err != nil {
					log.Println(err)
					return
				}
				ids := strconv.Itoa(int(id))
				v["_id"] = &ids
				select {
				case <-ctx.Done():
					return
				case vOut <- NewResult(q.TableName, v):
				}
			}
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

func NewInsertQuery(q string) (Query, error) {
	if !strings.HasPrefix(strings.ToUpper(q), "INTO") {
		return nil, fmt.Errorf("missing INTO in query")
	}
	qs := strings.SplitN(strings.TrimSpace(q[4:]), " ", 2)
	if len(qs) < 2 {
		return nil, fmt.Errorf("invalid INSERT.  No Values or Select")
	}
	tn := qs[0]
	q, cols, err := stringutil.ParseList(qs[1])
	if err != nil {
		return nil, fmt.Errorf("invalid columns %s", err)
	}

	if strings.HasPrefix(strings.ToUpper(q), "VALUES") {
		_, vals, err := stringutil.ParseList(strings.TrimSpace(q[6:]))
		vs, err := valuesList(cols, vals)
		if err != nil {
			return nil, err
		}
		return &InsertValuesQuery{
			TableName: tn,
			Values:    vs,
		}, nil
	}
	if strings.HasPrefix(strings.ToUpper(q), "SELECT") {
		sq, err := NewSelectQuery(strings.TrimSpace(q[6:]))
		if err != nil {
			return nil, err
		}
		return &InsertSelectQuery{
			TableName:   tn,
			SelectQuery: sq,
		}, nil

	}
	return nil, fmt.Errorf("invalid INSERT.  No Values or Select")
}
