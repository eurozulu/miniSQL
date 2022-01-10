package queries

import (
	"context"
	"eurozulu/tinydb/tinydb"
	"fmt"
	"log"
	"strconv"
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
