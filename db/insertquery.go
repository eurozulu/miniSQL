package db

import (
	"context"
	"fmt"
	"log"
	"strconv"
)

type InsertValuesQuery struct {
	TableName string
	Values    Values
}

func (q InsertValuesQuery) Execute(ctx context.Context, db *TinyDB) (<-chan Result, error) {
	iq := &InsertQuery{TableName: q.TableName, Values: make(chan Values)}
	rCh, err := iq.Execute(ctx, db)
	if err != nil {
		return nil, err
	}
	go func(iq *InsertQuery, v Values) {
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

func (q InsertSelectQuery) Execute(ctx context.Context, db *TinyDB) (<-chan Result, error) {
	src, err := q.SelectQuery.Execute(ctx, db)
	if err != nil {
		return nil, err
	}

	iq := &InsertQuery{TableName: q.TableName, Values: make(chan Values)}
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
	Values      chan Values
	selectQuery string
}

func (q InsertQuery) Execute(ctx context.Context, db *TinyDB) (<-chan Result, error) {
	_, ok := db.tables[q.TableName]
	if !ok {
		return nil, fmt.Errorf("%s is not a known table", q.TableName)
	}

	ch := make(chan Result)
	go func(q *InsertQuery, vOut chan<- Result) {
		defer close(vOut)
		t := db.tables[q.TableName]
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
				case vOut <- &result{
					tableName: q.TableName,
					values:    v,
				}:
				}
			}
		}
	}(&q, ch)
	return ch, nil
}
