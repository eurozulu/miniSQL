package queries

import (
	"context"
	"eurozulu/tinydb/tinydb"
	"fmt"
	"strconv"
	"strings"
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

func NewUpdateQuery(q string) (*UpdateQuery, error) {
	si := strings.Index(strings.ToUpper(q), "SET")
	if si < 2 {
		return nil, fmt.Errorf("missing SET command")
	}
	tn := strings.TrimSpace(q[:si])
	if tn == "" {
		return nil, fmt.Errorf("missing table name")
	}

	var where WhereClause
	wi := strings.Index(strings.ToUpper(q), "WHERE")
	if wi >= 0 {
		w, err := NewWhere(q[wi:])
		if err != nil {
			return nil, err
		}
		where = w
		q = q[:wi]
	}
	vals := tinydb.Values{}
	sets := strings.Split(strings.TrimSpace(q[si+len("SET"):]), ",")
	for _, s := range sets {
		ss := strings.SplitN(s, "=", 2)
		if len(ss) != 2 {
			return nil, fmt.Errorf("missing value for %s", s)
		}
		var v *string
		ss[1] = strings.TrimSpace(ss[1])
		if ss[1] != NULL {
			v = &ss[1]
		}
		vals[strings.TrimSpace(ss[0])] = v
	}
	return &UpdateQuery{
		TableName: tn,
		Values:    vals,
		Where:     where,
	}, nil
}
