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

type UpdateQuery struct {
	TableName string
	Values    tinydb.Values
	Where     whereclause.WhereClause
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

// NewUpdateQuery creates a new update query from the given string
// Query should be a valid update without the preceeding UPDATE.
// i.e it should begin with the table name.
// e.g. "mytable SET col1=bla, col3=haha WHERE col2=hoho"
func NewUpdateQuery(q string) (*UpdateQuery, error) {
	table, rest := stringutil.FirstWord(q)
	if table == "" {
		return nil, fmt.Errorf("missing table name")
	}

	if !strings.HasPrefix(strings.ToUpper(rest), "SET") {
		return nil, fmt.Errorf("missing SET command")
	}
	_, rest = stringutil.FirstWord(rest)

	var where whereclause.WhereClause
	wi := strings.Index(strings.ToUpper(rest), "WHERE")
	if wi >= 0 {
		w, err := whereclause.NewWhere(rest[wi:])
		if err != nil {
			return nil, err
		}
		where = w
		rest = rest[:wi]
	}
	vals := tinydb.Values{}
	sets := strings.Split(rest, ",")
	for _, s := range sets {
		ss := strings.SplitN(s, "=", 2)
		if len(ss) != 2 {
			return nil, fmt.Errorf("missing value for %s", s)
		}
		col := strings.TrimSpace(ss[0])
		if col == "" {
			return nil, fmt.Errorf("missing column name before =")
		}
		val := strings.TrimSpace(ss[1])
		if val == "" {
			return nil, fmt.Errorf("missing value after =")
		}
		var v *string
		if val != whereclause.NULL {
			v = &ss[1]
		}
		vals[col] = v
	}
	return &UpdateQuery{
		TableName: table,
		Values:    vals,
		Where:     where,
	}, nil
}
