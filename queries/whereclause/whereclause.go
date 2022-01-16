package whereclause

import (
	"context"
	"eurozulu/miniSQL/minisql"
	"eurozulu/miniSQL/stringutil"
	"fmt"
	"log"
	"strings"
)

const (
	NULL    = "NULL"
	NOTNULL = "NOTNULL"
)

const keyBuffer = 255

// WhereClause is an optional filter to limit the results of a query.
// Clause consists of at least one 'condition', a named column and a value, seperated with an Operator.
// e.g. mycolumn != 'some value'
// additional conditions may be added using the operators AND or OR.
// e.g. mycolumn != 'some value' AND _id <= 22
type WhereClause interface {
	// Keys returns a channel of all the keys in the given table, which match the where clause.
	Keys(ctx context.Context, t minisql.Table) <-chan minisql.Key
}

type whereClause struct {
	expression Expression
}

func (wc whereClause) Keys(ctx context.Context, t minisql.Table) <-chan minisql.Key {
	ch := make(chan minisql.Key, keyBuffer)
	go func(ch chan<- minisql.Key) {
		defer close(ch)
		last := t.NextID()
		var cols []string
		if wc.HasExpression() {
			cols = wc.expression.ColumnNames()
		}
		for k := minisql.Key(0); k < last; k++ {
			if !t.ContainsID(k) {
				continue
			}
			// If expression present, collect values for key and compare with expression
			if len(cols) > 0 {
				v, err := t.Select(k, cols)
				if err != nil {
					log.Println(err)
					return
				}
				if !wc.expression.Compare(v) {
					continue
				}
			}
			select {
			case <-ctx.Done():
				return
			case ch <- k:
			}
		}
	}(ch)
	return ch
}

func (wc whereClause) HasExpression() bool {
	return wc.expression != nil
}

//  NewWhere creates a new Where clause
// query is optional, when provided, can, optionally begin with the preceeding keyword "WHERE", begining with an expression
// expressions can be conditions (x = y), or complex (bracketed) conditions e.g. '(a = true AND b=true) OR c=false'
// conditions may be linked using the AND, OR or inverted with the NOT keyword.
// use the NULL keyword to search for nil values (or NOT NULL for non nil)
// If query is empty, where will generate aLL keys in the given table
func NewWhere(q string) (WhereClause, error) {
	w := &whereClause{}
	if q != "" {
		// trim off WHERE, if present
		ws, rest := stringutil.FirstWord(q)
		if strings.EqualFold(ws, "WHERE") {
			q = rest
		}
		ex, err := ParseExpression(q)
		if err != nil {
			return nil, fmt.Errorf("invalid WHERE  %w", err)
		}
		w.expression = ex
	}
	return w, nil
}
