package queries

import (
	"context"
	"eurozulu/tinydb/tinydb"
	"log"
	"strings"
)

const (
	NULL    = "NULL"
	NOTNULL = "NOTNULL"
)

const keyBuffer = 255

type WhereClause map[string]string

func (w WhereClause) Keys(ctx context.Context, t tinydb.Table) <-chan tinydb.Key {
	ch := make(chan tinydb.Key, keyBuffer)
	go func(ch chan<- tinydb.Key) {
		defer close(ch)
		last := t.NextID()
		cols := w.ColumnNames()
		for k := tinydb.Key(0); k < last; k++ {
			if !t.ContainsID(k) {
				continue
			}
			if len(cols) > 0 {
				v, err := t.Select(k, cols)
				if err != nil {
					log.Println(err)
					return
				}
				if !w.match(v) {
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

func (w WhereClause) ColumnNames() []string {
	cols := make([]string, len(w))
	var index int
	for k := range w {
		cols[index] = k
		index++
	}
	return cols
}

func (w WhereClause) match(v tinydb.Values) bool {
	for wk, wv := range w {
		vv, ok := v[wk]
		if !ok {
			return false
		}
		if vv == nil {
			if wv != NULL {
				return false
			}
			continue
		}
		if wv == NOTNULL {
			continue
		}
		if !strings.EqualFold(*vv, wv) {
			return false
		}
	}
	return true
}
