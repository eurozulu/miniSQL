package db

import (
	"context"
	"log"
	"strings"
)

const (
	NULL    = "NULL"
	NOTNULL = "NOTNULL"
)

const keyBuffer = 255

type Where map[string]string

func (w Where) keys(ctx context.Context, t *table) <-chan Key {
	ch := make(chan Key, keyBuffer)
	go func(ch chan<- Key) {
		defer close(ch)
		last := t.NextID()
		cols := w.ColumnNames()
		for k := Key(0); k < last; k++ {
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

func (w Where) ColumnNames() []string {
	cols := make([]string, len(w))
	var index int
	for k := range w {
		cols[index] = k
		index++
	}
	return cols
}

func (w Where) match(v Values) bool {
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
