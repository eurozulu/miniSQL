package db

import (
	"fmt"
)

type Values map[string]*string

type table struct {
	keys    keyColumn
	columns map[string]column
}

func (vs Values) ColumnNames() []string {
	names := make([]string, len(vs))
	var i int
	for c := range vs {
		names[i] = c
		i++
	}
	return names
}

func (tb table) ColumnNames() []string {
	cns := make([]string, len(tb.columns))
	var i int
	for cn := range tb.columns {
		cns[i] = cn
		i++
	}
	return cns
}

func (tb table) ContainsID(k Key) bool {
	_, ok := tb.keys[k]
	return ok
}

func (tb table) NextID() Key {
	nk := Key(0)
	for k := range tb.keys {
		if k >= nk {
			nk = k + 1
		}
	}
	return nk
}

func (tb *table) AlterColumns(cols map[string]bool) {
	for n, ok := range cols {
		if !ok {
			delete(tb.columns, n)
			continue
		}
		if _, ok := tb.columns[n]; !ok {
			tb.columns[n] = column{}
		}
	}
}

func (tb table) Select(id Key, columns []string) (Values, error) {
	vals := Values{}
	for _, c := range columns {
		col, ok := tb.columns[c]
		if !ok {
			return nil, fmt.Errorf("%s is not a known column", c)
		}
		var v *string
		cv, ok := col[id]
		if ok {
			v = &cv
		}
		vals[c] = v
	}
	return vals, nil
}

func (tb table) Update(id Key, values Values) error {
	if !tb.ContainsID(id) {
		return fmt.Errorf("%d is not a known _id", id)
	}
	return tb.updateRow(id, values)
}

func (tb table) Delete(id ...Key) {
	for _, k := range id {
		if tb.keys[k] {
			tb.keys[k] = false
		}
		for _, col := range tb.columns {
			delete(col, k)
		}
	}
}

func (tb table) Insert(values Values) (Key, error) {
	id := tb.NextID()
	if err := tb.updateRow(id, values); err != nil {
		return -1, err
	}
	tb.keys[id] = true
	return id, nil
}

func (tb table) updateRow(id Key, values Values) error {
	for k, v := range values {
		c, ok := tb.columns[k]
		if !ok {
			return fmt.Errorf("%s column not known", k)
		}
		if v != nil {
			if err := c.Insert(id, *v); err != nil {
				return err
			}
		} else {
			_ = c.Delete(id)
		}
	}
	return nil
}

func containsString(s string, ss []string) int {
	for i, sz := range ss {
		if sz == s {
			return i
		}
	}
	return -1
}

func NewTable(columns map[string]bool) *table {
	t := &table{
		keys:    keyColumn{},
		columns: map[string]column{},
	}
	if len(columns) > 0 {
		t.AlterColumns(columns)
	}
	return t
}
