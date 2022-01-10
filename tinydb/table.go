package tinydb

import (
	"encoding/json"
	"fmt"
	"strconv"
)

type Table interface {
	ColumnNames() []string
	AlterColumns(cols map[string]bool)
	ContainsID(k Key) bool
	NextID() Key

	Select(id Key, columns []string) (Values, error)
	Insert(values Values) (Key, error)
	Update(id Key, values Values) error
	Delete(id ...Key) []Key
}

type table struct {
	keys    keyColumn
	columns map[string]column
}

func (tb table) ColumnNames() []string {
	cns := make([]string, len(tb.columns)+1)
	cns[0] = "_id"
	var i int
	for cn := range tb.columns {
		i++
		cns[i] = cn
	}
	return cns
}

func (tb table) ContainsID(k Key) bool {
	return tb.keys[k]
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
		var v *string
		if c == "_id" {
			s := strconv.Itoa(int(id))
			v = &s
		} else {
			col, ok := tb.columns[c]
			if !ok {
				return nil, fmt.Errorf("%s is not a known column", c)
			}
			cv, ok := col[id]
			if ok {
				v = &cv
			}
		}
		vals[c] = v
	}
	return vals, nil
}

func (tb table) SelectValues(id Key, columns []string) ([]*string, error) {
	svals, err := tb.Select(id, columns)
	if err != nil {
		return nil, err
	}
	vals := make([]*string, len(columns))
	var index int
	for _, c := range columns {
		vals[index] = svals[c]
		index++
	}
	return vals, nil
}

func (tb table) Update(id Key, values Values) error {
	if !tb.ContainsID(id) {
		return fmt.Errorf("%d is not a known _id", id)
	}

	for k, v := range values {
		c, ok := tb.columns[k]
		if !ok {
			return fmt.Errorf("%s column not known", k)
		}
		if v != nil {
			c.Update(id, *v)
		} else {
			_ = c.Delete(id)
		}
	}
	return nil
}

func (tb table) Delete(id ...Key) []Key {
	var dks []Key
	for _, k := range id {
		if tb.keys[k] {
			tb.keys[k] = false
			dks = append(dks, k)
		}
		for _, col := range tb.columns {
			delete(col, k)
		}
	}
	return dks
}

func (tb table) Insert(values Values) (Key, error) {
	id := tb.NextID()
	for k, v := range values {
		c, ok := tb.columns[k]
		if !ok {
			return -1, fmt.Errorf("%s column not known", k)
		}
		if v != nil {
			if err := c.Insert(id, *v); err != nil {
				return -1, err
			}
		} else {
			_ = c.Delete(id)
		}
	}
	tb.keys[id] = true
	return id, nil
}

func (tb table) MarshalJSON() ([]byte, error) {
	s := &struct {
		Keys    keyColumn         `json:"Keys"`
		Columns map[string]column `json:"columns"`
	}{
		Keys:    tb.keys,
		Columns: tb.columns,
	}
	return json.Marshal(s)
}

func (tb *table) UnmarshalJSON(bytes []byte) error {
	s := &struct {
		Keys    keyColumn         `json:"Keys"`
		Columns map[string]column `json:"columns"`
	}{}
	if err := json.Unmarshal(bytes, s); err != nil {
		return err
	}
	tb.keys = s.Keys
	tb.columns = s.Columns
	return nil
}

func newTable(columns map[string]bool) Table {
	t := &table{
		keys:    keyColumn{},
		columns: map[string]column{},
	}
	if len(columns) > 0 {
		t.AlterColumns(columns)
	}
	return t
}
