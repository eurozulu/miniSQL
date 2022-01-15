package minisql

import (
	"bytes"
)

type Values map[string]*string

func (v Values) String() string {
	buf := bytes.NewBuffer(nil)
	for k, val := range v {
		var vs string
		if val == nil {
			vs = "NULL"
		} else {
			vs = *val
		}
		if buf.Len() > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(k)
		buf.WriteString(" = ")
		buf.WriteString(vs)
	}
	return buf.String()
}
