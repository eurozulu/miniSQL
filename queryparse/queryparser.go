package queryparse

import (
	"eurozulu/tinydb/db"
	"fmt"
	"strings"
)

type QueryParser struct {
}

func (qp QueryParser) Parse(q string) (db.Query, error) {
	cmd := strings.SplitN(q, " ", 2)
	if len(cmd) != 2 {
		return nil, fmt.Errorf("invalid query, missing column names")
	}
	switch strings.ToUpper(cmd[0]) {
	case "SELECT":
		return qp.parseSelect(cmd[1])
	case "INSERT":
		return qp.parseInsert(cmd[1])
	case "DELETE":
		return qp.parseDelete(cmd[1])

	default:
		return nil, fmt.Errorf("unrecognised query")
	}
}

func (qp QueryParser) parseSelect(q string) (*db.SelectQuery, error) {
	fi := strings.Index(strings.ToUpper(q), "FROM")
	if fi < 0 {
		return nil, fmt.Errorf("missing FROM in query")
	}
	cols := strings.Split(strings.TrimSpace(q[:fi]), ",")
	q = strings.TrimSpace(q[fi+4:])
	cmd := strings.SplitN(q, " ", 2)
	var where db.Where
	if len(cmd) > 1 {
		w, err := qp.parseWhere(cmd[1])
		if err != nil {
			return nil, err
		}
		where = w
	}
	return &db.SelectQuery{
		TableName: cmd[0],
		Columns:   cols,
		Where:     where,
	}, nil
}

func (qp QueryParser) parseInsert(q string) (db.Query, error) {
	if !strings.HasPrefix(strings.ToUpper(q), "INTO") {
		return nil, fmt.Errorf("missing INTO in query")
	}
	qs := strings.SplitN(strings.TrimSpace(q[4:]), " ", 2)
	if len(qs) < 2 {
		return nil, fmt.Errorf("invalid INSERT.  No Values or Select")
	}
	tn := qs[0]
	q, cols, err := ParseList(qs[1])
	if err != nil {
		return nil, fmt.Errorf("invalid columns %s", err)
	}

	if strings.HasPrefix(strings.ToUpper(q), "VALUES") {
		_, vals, err := ParseList(strings.TrimSpace(q[6:]))
		vs, err := valuesList(cols, vals)
		if err != nil {
			return nil, err
		}
		return &db.InsertValuesQuery{
			TableName: tn,
			Values:    vs,
		}, nil
	}
	if strings.HasPrefix(strings.ToUpper(q), "SELECT") {
		sq, err := qp.parseSelect(strings.TrimSpace(q[6:]))
		if err != nil {
			return nil, err
		}
		return &db.InsertSelectQuery{
			TableName:   tn,
			SelectQuery: sq,
		}, nil

	}
	return nil, fmt.Errorf("invalid INSERT.  No Values or Select")
}

func (qp QueryParser) parseDelete(q string) (*db.DeleteQuery, error) {
	if !strings.HasPrefix(strings.ToUpper(q), "FROM") {
		return nil, fmt.Errorf("missing FROM in query")
	}
	qs := strings.SplitN(strings.TrimSpace(q[4:]), " ", 2)
	var wh db.Where
	if len(qs) > 1 {
		w, err := qp.parseWhere(qs[1])
		if err != nil {
			return nil, err
		}
		wh = w
	}
	return &db.DeleteQuery{
		TableName: qs[0],
		Where:     wh,
	}, nil
}

func (qp QueryParser) parseWhere(q string) (db.Where, error) {
	if q == "" {
		return nil, nil
	}
	if !strings.HasPrefix(strings.ToUpper(q), "WHERE") {
		return nil, fmt.Errorf("%s is not a recognised WHERE", q)
	}
	q = strings.TrimSpace(q[5:])
	ws := strings.Split(q, "AND")
	wh := db.Where{}
	for _, w := range ws {
		v := strings.SplitN(w, "=", 2)
		if len(v) != 2 {
			return nil, fmt.Errorf("%s is not a valid WHERE", w)
		}
		wh[strings.TrimSpace(v[0])] = strings.TrimSpace(v[1])
	}
	return wh, nil
}

func valuesList(keys []string, vals []string) (db.Values, error) {
	if len(keys) != len(vals) {
		return nil, fmt.Errorf("columns / values count mismatch")
	}
	vm := db.Values{}
	for i, k := range keys {
		vs := strings.Trim(vals[i], "'")
		vm[k] = &vs
	}
	return vm, nil
}

func ParseList(q string) (string, []string, error) {
	if !strings.HasPrefix(q, "(") {
		return "", nil, fmt.Errorf("expected '(' not found")
	}
	i := strings.Index(q, ")")
	if i < 0 {
		return "", nil, fmt.Errorf("expected ')' not found")
	}
	ls := strings.TrimRight(q[:i], ")")
	ls = strings.TrimLeft(ls, "(")
	if i+1 < len(q) {
		q = strings.TrimSpace(q[i+1:])
	} else {
		q = ""
	}
	cols := strings.Split(ls, ",")
	for i, c := range cols {
		cols[i] = strings.TrimSpace(c)
	}
	return q, cols, nil
}
