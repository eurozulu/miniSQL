package queryparse

import (
	"fmt"
	"strings"
	"tinydb/db"
)

type QueryParser struct {
}

func (qp QueryParser) Parse(q string) (db.Query, error) {
	cmd := strings.SplitN(q, " ", 2)
	if len(cmd) != 2 {
		return nil, fmt.Errorf("invalid query")
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
	cols := strings.Split(q[:fi], ",")
	q = strings.TrimSpace(q[fi+4:])
	cmd := strings.SplitN(q, " ", 2)
	w, err := qp.parseWhere(cmd[1])
	if err != nil {
		return nil, err
	}
	return &db.SelectQuery{
		TableName: cmd[0],
		Columns:   cols,
		Where:     w,
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
	cmd := strings.SplitN(strings.TrimSpace(qs[1]), " ", 2)
	switch strings.ToUpper(cmd[0]) {
	case "VALUES":
		vs, err := qp.parseValue(cmd[1])
		if err != nil {
			return nil, err
		}
		return &db.InsertValuesQuery{
			TableName: tn,
			Values:    vs,
		}, nil
	case "SELECT":
		sq, err := qp.parseSelect(cmd[1])
		if err != nil {
			return nil, err
		}
		return &db.InsertQuery{
			TableName: tn,
			Select:    sq,
		}, nil
	default:
		return nil, fmt.Errorf("invalid INSERT.  No Values or Select")
	}
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

func (qp QueryParser) parseValue(q string) (db.Values, error) {
	q = strings.TrimLeft(q, "(")
	q = strings.TrimRight(q, ")")
	q = strings.TrimSpace(q)
	qs := strings.Split(q, ",")
	v := db.Values{}
	for _, qv := range qs {
		qvs := strings.SplitN(qv, "=", 2)
		if len(qvs) != 2 {
			return nil, fmt.Errorf("invalud value, no equals found %s", qv)
		}
		val := strings.TrimSpace(qvs[1])
		var vp *string
		if val != "NULL" {
			vp = &val
		}
		v[strings.TrimSpace(qvs[0])] = vp
	}
	return v, nil
}
