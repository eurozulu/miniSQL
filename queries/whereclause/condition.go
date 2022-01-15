package whereclause

import (
	"eurozulu/miniSQL/minisql"
	"eurozulu/miniSQL/stringutil"
	"fmt"
	"strings"
)

// condition is a single evaluation of a named column, an operator and a comparison value.
// e.g. mycol = 'hello world'  or _id > 234
type condition struct {
	Column   string
	Operator operator
	Value    *string
}

func (c condition) String() string {
	v := NULL
	if c.Value != nil {
		v = *c.Value
	}
	return fmt.Sprintf("%s %s %s", c.Column, c.Operator, v)
}

func (c condition) ColumnNames() []string {
	return []string{c.Column}
}

func (c condition) Compare(values minisql.Values) bool {
	v, ok := values[c.Column]
	if !ok {
		return false
	}
	return c.Operator.Compare(v, c.Value)
}

func ParseCondition(q string) (*condition, string, error) {
	col, op, rest := SplitOperator(q)
	if op == OP_UNKNOWN {
		return nil, q, fmt.Errorf("no operator found in condition %q", q)
	}
	if col == "" {
		return nil, q, fmt.Errorf("missing condition column name before '%s %s'", op, rest)
	}

	vals := stringutil.SplitIgnoreQuoted(rest, " ")
	val := stringutil.Unquote(vals[0])
	if val == "" {
		return nil, q, fmt.Errorf("missing condition value after '%s %s'  use 'NULL' to compare to empty value", col, op)
	}
	var vp *string
	if !strings.EqualFold(vals[0], NULL) {
		vp = &val
	}
	rest = strings.Join(vals[1:], " ")
	return &condition{
		Column:   col,
		Operator: op,
		Value:    vp,
	}, rest, nil
}
