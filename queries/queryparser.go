package queries

import (
	"fmt"
	"strings"
)

func ParseQuery(q string) (Query, error) {
	cmd := strings.SplitN(q, " ", 2)
	if len(cmd) != 2 {
		return nil, fmt.Errorf("invalid query, missing column names")
	}
	q = strings.Join(cmd[1:], " ")
	switch strings.ToUpper(cmd[0]) {
	case "SELECT":
		return NewSelectQuery(q)
	case "INSERT":
		return NewInsertQuery(q)
	case "UPDATE":
		return NewUpdateQuery(q)
	case "DELETE":
		return NewDeleteQuery(q)

	default:
		return nil, fmt.Errorf("unrecognised query")
	}
}
