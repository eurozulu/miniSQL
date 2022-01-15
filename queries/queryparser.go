package queries

import (
	"eurozulu/miniSQL/stringutil"
	"fmt"
	"strings"
)

func ParseQuery(q string) (Query, error) {
	cmd, rest := stringutil.FirstWord(q)
	if cmd == "" {
		return nil, fmt.Errorf("invalid query, missing query type SELECT, INSERT, DELETE or UPDATE")
	}
	if rest == "" {
		return nil, fmt.Errorf("invalid query, missing values after %s", cmd)
	}
	switch strings.ToUpper(cmd) {
	case "SELECT":
		return NewSelectQuery(rest)
	case "INSERT":
		return NewInsertQuery(rest)
	case "UPDATE":
		return NewUpdateQuery(rest)
	case "DELETE":
		return NewDeleteQuery(rest)

	default:
		return nil, fmt.Errorf("unrecognised query")
	}
}
