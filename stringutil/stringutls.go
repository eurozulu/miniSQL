package stringutil

import (
	"fmt"
	"strings"
)

func ContainsString(s string, ss []string) int {
	for i, sz := range ss {
		if sz == s {
			return i
		}
	}
	return -1
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
		c = strings.Trim(strings.TrimSpace(c), "\"")
		c = strings.Trim(c, "'")
		cols[i] = c
	}
	return q, cols, nil
}
