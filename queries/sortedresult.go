package queries

import (
	"context"
	"eurozulu/miniSQL/queries/whereclause"
	"eurozulu/miniSQL/stringutil"
	"fmt"
	"sort"
	"strings"
)

const DESC = "DESC"
const ASC = "ASC"

type sortedResult struct {
	Columns    []string
	Descending bool
}

func (sr sortedResult) Sort(ctx context.Context, results <-chan Result) <-chan Result {
	chOut := make(chan Result)
	go func(chIn <-chan Result, chOut chan<- Result) {
		defer close(chOut)
		for _, r := range sr.readAllResults(ctx, chIn) {
			select {
			case <-ctx.Done():
				return
			case chOut <- r:

			}
		}
	}(results, chOut)
	return chOut
}

func (sr sortedResult) readAllResults(ctx context.Context, results <-chan Result) []Result {
	var rs []Result
outerLoop:
	for {
		select {
		case <-ctx.Done():
			return nil
		case r, ok := <-results:
			if !ok {
				break outerLoop
			}
			rs = append(rs, r)
		}
	}
	sort.Slice(rs, func(i, j int) bool {
		return sr.sortResult(rs[i], rs[j])
	})
	return rs
}

func (sr sortedResult) sortResult(r1, r2 Result) bool {
	lt := whereclause.Operator("<")
	eq := whereclause.Operator("=")

	var v1 *string
	var v2 *string
	// find first column where values are not equal
	var index int
	for index < len(sr.Columns) {
		v1 = r1.Values()[sr.Columns[index]]
		v2 = r2.Values()[sr.Columns[index]]
		if !eq.Compare(v1, v2) {
			break
		}
		index++
	}
	if index >= len(sr.Columns) {
		// all values in all columns are equal
		return false
	}
	if sr.Descending {
		return lt.Compare(v2, v1)
	} else {
		return lt.Compare(v1, v2)
	}
}

func newSortedResult(q string) (*sortedResult, error) {
	if strings.HasPrefix(strings.ToUpper(q), "ORDER") {
		_, q = stringutil.FirstWord(q)
	}
	if strings.HasPrefix(strings.ToUpper(q), "BY") {
		_, q = stringutil.FirstWord(q)
	}
	before, l := stringutil.LastWord(q)
	desc := strings.EqualFold(l, DESC)
	if desc || strings.EqualFold(l, ASC) {
		q = before
	}
	cols := stringutil.SplitTrim(q, ",")
	if len(cols) == 0 || cols[0] == "" {
		return nil, fmt.Errorf("no column names found in sort")
	}
	return &sortedResult{
		Columns:    cols,
		Descending: desc,
	}, nil
}
