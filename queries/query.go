package queries

import (
	"context"
	"eurozulu/miniSQL/minisql"
)

type Query interface {
	Execute(ctx context.Context, db *minisql.MiniDB) (<-chan Result, error)
}
