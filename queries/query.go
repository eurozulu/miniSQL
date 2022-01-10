package queries

import (
	"context"
	"eurozulu/tinydb/tinydb"
)

type Query interface {
	Execute(ctx context.Context, db *tinydb.TinyDB) (<-chan Result, error)
}
