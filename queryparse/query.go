package queryparse

import (
	"context"
	"eurozulu/tinydb/queries"
	"eurozulu/tinydb/tinydb"
)

type Query interface {
	Execute(ctx context.Context, db *tinydb.TinyDB) (<-chan queries.Result, error)
}
