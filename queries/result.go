package queries

import "eurozulu/miniSQL/minisql"

type Result interface {
	TableName() string
	Values() minisql.Values
}

type result struct {
	tableName string
	values    minisql.Values
}

func (r result) TableName() string {
	return r.tableName
}

func (r result) Values() minisql.Values {
	return r.values
}

func NewResult(tableName string, values minisql.Values) Result {
	return &result{
		tableName: tableName,
		values:    values,
	}
}
