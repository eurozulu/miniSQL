package queries

import "eurozulu/tinydb/tinydb"

type Result interface {
	TableName() string
	Values() tinydb.Values
}

type result struct {
	tableName string
	values    tinydb.Values
}

func (r result) TableName() string {
	return r.tableName
}

func (r result) Values() tinydb.Values {
	return r.values
}

func NewResult(tableName string, values tinydb.Values) Result {
	return &result{
		tableName: tableName,
		values:    values,
	}
}
