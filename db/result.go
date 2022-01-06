package db

type Result interface {
	TableName() string
	Values() Values
}

type result struct {
	tableName string
	values    Values
}

func (r result) TableName() string {
	return r.tableName
}

func (r result) Values() Values {
	return r.values
}
