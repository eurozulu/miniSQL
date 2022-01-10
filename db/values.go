package db

type Values map[string]*string

func (vs Values) ColumnNames() []string {
	names := make([]string, len(vs))
	var i int
	for c := range vs {
		names[i] = c
		i++
	}
	return names
}
