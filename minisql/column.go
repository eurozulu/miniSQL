package minisql

import (
	"fmt"
	"strconv"
	"strings"
)

type column map[Key]string

type keyColumn map[Key]bool

func (pc column) Find(value string) []Key {
	var keys []Key
	for k, v := range pc {
		if !strings.EqualFold(v, value) {
			continue
		}
		keys = append(keys, k)
	}
	return keys
}

func (pc column) Insert(id Key, value string) error {
	if _, ok := pc[id]; ok {
		return fmt.Errorf("id %d already exists", id)
	}
	if strings.HasPrefix(value, "'") || strings.HasPrefix(value, "\"") {
		v, err := strconv.Unquote(value)
		if err != nil {
			return err
		}
		value = v
	}
	pc[id] = value
	return nil
}

func (pc column) Update(id Key, value string) {
	pc[id] = value
}

func (pc column) Delete(id Key) error {
	if _, ok := pc[id]; !ok {
		return fmt.Errorf("id %d not known", id)
	}
	delete(pc, id)
	return nil
}
