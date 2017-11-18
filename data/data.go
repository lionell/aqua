package data

import "fmt"

type Value interface {
	Less(v Value) bool
	Equals(v Value) bool
}

type Header []string

func (h Header) Find(s string) (int, error) {
	for i, v := range h {
		if v == s {
			return i, nil
		}
	}
	return -1, fmt.Errorf("element (%v) not found in the header %v", s, h)
}

func Bind(r Row, h Header) (map[string]Value, error) {
	if len(r) != len(h) {
		return nil, fmt.Errorf("row length (%v) is not equal to header length (%v)", len(r), len(h))
	}
	res := make(map[string]Value)
	for i, v := range r {
		res[h[i]] = v
	}
	return res, nil
}

type Table struct {
	Header
	Rows []Row
}

func MakeTable(h Header, r []Row) Table {
	return Table{h, r}
}
