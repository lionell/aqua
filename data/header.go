package data

import "fmt"

type Column struct {
	Name string
	Type
}

type Header []Column

func (h Header) Find(s string) (int, error) {
	for i, c := range h {
		if c.Name == s {
			return i, nil
		}
	}
	return -1, fmt.Errorf("element %v not found in the header %v", s, h)
}

func (h Header) BindRow(r Row) (map[string]Value, error) {
	if len(r) != len(h) {
		return nil, fmt.Errorf("row length (%v) is not equal to header length (%v)", len(r), len(h))
	}
	res := make(map[string]Value)
	for i, v := range r {
		res[h[i].Name] = v
	}
	return res, nil
}

func (h Header) Types() map[string]Type {
	res := make(map[string]Type)
	for _, c := range h {
		res[c.Name] = c.Type
	}
	return res
}
