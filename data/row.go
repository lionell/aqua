package data

import "fmt"

type Row []Value

func (r Row) Equals(o Row) bool {
	if len(r) != len(o) {
		return false
	}
	for i, v := range r {
		if !v.Equals(o[i]) {
			return false
		}
	}
	return true
}

func (r Row) toString() string {
	return fmt.Sprintf("%#v", r)
}

type RowSet struct {
	m map[string]bool
}

func (s RowSet) Put(r Row) {
	s.m[r.toString()] = true
}

func (s RowSet) Has(r Row) bool {
	return s.m[r.toString()]
}

func (s RowSet) Remove(r Row) {
	delete(s.m, r.toString())
}

func NewRowSet() RowSet {
	return RowSet{make(map[string]bool)}
}
