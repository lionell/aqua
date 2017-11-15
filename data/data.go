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
