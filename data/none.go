package data

import "fmt"

type None struct{}

func (n None) Less(v Value) bool {
	return false
}

func (n None) Equals(v Value) bool {
	return isNone(v)
}

func isNone(v Value) bool {
	_, ok := v.(None)
	if !ok {
		panic(fmt.Sprintf("Can't convert value (%v) of type (%T) to type None", v, v))
	}
	return true
}
