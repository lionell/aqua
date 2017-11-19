package data

import "fmt"

type I32 int32

func (i I32) Type() Type {
	return TypeI32
}

func (i I32) Less(v Value) bool {
	return i < toI32(v)
}

func (i I32) Equals(v Value) bool {
	return i == toI32(v)
}

func toI32(v Value) I32 {
	j, ok := v.(I32)
	if !ok {
		panic(fmt.Sprintf("Can't convert value (%v) of type (%T) to type I32", v, v))
	}
	return j
}
