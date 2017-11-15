package testutil

import (
	"github.com/lionell/aqua/data"
	"github.com/lionell/aqua/column"
	"fmt"
)

type sumExpression []string

func (e sumExpression) Eval(m map[string]data.Value) (data.Value, error) {
	sum := 0
	for _, c := range e {
		if v, ok := m[string(c)]; ok {
			if i, ok := v.(data.I32); ok {
				sum += int(i)
			} else {
				return nil, fmt.Errorf("can't check if odd, expected value of type I32 got %#v", v)
			}
		} else {
			return nil, fmt.Errorf("there is no value bound to column (%v) in the map %v", string(c), m)
		}
	}
	return data.I32(sum), nil
}

func NewSumExpression(columns ...string) column.Expression {
	return sumExpression(columns)
}
