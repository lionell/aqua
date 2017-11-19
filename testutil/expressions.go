package testutil

import (
	"fmt"
	"github.com/lionell/aqua/column"
	"github.com/lionell/aqua/data"
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

func (e sumExpression) Verify(m map[string]data.Type) error {
	return nil
}

func (e sumExpression) DeduceType(m map[string]data.Type) (data.Type, error) {
	return data.TypeI32, nil
}

func NewSumExpression(columns ...string) column.Expression {
	return sumExpression(columns)
}

type WrongExpression struct{}

func (e WrongExpression) Eval(map[string]data.Value) (data.Value, error) {
	return data.None{}, fmt.Errorf("can't eval wrong expression")
}

func (e WrongExpression) DeduceType(map[string]data.Type) (data.Type, error) {
	return data.TypeNone, fmt.Errorf("can't deduce type of wrong expression")
}
