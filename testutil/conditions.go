package testutil

import (
	"fmt"
	"github.com/lionell/aqua/column"
	"github.com/lionell/aqua/data"
)

type conditionWithLimit int

func (c *conditionWithLimit) Check(m map[string]data.Value) (bool, error) {
	if *c > 0 {
		*c--
		return true, nil
	}
	return false, nil
}

func (c conditionWithLimit) Verify(m map[string]data.Type) error {
	return nil
}

func NewTrueConditionWithLimit(limit int) column.Condition {
	var res = conditionWithLimit(limit)
	return &res
}

type oddCondition string

func (c oddCondition) Check(m map[string]data.Value) (bool, error) {
	if v, ok := m[string(c)]; ok {
		if i, ok := v.(data.I32); ok {
			return i%2 == 1, nil
		} else {
			return false, fmt.Errorf("can't check if odd, expected value of type I32 got %#v", v)
		}
	} else {
		return false, fmt.Errorf("there is no value bound to column (%v) in the map %v", string(c), m)
	}
}

func (c oddCondition) Verify(m map[string]data.Type) error {
	return nil
}

func NewOddCondition(columnName string) column.Condition {
	return oddCondition(columnName)
}
