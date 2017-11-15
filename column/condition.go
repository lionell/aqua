package column

import (
	"fmt"
	"github.com/lionell/aqua/data"
)

type Condition interface {
	Check(bindings map[string]data.Value) (bool, error)
}

type FakeCondition struct{}

func (c *FakeCondition) Check(bindings map[string]data.Value) (bool, error) {
	v, ok := bindings["test"]
	if !ok {
		return false, fmt.Errorf("can't find column (%v) in the mappings", "test")
	}
	i, ok := v.(data.I32)
	if !ok {
		return false, fmt.Errorf("expected value of type I32 got of type (%T)", v)
	}
	return i%2 == 0, nil
}

func NewFakeCondition() *FakeCondition {
	return &FakeCondition{}
}
