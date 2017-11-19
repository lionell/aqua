package column

import "github.com/lionell/aqua/data"

type Expression interface {
	Eval(vs map[string]data.Value) (data.Value, error)
	DeduceType(ts map[string]data.Type) (data.Type, error)
}
