package column

import "github.com/lionell/aqua/data"

type Expression interface {
	Eval(bindings map[string]data.Value)
}
