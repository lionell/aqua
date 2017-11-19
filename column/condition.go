package column

import (
	"github.com/lionell/aqua/data"
)

type Condition interface {
	Check(bindings map[string]data.Value) (bool, error)
	Verify(bindings map[string]data.Type) error
}
