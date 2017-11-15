package ops

import (
	"github.com/lionell/aqua/column"
	"github.com/lionell/aqua/data"
)

func Project(in data.Source, defs []column.Definition) data.Source {
	return in
}
