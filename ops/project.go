package ops

import (
	"fmt"
	"github.com/lionell/aqua/column"
	"github.com/lionell/aqua/data"
	"log"
	"sync/atomic"
)

var ProjectCnt uint64 = 0

func Project(in data.Source, h data.Header, es []column.Expression) data.Source {
	out := data.NewSource()
	id := fmt.Sprintf("[Project %v]: ", atomic.AddUint64(&ProjectCnt, 1))

	go func() {
		for goOn := true; goOn; {
			select {
			case r := <-in.Data:
				m, err := data.Bind(r, h)
				if err != nil {
					// TODO(lionell): Handle error
					break
				}
				r, err = eval(es, m)
				if err != nil {
					// TODO(lionell): Handle error
					break
				}
				goOn = out.TrySend(r)
			case <-in.Done:
				log.Println(id + "No more work to do.")
				in.SetFinalized()
				goOn = false
			case <-out.Stop:
				goOn = false
			}
		}
		in.Finalize()
		out.Signal()
	}()

	return out
}

func eval(es []column.Expression, m map[string]data.Value) (data.Row, error) {
	r := data.Row{}
	for _, e := range es {
		v, err := e.Eval(m)
		if err != nil {
			return nil, err
		}
		r = append(r, v)
	}
	return r, nil
}
