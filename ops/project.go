package ops

import (
	"fmt"
	"github.com/lionell/aqua/column"
	"github.com/lionell/aqua/data"
	"log"
	"sync/atomic"
)

var ProjectCnt uint64 = 0

func Project(in data.Source, ds []column.Definition) data.Source {
	var h data.Header
	for _, d := range ds {
		h = append(h, d.Name)
	}
	out := data.NewSource(h)
	id := fmt.Sprintf("[Project %v]: ", atomic.AddUint64(&ProjectCnt, 1))
	go func() {
		for goOn := true; goOn; {
			select {
			case r := <-in.Data:
				m, err := data.Bind(r, in.Header)
				if err != nil {
					// TODO(lionell): Handle error
					break
				}
				r, err = eval(ds, m)
				if err != nil {
					// TODO(lionell): Handle error
					break
				}
				goOn = out.TrySend(r)
			case <-in.Done:
				log.Println(id + "No more work to do.")
				in.MarkFinalized()
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

func eval(ds []column.Definition, m map[string]data.Value) (data.Row, error) {
	var r data.Row
	for _, d := range ds {
		v, err := d.Eval(m)
		if err != nil {
			return nil, err
		}
		r = append(r, v)
	}
	return r, nil
}
