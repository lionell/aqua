package ops

import (
	"fmt"
	"github.com/lionell/aqua/column"
	"github.com/lionell/aqua/data"
	"log"
	"sync/atomic"
)

var ProjectCnt uint64 = 0

func Project(in data.Source, ds []column.Definition) (data.Source, error) {
	h, err := projectHeader(ds, in.Header.Types())
	if err != nil {
		in.Finalize()
		return data.Source{}, err
	}
	out := data.NewSource(h)
	id := fmt.Sprintf("[Project %v]: ", atomic.AddUint64(&ProjectCnt, 1))
	go func() {
		for goOn := true; goOn; {
			select {
			case r := <-in.Data:
				m, err := in.Header.BindRow(r)
				if err != nil {
					panic(err)
				}
				r, err = evalDefinitions(ds, m)
				if err != nil {
					panic(err)
				}
				goOn = out.Send(r)
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
	return out, nil
}

func projectHeader(ds []column.Definition, tm map[string]data.Type) (data.Header, error) {
	var h data.Header
	for _, d := range ds {
		t, err := d.DeduceType(tm)
		if err != nil {
			return nil, err
		}
		h = append(h, data.Column{Name: d.Name, Type: t})
	}
	return h, nil
}

func evalDefinitions(ds []column.Definition, m map[string]data.Value) (data.Row, error) {
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
