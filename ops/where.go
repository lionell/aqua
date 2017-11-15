package ops

import (
	"fmt"
	"github.com/lionell/aqua/column"
	"github.com/lionell/aqua/data"
	"log"
	"sync/atomic"
)

var WhereCnt uint64 = 0

func Where(in data.Source, c column.Condition, h data.Header) (data.Source, <-chan error) {
	out := data.NewSource()
	errs := make(chan error)
	id := fmt.Sprintf("[Where %v]: ", atomic.AddUint64(&WhereCnt, 1))

	go func() {
	Loop:
		for {
			select {
			case r := <-in.Data:
				m, err := bind(r, h)
				if err != nil {
					errs <- err
					break
				}
				ok, err := c.Check(m)
				if err != nil {
					errs <- err
					break
				}
				if !ok {
					break
				}
				select {
				case out.Data <- r:
				case <-out.Stop:
					break Loop
				}
			case <-in.Done:
				log.Println(id + "No more work to do.")
				in.SetFinalized()
				break Loop
			case <-out.Stop:
				break Loop
			}
		}
		in.Finalize()
		log.Println(id + "Finished.")
		out.Signal()
	}()

	return out, errs
}

func bind(r data.Row, h data.Header) (map[string]data.Value, error) {
	if len(r) != len(h) {
		return nil, fmt.Errorf("row length (%v) is not equal to header length (%v)", len(r), len(h))
	}
	res := make(map[string]data.Value)
	for i, v := range r {
		res[h[i]] = v
	}
	return res, nil
}
