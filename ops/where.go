package ops

import (
	"fmt"
	"github.com/lionell/aqua/column"
	"github.com/lionell/aqua/data"
	"log"
	"sync/atomic"
)

var WhereCnt uint64 = 0

func Where(in data.Source, h data.Header, c column.Condition) data.Source {
	out := data.NewSource()
	id := fmt.Sprintf("[Where %v]: ", atomic.AddUint64(&WhereCnt, 1))

	go func() {
		for goOn := true; goOn; {
			select {
			case r := <-in.Data:
				m, err := data.Bind(r, h)
				if err != nil {
					// TODO(lionell): Handle error
					break
				}
				ok, err := c.Check(m)
				if err != nil {
					// TODO(lionell): Handle error
					break
				}
				if !ok {
					continue
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
		log.Println(id + "Finished.")
		out.Signal()
	}()

	return out
}
