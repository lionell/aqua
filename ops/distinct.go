package ops

import (
	"fmt"
	"github.com/lionell/aqua/data"
	"log"
	"sync/atomic"
)

var DistinctCnt uint64 = 0

func Distinct(in data.Source) data.Source {
	out := data.NewSource()
	id := fmt.Sprintf("[Distinct %v]: ", atomic.AddUint64(&DistinctCnt, 1))

	go func() {
		s := data.NewRowSet()
		for goOn := true; goOn; {
			select {
			case r := <-in.Data:
				if s.Has(r) {
					log.Printf(id+"Skipping %v", r)
					continue
				}
				s.Put(r)
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
