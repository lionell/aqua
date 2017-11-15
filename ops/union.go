package ops

import (
	"fmt"
	"github.com/lionell/aqua/data"
	"log"
	"sync/atomic"
)

var UnionCnt uint64 = 0

func Union(in1, in2 data.Source) data.Source {
	out := data.NewSource()
	id := fmt.Sprintf("[Union %v]: ", atomic.AddUint64(&UnionCnt, 1))

	go func() {
		for goOn := true; goOn; {
			select {
			case r := <-in1.Data:
				goOn = out.TrySend(r)
			case <-in1.Done:
				log.Println(id + "First source is empty.")
				in1.SetFinalized()
				for goOn {
					select {
					case r := <-in2.Data:
						goOn = out.TrySend(r)
					case <-in2.Done:
						log.Println(id + "Second source is empty.")
						in2.SetFinalized()
						goOn = false
					case <-out.Stop:
						goOn = false
					}
				}
			case <-out.Stop:
				goOn = false
			}
		}
		log.Println(id + "Finished.")
		in1.Finalize()
		in2.Finalize()
		out.Signal()
	}()

	return out
}
