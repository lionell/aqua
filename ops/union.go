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
	Loop:
		for {
			select {
			case r := <-in1.Data:
				select {
				case out.Data <- r:
				case <-out.Stop:
					break Loop
				}
			case <-in1.Done:
				log.Println(id + "First source is empty.")
				in1.SetFinalized()
				for {
					select {
					case r := <-in2.Data:
						select {
						case out.Data <- r:
						case <-out.Stop:
							break Loop
						}
					case <-in2.Done:
						log.Println(id + "Second source is empty.")
						in2.SetFinalized()
						break Loop
					case <-out.Stop:
						break Loop
					}
				}
			case <-out.Stop:
				break Loop
			}
		}
		log.Println(id + "Finished.")
		in1.Finalize()
		in2.Finalize()
		out.Signal()
	}()

	return out
}
