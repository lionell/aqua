package ops

import (
	"fmt"
	"github.com/lionell/aqua/data"
	"log"
	"sync/atomic"
)

var TakeCnt uint64 = 0

func Take(in data.Source, cnt int) data.Source {
	out := data.NewSource()
	id := fmt.Sprintf("[Take %v]: ", atomic.AddUint64(&TakeCnt, 1))

	go func() {
	Loop:
		for {
			if cnt == 0 {
				break Loop
			}
			select {
			case r := <-in.Data:
				select {
				case out.Data <- r:
				case <-out.Stop:
					break Loop
				}
				cnt--
			case <-in.Done:
				log.Println(id + "No more work to do.")
				in.SetFinalized()
				break Loop
			case <-out.Stop:
				log.Println(id + "Stop.")
				break Loop
			}
		}
		in.Finalize()
		log.Println(id + "Finished.")
		out.Done <- struct{}{}
	}()

	return out
}
