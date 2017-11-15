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
		for goOn := true; goOn; {
			if cnt == 0 {
				break
			}
			select {
			case r := <-in.Data:
				cnt--
				goOn = out.TrySend(r)
				//select {
				//case out.Data <- r:
				//case <-out.Stop:
				//	break Loop
				//}
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
