package jobs

import (
	"fmt"
	"github.com/lionell/aqua/data"
	"log"
	"sync"
	"sync/atomic"
)

var ConsCnt uint64 = 0

func NewPrinterWithLimit(in data.Source, lim int) *sync.WaitGroup {
	wg := &sync.WaitGroup{}
	id := fmt.Sprintf("[Cons %v]: ", atomic.AddUint64(&ConsCnt, 1))

	wg.Add(1)
	go func() {
	Loop:
		for {
			if lim == 0 {
				log.Println("Limit reached.")
				break
			}
			select {
			case r := <-in.Data:
				lim--
				log.Printf(id+"Recv %v (%v left)\n", r, lim)
			case <-in.Done:
				log.Println(id + "No more work to do.")
				in.SetFinalized()
				break Loop
			}
		}
		in.Finalize()
		log.Println(id + "Finished.")
		wg.Done()
	}()

	return wg
}
