package jobs

import (
	"fmt"
	"github.com/lionell/aqua/data"
	"log"
	"math/rand"
	"sync/atomic"
	"time"
)

var ProdCnt uint64 = 0

func NewRandomProducer(d time.Duration) data.Source {
	out := data.NewSource()
	id := fmt.Sprintf("[Prod %v]: ", atomic.AddUint64(&ProdCnt, 1))

	go func() {
	Loop:
		for {
			r := data.RowOf(data.I32(rand.Intn(30)), data.I32(rand.Intn(30)), data.I32(rand.Intn(30)))
			//r := data.RowOf(data.I32(rand.Intn(30)))
			select {
			case out.Data <- r:
				log.Printf(id+"Send %v\n", r)
				time.Sleep(d)
			case <-out.Stop:
				break Loop
			}
		}
		log.Println(id + "Finished.")
		out.Done <- struct{}{}
	}()

	return out
}
