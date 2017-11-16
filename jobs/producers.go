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

func NewRandomProducer(d time.Duration, header ...string) data.Source {
	out := data.NewSource(header)
	id := fmt.Sprintf("[Prod %v]: ", atomic.AddUint64(&ProdCnt, 1))
	go func() {
		for goOn := true; goOn; {
			var r data.Row
			for i := 0; i < len(header); i++ {
				r = append(r, data.I32(rand.Intn(30)))
			}
			select {
			case out.Data <- r:
				log.Printf(id+"Send %v\n", r)
				time.Sleep(d)
			case <-out.Stop:
				goOn = false
			}
		}
		log.Println(id + "Finished.")
		out.Signal()
	}()
	return out
}
