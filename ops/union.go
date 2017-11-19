package ops

import (
	"fmt"
	"github.com/lionell/aqua/data"
	"log"
	"sync/atomic"
)

var UnionCnt uint64 = 0

func Union(in1, in2 data.Source) (data.Source, error) {
	if err := verifySameHeaders(in1.Header, in2.Header); err != nil {
		in1.Finalize()
		in2.Finalize()
		return data.Source{}, err
	}
	out := data.NewSource(in1.Header)
	id := fmt.Sprintf("[Union %v]: ", atomic.AddUint64(&UnionCnt, 1))
	go func() {
		for goOn := true; goOn; {
			select {
			case r := <-in1.Data:
				goOn = out.Send(r)
			case <-in1.Done:
				log.Println(id + "First source is empty.")
				in1.MarkFinalized()
				for goOn {
					select {
					case r := <-in2.Data:
						goOn = out.Send(r)
					case <-in2.Done:
						log.Println(id + "Second source is empty.")
						in2.MarkFinalized()
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
	return out, nil
}

func verifySameHeaders(h1, h2 data.Header) error {
	if len(h1) != len(h2) {
		return fmt.Errorf("header length mismatch (%v vs %v)", len(h1), len(h2))
	}
	for i, x := range h1 {
		if h2[i] != x {
			return fmt.Errorf("header mismatch in column %v (%v vs %v)", i, h2[i], x)
		}
	}
	return nil
}
