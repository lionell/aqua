package ops

import (
	"fmt"
	"github.com/lionell/aqua/column"
	"github.com/lionell/aqua/data"
	"log"
	"sync/atomic"
)

var WhereCnt uint64 = 0

func Where(in data.Source, c column.Condition) (data.Source, error) {
	if err := c.Verify(in.Header.Types()); err != nil {
		in.Finalize()
		return data.Source{}, err
	}
	out := data.NewSource(in.Header)
	id := fmt.Sprintf("[Where %v]: ", atomic.AddUint64(&WhereCnt, 1))
	go func() {
		for goOn := true; goOn; {
			select {
			case r := <-in.Data:
				m, err := in.Header.BindRow(r)
				if err != nil {
					panic(err)
				}
				ok, err := c.Check(m)
				if err != nil {
					panic(err)
				}
				if !ok {
					continue
				}
				goOn = out.Send(r)
			case <-in.Done:
				log.Println(id + "No more work to do.")
				in.MarkFinalized()
				goOn = false
			case <-out.Stop:
				goOn = false
			}
		}
		in.Finalize()
		log.Println(id + "Finished.")
		out.Signal()
	}()
	return out, nil
}
