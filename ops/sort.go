package ops

import (
	"fmt"
	"github.com/lionell/aqua/column"
	"github.com/lionell/aqua/data"
	"log"
	"sort"
	"sync/atomic"
)

var SortCnt uint64 = 0

func Sort(in data.Source, orders []column.Order) data.Source {
	out := data.NewSource()
	id := fmt.Sprintf("[Sort %v]: ", atomic.AddUint64(&SortCnt, 1))

	go func() {
		var rows []data.Row
	Loop:
		for {
			select {
			case r := <-in.Data:
				rows = append(rows, r)
			case <-in.Done:
				in.SetFinalized()
				log.Println(id + "Sorting...")
				sort.Sort(withOrder{rows, orders})
				for _, r := range rows {
					select {
					case out.Data <- r:
					case <-out.Stop:
						break Loop
					}
				}
				break Loop
			case <-out.Stop:
				break Loop
			}
		}
		log.Println(id + "Finished.")
		in.Finalize()
		out.Signal()
	}()

	return out
}

type withOrder struct {
	rows   []data.Row
	orders []column.Order
}

func (wo withOrder) Len() int {
	return len(wo.rows)
}

func (wo withOrder) Swap(i, j int) {
	wo.rows[i], wo.rows[j] = wo.rows[j], wo.rows[i]
}

func (wo withOrder) Less(i, j int) bool {
	if wo.orders == nil {
		return i < j
	}
	r1 := wo.rows[i]
	r2 := wo.rows[j]
	for _, o := range wo.orders {
		c := o.Column
		switch o.Order {
		case column.ASC:
			if r1[c].Less(r2[c]) {
				return true
			} else if r2[c].Less(r1[c]) {
				return false
			}
		case column.DESC:
			if r1[c].Less(r2[c]) {
				return false
			} else if r2[c].Less(r1[c]) {
				return true
			}
		default:
			log.Fatalf("Unknown order specified %v", o.Order)
		}
	}
	return false
}
