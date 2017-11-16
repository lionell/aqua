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

func Sort(in data.Source, orders []column.SortBy) data.Source {
	out := data.NewSource(in.Header)
	id := fmt.Sprintf("[Sort %v]: ", atomic.AddUint64(&SortCnt, 1))
	go func() {
		var rows []data.Row
		for goOn := true; goOn; {
			select {
			case r := <-in.Data:
				rows = append(rows, r)
			case <-in.Done:
				in.MarkFinalized()
				log.Println(id + "Sorting...")
				o, err := convert(orders, in.Header)
				if err != nil {
					// TODO(lionell): Handle error.
				}
				sort.Sort(byOrders{rows, o})
				for _, r := range rows {
					if goOn = out.TrySend(r); !goOn {
						break
					}
				}
				goOn = false
			case <-out.Stop:
				goOn = false
			}
		}
		log.Println(id + "Finished.")
		in.Finalize()
		out.Signal()
	}()
	return out
}

type indexAndOrder struct {
	index int
	order column.Order
}

func convert(orders []column.SortBy, h data.Header) ([]indexAndOrder, error) {
	var res []indexAndOrder
	for _, o := range orders {
		i, err := h.Find(o.Column)
		if err != nil {
			return nil, err
		}
		res = append(res, indexAndOrder{i, o.Order})
	}
	return res, nil
}

type byOrders struct {
	rows   []data.Row
	orders []indexAndOrder
}

func (bo byOrders) Len() int {
	return len(bo.rows)
}

func (bo byOrders) Swap(i, j int) {
	bo.rows[i], bo.rows[j] = bo.rows[j], bo.rows[i]
}

func (bo byOrders) Less(i, j int) bool {
	if bo.orders == nil {
		return i < j
	}
	r1 := bo.rows[i]
	r2 := bo.rows[j]
	for _, o := range bo.orders {
		c := o.index
		switch o.order {
		case column.OrderAsc:
			if r1[c].Less(r2[c]) {
				return true
			} else if r2[c].Less(r1[c]) {
				return false
			}
		case column.OrderDesc:
			if r1[c].Less(r2[c]) {
				return false
			} else if r2[c].Less(r1[c]) {
				return true
			}
		default:
			log.Fatalf("Unknown order specified %v", o.order)
		}
	}
	return false
}
