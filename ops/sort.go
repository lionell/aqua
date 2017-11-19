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

func Sort(in data.Source, so []column.SortingOrder) (data.Source, error) {
	o, err := indexOrders(so, in.Header)
	if err != nil {
		return data.Source{}, nil
	}
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
				sort.Sort(byOrders{rows, o})
				for _, r := range rows {
					if goOn = out.Send(r); !goOn {
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
	return out, nil
}

type order struct {
	index int
	order column.Order
}

func indexOrders(so []column.SortingOrder, h data.Header) ([]order, error) {
	var res []order
	for _, o := range so {
		i, err := h.Find(o.Column)
		if err != nil {
			return nil, fmt.Errorf("can't find order column %v in the header %v", o.Column, h)
		}
		res = append(res, order{i, o.Order})
	}
	return res, nil
}

type byOrders struct {
	rows   []data.Row
	orders []order
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
			panic(fmt.Sprintf("Unknown order specified %v", o.order))
		}
	}
	return false
}
