package ops

import (
	"github.com/lionell/aqua/column"
	"github.com/lionell/aqua/data"
	"fmt"
)

func Join(in1 data.Source, in2 data.Source, jc []column.JoinCondition, t column.JoinType) data.Source {
	out := data.NewSource(generateHeader(in1.Header, in2.Header, jc))
	go func() {
		var d1, d2 []data.Row
		s1 := data.NewRowSet()
		s2 := data.NewRowSet()
		ic, err := indexConditions(jc, in1.Header, in2.Header)
		if err != nil {
			// TODO(lionell): Handle error.
		}
		for goOn := true; goOn; {
			select {
			case r1 := <-in1.Data:
				d1 = append(d1, r1)
				for _, r2 := range d2 {
					if !satisfy(r1, r2, ic) {
						continue
					}
					s1.Put(r1)
					s2.Put(r2)
					n := join(r1, r2, ic)
					if goOn = out.TrySend(n); !goOn {
						break
					}
				}
			case r2 := <-in2.Data:
				d2 = append(d2, r2)
				for _, r1 := range d1 {
					if !satisfy(r1, r2, ic) {
						continue
					}
					s1.Put(r1)
					s2.Put(r2)
					n := join(r1, r2, ic)
					if goOn = out.TrySend(n); !goOn {
						break
					}
				}
			case <-in1.Done:
				in1.MarkFinalized()
				switch t {
				case column.JoinLeft, column.JoinFull:
					for _, r := range d1 {
						if s1.Has(r) {
							continue
						}
						n := join(r, nil, ic)
						if goOn = out.TrySend(n); !goOn {
							break
						}
					}
				case column.JoinInner:
					goOn = false
				}
			case <-in2.Done:
				in2.MarkFinalized()
				switch t {
				case column.JoinRight, column.JoinFull:
					for _, r := range d2 {
						if s2.Has(r) {
							continue
						}
						n := join(r, nil, ic)
						if goOn = out.TrySend(n); !goOn {
							break
						}
					}
				case column.JoinInner:
					goOn = false
				}
			case <-out.Stop:
				goOn = false
			}
		}
		in1.Finalize()
		in2.Finalize()
		out.Signal()
	}()
	return out
}

func generateHeader(h1, h2 data.Header, cs []column.JoinCondition) data.Header {
	// TODO(lionell): Implement this.
	return nil
}

type condition struct {
	leftIndex, rightIndex int
}

func indexConditions(jc []column.JoinCondition, h1, h2 data.Header) ([]condition, error) {
	var res []condition
	for _, c := range jc {
		i, err := h1.Find(c.LeftColumn)
		if err != nil {
			return nil, fmt.Errorf("can't find left part of join condition %v in the header %v", c.LeftColumn, h1)
		}
		j, err := h2.Find(c.RightColumn)
		if err != nil {
			return nil, fmt.Errorf("can't find right part of join condition %v in the header %v", c.RightColumn, h2)
		}
		res = append(res, condition{i, j})
	}
	return res, nil
}

func satisfy(r1, r2 data.Row, ic []condition) bool {
	for _, c := range ic {
		if !r1[c.leftIndex].Equals(r2[c.rightIndex]) {
			return false
		}
	}
	return true
}

func join(r1, r2 data.Row, ic []condition) data.Row {
	// TODO(lionell): Implement this.
	return nil
}
