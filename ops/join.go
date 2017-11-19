package ops

import (
	"fmt"
	"github.com/lionell/aqua/column"
	"github.com/lionell/aqua/data"
	"github.com/pkg/errors"
)

func Join(in1, in2 data.Source, jc []column.JoinCondition, t column.JoinType) (data.Source, error) {
	ic, err := indexConditions(jc, in1.Header, in2.Header)
	if err != nil {
		return data.Source{}, errors.Wrap(err, "can't index join condition")
	}
	out := data.NewSource(generateHeader(in1.Header, in2.Header))
	go func() {
		var d1, d2 []data.Row
		s1 := data.NewRowSet()
		s2 := data.NewRowSet()
		goOn := true
		for goOn && (!in1.IsFinalized() || !in2.IsFinalized()) {
			select {
			case r1 := <-in1.Data:
				d1 = append(d1, r1)
				for _, r2 := range d2 {
					if !satisfy(r1, r2, ic) {
						continue
					}
					s1.Put(r1)
					s2.Put(r2)
					var n []data.Value
					n = append(append(n, r1...), r2...)
					if goOn = out.Send(n); !goOn {
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
					var n []data.Value
					n = append(append(n, r1...), r2...)
					if goOn = out.Send(n); !goOn {
						break
					}
				}
			case <-in1.Done:
				in1.MarkFinalized()
			case <-in2.Done:
				in2.MarkFinalized()
			case <-out.Stop:
				goOn = false
			}
		}
		if goOn {
			// Handle not matched rows
			if t == column.JoinLeft || t == column.JoinFull {
				for _, r := range d1 {
					if s1.Has(r) {
						continue
					}
					// Make row [r..., none, none, ...]
					var n []data.Value
					n = append(append(n, r...), makeNoneRow(len(in2.Header))...)
					if goOn = out.Send(n); !goOn {
						break
					}
				}
			}
			if t == column.JoinRight || t == column.JoinFull {
				for _, r := range d2 {
					if s2.Has(r) {
						continue
					}
					// Make row [none, none, ..., r...]
					n := append(makeNoneRow(len(in1.Header)), r...)
					if goOn = out.Send(n); !goOn {
						break
					}
				}
			}
		}
		in1.Finalize()
		in2.Finalize()
		out.Signal()
	}()
	return out, nil
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

func generateHeader(h1, h2 data.Header) data.Header {
	var res []data.Column
	res = append(append(res, h1...), h2...)
	u := make(map[string]bool)
	for i, c := range res {
		if u[c.Name] {
			res[i].Name = "$" + res[i].Name
		}
		u[c.Name] = true
	}
	return res
}

func makeNoneRow(length int) data.Row {
	r := make([]data.Value, length)
	for i := range r {
		r[i] = data.None{}
	}
	return r
}
