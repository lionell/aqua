package testutil

import (
	"fmt"
	"github.com/lionell/aqua/data"
	"io/ioutil"
	"log"
	"testing"
)

func init() {
	log.SetOutput(ioutil.Discard)
}

func AssertEqualRowsInOrder(t *testing.T, act, exp []data.Row) {
	if len(act) != len(exp) {
		t.Errorf("%v rows expected, got %v", len(exp), len(act))
		return
	}
	for i, r1 := range act {
		r2 := exp[i]
		if !r1.Equals(r2) {
			t.Errorf("rows #%v are not equal, expected %v got %v", i, r2, r1)
			return
		}
	}
}

func AssertEqualRows(t *testing.T, act, exp []data.Row) {
	if len(act) != len(exp) {
		t.Errorf("%v rows expected, got %v", len(exp), len(act))
		return
	}
	for len(act) > 0 {
		j, err := find(act[0], exp)
		if err != nil {
			t.Error(err)
			return
		}
		act = act[1:]
		exp = append(exp[:j], exp[j+1:]...)
	}
}

func find(r data.Row, d []data.Row) (int, error) {
	for i, x := range d {
		if r.Equals(x) {
			return i, nil
		}
	}
	return -1, fmt.Errorf("can't find row %v among rows %v", r, d)
}

func AssertEqualHeaders(t *testing.T, act, exp data.Header) {
	if len(act) != len(exp) {
		t.Errorf("%v columns expected, got %v", len(exp), len(act))
		return
	}
	for i, v1 := range act {
		if v1 != exp[i] {
			t.Errorf("column names with index %v are not equal (%v vs %v)", i, v1, exp[i])
			return
		}
	}
}

func AssertEqualTables(t *testing.T, act, exp data.Table) {
	AssertEqualRows(t, act.Rows, exp.Rows)
	AssertEqualHeaders(t, act.Header, exp.Header)
}

func AssertEqualTablesInOrder(t *testing.T, act, exp data.Table) {
	AssertEqualRowsInOrder(t, act.Rows, exp.Rows)
	AssertEqualHeaders(t, act.Header, exp.Header)
}
