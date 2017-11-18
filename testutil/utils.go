package testutil

import (
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
		t.Fatalf("%v rows expected, got %v", len(exp), len(act))
	}
	for i, r1 := range act {
		r2 := exp[i]
		if !r1.Equals(r2) {
			t.Fatalf("rows #%v are not equal, expected %v got %v", i, r2, r1)
		}
	}
}

func AssertEqualRows(t *testing.T, act, exp []data.Row) {
	if len(act) != len(exp) {
		t.Fatalf("%v rows expected, got %v", len(exp), len(act))
	}
	for len(act) > 0 {
		j := find(t, act[0], exp)
		act = act[1:]
		exp = append(exp[:j], exp[j+1:]...)
	}
}

func find(t *testing.T, r data.Row, d []data.Row) int {
	for i, x := range d {
		if r.Equals(x) {
			return i
		}
	}
	t.Fatalf("can't find row %v in %v", r, d)
	return 0
}

func AssertEqualHeaders(t *testing.T, act, exp data.Header) {
	if len(act) != len(exp) {
		t.Fatalf("%v column expected, got %v", len(exp), len(act))
	}
	for i, v1 := range act {
		if v1 != exp[i] {
			t.Fatalf("column names with index %v are not equal (%v vs %v)", i, v1, exp[i])
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
