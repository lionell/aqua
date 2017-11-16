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

func AssertEqualRows(t *testing.T, a, b []data.Row) {
	if len(a) != len(b) {
		t.Fatalf("rows have different lengths (%v vs %v)", len(a), len(b))
	}
	for i, r1 := range a {
		r2 := b[i]
		if !r1.Equals(r2) {
			t.Fatalf("elements with index %v are not equal (%v vs %v)", i, r1, r2)
		}
	}
}

func AssertEqualHeaders(t *testing.T, a, b data.Header) {
	if len(a) != len(b) {
		t.Fatalf("headers have different lengths (%v vs %v)", len(a), len(b))
	}
	for i, v1 := range a {
		if v1 != b[i] {
			t.Fatalf("column names with index %v are not equal (%v vs %v)", i, v1, b[i])
		}
	}
}
