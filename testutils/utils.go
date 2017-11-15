package testutils

import (
	"github.com/lionell/aqua/data"
	"testing"
)

func AssertEquals(t *testing.T, a, b []data.Row) {
	if len(a) != len(b) {
		t.Fatalf("arrays have different lengths (%v vs %v)", len(a), len(b))
	}
	for i, r1 := range a {
		r2 := b[i]
		if !r1.Equals(r2) {
			t.Fatalf("elements with index %v are not equal (%v vs %v)", i, r1, r2)
		}
	}
}
