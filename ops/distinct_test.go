package ops

import (
	. "github.com/lionell/aqua/data"
	. "github.com/lionell/aqua/testutil"
	"testing"
)

func TestDistinct(t *testing.T) {
	tests := []struct {
		desc string
		in   Table
		exp  []Row
	}{
		{
			desc: "empty source",
			in:   MakeTable([]Column{{"a", TypeI32}}, nil),
			exp:  nil,
		},
		{
			desc: "different rows",
			in: MakeTable([]Column{{"a", TypeI32}, {"b", TypeI32}}, []Row{
				{I32(1), I32(7)},
				{I32(3), I32(8)},
			}),
			exp: []Row{
				{I32(1), I32(7)},
				{I32(3), I32(8)},
			},
		},
		{
			desc: "duplicate rows",
			in: MakeTable([]Column{{"a", TypeI32}, {"b", TypeI32}}, []Row{
				{I32(1), I32(2)},
				{I32(3), I32(4)},
				{I32(1), I32(2)},
			}),
			exp: []Row{
				{I32(1), I32(2)},
				{I32(3), I32(4)},
			},
		},
	}
	for _, ts := range tests {
		ds := StartProducer(ts.in)
		ds = Distinct(ds)
		res := RunConsumer(ds)
		AssertEqualRowsInOrder(t, res.Rows, ts.exp)
	}
}

func TestDistinctCanStop(t *testing.T) {
	in := MakeTable([]Column{{"a", TypeI32}}, []Row{
		{I32(1)},
		{I32(3)},
	})
	exp := []Row{
		{I32(1)},
	}

	ds := StartInfiniteProducer(in)
	ds = Distinct(ds)
	res := RunConsumerWithLimit(ds, 1)

	AssertEqualRowsInOrder(t, res.Rows, exp)
}

func TestDistinctPreservesHeader(t *testing.T) {
	ds := StartProducer(MakeTable([]Column{{"a", TypeI32}, {"b", TypeI32}}, nil))
	ds = Distinct(ds)
	res := RunConsumer(ds)

	AssertEqualHeaders(t, res.Header, []Column{{"a", TypeI32}, {"b", TypeI32}})
}
