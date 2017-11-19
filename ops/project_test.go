package ops

import (
	. "github.com/lionell/aqua/column"
	. "github.com/lionell/aqua/data"
	. "github.com/lionell/aqua/testutil"
	"testing"
)

// TODO(lionell): Test for errors.

func TestProject(t *testing.T) {
	tests := []struct {
		desc    string
		in, exp Table
		defs    []Definition
	}{
		{
			desc: "project column on itself",
			in: MakeTable([]Column{{"a", TypeI32}}, []Row{
				{I32(1)},
				{I32(2)},
			}),
			defs: []Definition{{"a", NewSumExpression("a")}},
			exp: MakeTable([]Column{{"a", TypeI32}}, []Row{
				{I32(1)},
				{I32(2)},
			}),
		},
		{
			desc: "sum of columns",
			in: MakeTable([]Column{{"a", TypeI32}, {"b", TypeI32}}, []Row{
				{I32(1), I32(8)},
				{I32(2), I32(-1)},
			}),
			defs: []Definition{{"c", NewSumExpression("a", "b")}},
			exp: MakeTable([]Column{{"c", TypeI32}}, []Row{
				{I32(9)},
				{I32(1)},
			}),
		},
		{
			desc: "no redundant columns in output",
			in: MakeTable([]Column{{"a", TypeI32}, {"b", TypeI32}}, []Row{
				{I32(1), I32(8)},
				{I32(2), I32(-1)},
			}),
			defs: []Definition{{"c", NewSumExpression("a")}},
			exp: MakeTable([]Column{{"c", TypeI32}}, []Row{
				{I32(1)},
				{I32(2)},
			}),
		},
	}
	for _, ts := range tests {
		ds := StartProducer(ts.in)
		ds, err := Project(ds, ts.defs)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		res := RunConsumer(ds)
		AssertEqualTablesInOrder(t, res, ts.exp)
	}
}

func TestProjectCanStop(t *testing.T) {
	in := MakeTable([]Column{{"a", TypeI32}}, []Row{
		{I32(1)},
	})
	exp := []Row{
		{I32(1)},
		{I32(1)},
	}

	ds := StartInfiniteProducer(in)
	ds, err := Project(ds, []Definition{{"a", NewSumExpression("a")}})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	res := RunConsumerWithLimit(ds, 2)

	AssertEqualRowsInOrder(t, res.Rows, exp)
}
