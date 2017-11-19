package ops

import (
	. "github.com/lionell/aqua/data"
	. "github.com/lionell/aqua/testutil"
	"testing"
)

// TODO(lionell): Test union throws error when headers don't match.

func TestUnion(t *testing.T) {
	tests := []struct {
		in1, in2 Table
		exp      []Row
	}{
		{
			in1: MakeTable([]Column{{"a", TypeI32}}, []Row{
				{I32(1)},
				{I32(3)},
			}),
			in2: MakeTable([]Column{{"a", TypeI32}}, nil),
			exp: []Row{
				{I32(1)},
				{I32(3)},
			},
		},
		{
			in1: MakeTable([]Column{{"a", TypeI32}}, nil),
			in2: MakeTable([]Column{{"a", TypeI32}}, []Row{
				{I32(1)},
				{I32(3)},
			}),
			exp: []Row{
				{I32(1)},
				{I32(3)},
			},
		},
		{
			in1: MakeTable([]Column{{"a", TypeI32}}, nil),
			in2: MakeTable([]Column{{"a", TypeI32}}, nil),
			exp: nil,
		},
		{
			in1: MakeTable([]Column{{"a", TypeI32}}, []Row{
				{I32(1)},
				{I32(3)},
			}),
			in2: MakeTable([]Column{{"a", TypeI32}}, []Row{
				{I32(7)},
				{I32(8)},
			}),
			exp: []Row{
				{I32(1)},
				{I32(3)},
				{I32(7)},
				{I32(8)},
			},
		},
	}
	for _, ts := range tests {
		ds1 := StartProducer(ts.in1)
		ds2 := StartProducer(ts.in2)
		ds, err := Union(ds1, ds2)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		res := RunConsumer(ds)
		AssertEqualRowsInOrder(t, res.Rows, ts.exp)
	}
}

func TestUnionCanStopWhileProcessingFirstSource(t *testing.T) {
	in := MakeTable([]Column{{"a", TypeI32}}, []Row{
		{I32(5)},
		{I32(7)},
	})

	ds1 := StartInfiniteProducer(in)
	ds2 := StartProducer(MakeTable([]Column{{"a", TypeI32}}, nil))
	ds, err := Union(ds1, ds2)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	res := RunConsumerWithLimit(ds, 2)

	AssertEqualRowsInOrder(t, res.Rows, in.Rows)
}

func TestUnionCanStopWhileProcessingSecondSource(t *testing.T) {
	in := MakeTable([]Column{{"a", TypeI32}}, []Row{
		{I32(5)},
		{I32(7)},
	})

	ds1 := StartProducer(MakeTable([]Column{{"a", TypeI32}}, nil))
	ds2 := StartInfiniteProducer(in)
	ds, err := Union(ds1, ds2)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	res := RunConsumerWithLimit(ds, 2)

	AssertEqualRowsInOrder(t, res.Rows, in.Rows)
}

func TestUnionPreservesHeader(t *testing.T) {
	ds1 := StartProducer(MakeTable([]Column{{"a", TypeI32}, {"b", TypeI32}}, nil))
	ds2 := StartProducer(MakeTable([]Column{{"a", TypeI32}, {"b", TypeI32}}, nil))
	ds, err := Union(ds1, ds2)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	res := RunConsumer(ds)

	AssertEqualHeaders(t, res.Header, []Column{{"a", TypeI32}, {"b", TypeI32}})
}
