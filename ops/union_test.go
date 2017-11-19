package ops

import (
	. "github.com/lionell/aqua/data"
	. "github.com/lionell/aqua/testutil"
	"testing"
	"time"
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

func TestUnionCanStopWhileStreamingResults(t *testing.T) {
	in := MakeTable([]Column{{"a", TypeI32}}, []Row{
		{I32(5)},
		{I32(7)},
	})

	ds1 := StartLoopingProducer(in)
	ds2 := StartProducer(MakeTable([]Column{{"a", TypeI32}}, nil))
	ds, err := Union(ds1, ds2)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	res := RunConsumerWithLimit(ds, 2)

	AssertEqualRowsInOrder(t, res.Rows, in.Rows)
}

func TestUnionCanStopWhileStreamingResults1(t *testing.T) {
	in := MakeTable([]Column{{"a", TypeI32}}, []Row{
		{I32(5)},
		{I32(7)},
	})

	ds1 := StartProducer(MakeTable([]Column{{"a", TypeI32}}, nil))
	ds2 := StartLoopingProducer(in)
	ds, err := Union(ds1, ds2)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	res := RunConsumerWithLimit(ds, 2)

	AssertEqualRowsInOrder(t, res.Rows, in.Rows)
}

func TestUnionCanStopWhileWaitingForInput(t *testing.T) {
	t.Parallel()
	ds1 := StartBlockingProducer([]Column{{"a", TypeI32}})
	ds2 := StartProducer(MakeTable([]Column{{"a", TypeI32}}, nil))
	ds, err := Union(ds1, ds2)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	RunConsumerWithTimeout(ds, time.Millisecond*50)
}

func TestUnionCanStopWhileWaitingForInput1(t *testing.T) {
	t.Parallel()
	ds1 := StartProducer(MakeTable([]Column{{"a", TypeI32}}, nil))
	ds2 := StartBlockingProducer([]Column{{"a", TypeI32}})
	ds, err := Union(ds1, ds2)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	RunConsumerWithTimeout(ds, time.Millisecond*50)
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

func TestUnionReturnsErrorOnHeaderMismatch(t *testing.T) {
	ds1 := StartBlockingProducer([]Column{{"b", TypeI32}})
	ds2 := StartBlockingProducer([]Column{{"a", TypeI32}})
	_, err := Union(ds1, ds2)
	if err == nil {
		t.Errorf("Error expected")
	}
}

func TestUnionReturnsErrorOnHeaderMismatch1(t *testing.T) {
	ds1 := StartBlockingProducer([]Column{{"a", TypeI32}})
	ds2 := StartBlockingProducer([]Column{{"a", TypeI32}, {"b", TypeI32}})
	_, err := Union(ds1, ds2)
	if err == nil {
		t.Errorf("Error expected")
	}
}
