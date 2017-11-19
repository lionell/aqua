package ops

import (
	. "github.com/lionell/aqua/data"
	. "github.com/lionell/aqua/testutil"
	"testing"
	"time"
)

func TestTake(t *testing.T) {
	tests := []struct {
		desc string
		in   Table
		cnt  int
		exp  []Row
	}{
		{
			desc: "empty source",
			in:   MakeTable([]Column{{"a", TypeI32}}, nil),
			cnt:  10,
			exp:  nil,
		},
		{
			desc: "rows less than limit",
			in: MakeTable([]Column{{"a", TypeI32}}, []Row{
				{I32(1)},
			}),
			cnt: 10,
			exp: []Row{
				{I32(1)},
			},
		},
		{
			desc: "rows just as for limit",
			in: MakeTable([]Column{{"a", TypeI32}}, []Row{
				{I32(1)},
				{I32(2)},
			}),
			cnt: 2,
			exp: []Row{
				{I32(1)},
				{I32(2)},
			},
		},
		{
			desc: "rows more than limit",
			in: MakeTable([]Column{{"a", TypeI32}}, []Row{
				{I32(1)},
				{I32(2)},
			}),
			cnt: 1,
			exp: []Row{
				{I32(1)},
			},
		},
	}
	for _, ts := range tests {
		ds := StartProducer(ts.in)
		ds = Take(ds, ts.cnt)
		res := RunConsumer(ds)
		AssertEqualRowsInOrder(t, res.Rows, ts.exp)
	}
}

func TestTakeCanStopWhileStreamingResults(t *testing.T) {
	in := MakeTable([]Column{{"a", TypeI32}}, []Row{
		{I32(1)},
		{I32(2)},
	})

	ds := StartLoopingProducer(in)
	ds = Take(ds, 10)
	res := RunConsumerWithLimit(ds, 1)

	AssertEqualRowsInOrder(t, res.Rows, in.Rows[:1])
}

func TestTakeCanStopWhileWaitingForInput(t *testing.T) {
	t.Parallel()
	ds := StartBlockingProducer([]Column{{"a", TypeI32}})
	ds = Take(ds, 10)
	RunConsumerWithTimeout(ds, time.Millisecond*50)
}

func TestTakePreservesHeader(t *testing.T) {
	ds := StartProducer(MakeTable([]Column{{"a", TypeI32}, {"b", TypeI32}}, nil))
	ds = Take(ds, 10)
	res := RunConsumer(ds)

	AssertEqualHeaders(t, res.Header, []Column{{"a", TypeI32}, {"b", TypeI32}})
}
