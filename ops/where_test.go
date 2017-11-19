package ops

import (
	. "github.com/lionell/aqua/column"
	. "github.com/lionell/aqua/data"
	. "github.com/lionell/aqua/testutil"
	"testing"
	"time"
)

// TODO(lionell): Test for errors.

func TestWhere(t *testing.T) {
	tests := []struct {
		desc string
		in   Table
		cond Condition
		exp  []Row
	}{
		{
			desc: "take first 1 row",
			in: MakeTable([]Column{{"a", TypeI32}}, []Row{
				{I32(1)},
				{I32(2)},
			}),
			cond: NewTrueConditionWithLimit(1),
			exp: []Row{
				{I32(1)},
			},
		},
		{
			desc: "always false condition",
			in: MakeTable([]Column{{"a", TypeI32}}, []Row{
				{I32(1)},
				{I32(2)},
			}),
			cond: NewTrueConditionWithLimit(0),
			exp:  nil,
		},
		{
			desc: "correctly maps header",
			in: MakeTable([]Column{{"a", TypeI32}, {"b", TypeI32}}, []Row{
				{I32(2), I32(8)},
				{I32(1), I32(7)},
				{I32(2), I32(9)},
			}),
			cond: NewOddCondition("a"),
			exp: []Row{
				{I32(1), I32(7)},
			},
		},
	}
	for _, ts := range tests {
		ds := StartProducer(ts.in)
		ds, err := Where(ds, ts.cond)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		res := RunConsumer(ds)
		AssertEqualRowsInOrder(t, res.Rows, ts.exp)
	}
}

func TestWhereCanStopWhileStreamingResults(t *testing.T) {
	in := MakeTable([]Column{{"a", TypeI32}}, []Row{
		{I32(1)},
	})
	exp := []Row{
		{I32(1)},
		{I32(1)},
	}

	ds := StartLoopingProducer(in)
	ds, err := Where(ds, NewTrueConditionWithLimit(5))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	res := RunConsumerWithLimit(ds, 2)

	AssertEqualRowsInOrder(t, res.Rows, exp)
}

func TestWhereCanStopWhileWaitingForInput(t *testing.T) {
	t.Parallel()
	ds := StartBlockingProducer([]Column{{"a", TypeI32}})
	ds, err := Where(ds, NewTrueConditionWithLimit(5))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	RunConsumerWithTimeout(ds, time.Microsecond*50)
}

func TestWherePreservesHeader(t *testing.T) {
	ds := StartProducer(MakeTable([]Column{{"a", TypeI32}}, nil))
	ds, err := Where(ds, NewOddCondition("a"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	res := RunConsumer(ds)

	AssertEqualHeaders(t, res.Header, []Column{{"a", TypeI32}})
}

func TestWhereReturnsErrorWhenCantVerifyCondition(t *testing.T) {
	ds := StartBlockingProducer([]Column{{"a", TypeI32}})
	ds, err := Where(ds, WrongCondition{})
	if err == nil {
		t.Fatalf("Error expected")
	}
}
