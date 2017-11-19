package ops

import (
	. "github.com/lionell/aqua/column"
	. "github.com/lionell/aqua/data"
	. "github.com/lionell/aqua/testutil"
	"testing"
	"time"
)

func TestJoin(t *testing.T) {
	tests := []struct {
		desc          string
		in1, in2, exp Table
		conds         []JoinCondition
		joinType      JoinType
	}{
		{
			desc: "inner join with no matched rows",
			in1: MakeTable([]Column{{"a", TypeI32}}, []Row{
				{I32(1)},
			}),
			in2: MakeTable([]Column{{"b", TypeI32}}, []Row{
				{I32(2)},
			}),
			conds:    []JoinCondition{{"a", "b"}},
			joinType: JoinInner,
			exp:      MakeTable([]Column{{"a", TypeI32}, {"b", TypeI32}}, nil),
		},
		{
			desc: "inner join",
			in1: MakeTable([]Column{{"a", TypeI32}, {"b", TypeI32}}, []Row{
				{I32(1), I32(2)},
				{I32(1), I32(1)},
				{I32(3), I32(4)},
				{I32(4), I32(7)},
			}),
			in2: MakeTable([]Column{{"c", TypeI32}, {"d", TypeI32}}, []Row{
				{I32(7), I32(3)},
				{I32(9), I32(3)},
				{I32(5), I32(1)},
				{I32(8), I32(7)},
			}),
			conds:    []JoinCondition{{"a", "d"}},
			joinType: JoinInner,
			exp: MakeTable([]Column{{"a", TypeI32}, {"b", TypeI32}, {"c", TypeI32}, {"d", TypeI32}}, []Row{
				{I32(1), I32(2), I32(5), I32(1)},
				{I32(1), I32(1), I32(5), I32(1)},
				{I32(3), I32(4), I32(7), I32(3)},
				{I32(3), I32(4), I32(9), I32(3)},
			}),
		},
		{
			desc: "left join",
			in1: MakeTable([]Column{{"a", TypeI32}, {"b", TypeI32}}, []Row{
				{I32(1), I32(2)},
				{I32(1), I32(1)},
				{I32(3), I32(4)},
				{I32(4), I32(7)},
			}),
			in2: MakeTable([]Column{{"c", TypeI32}, {"d", TypeI32}}, []Row{
				{I32(7), I32(3)},
				{I32(9), I32(3)},
				{I32(5), I32(1)},
				{I32(8), I32(7)},
			}),
			conds:    []JoinCondition{{"a", "d"}},
			joinType: JoinLeft,
			exp: MakeTable([]Column{{"a", TypeI32}, {"b", TypeI32}, {"c", TypeI32}, {"d", TypeI32}}, []Row{
				{I32(1), I32(2), I32(5), I32(1)},
				{I32(1), I32(1), I32(5), I32(1)},
				{I32(3), I32(4), I32(7), I32(3)},
				{I32(3), I32(4), I32(9), I32(3)},
				{I32(4), I32(7), None{}, None{}},
			}),
		},
		{
			desc: "right join",
			in1: MakeTable([]Column{{"a", TypeI32}, {"b", TypeI32}}, []Row{
				{I32(1), I32(2)},
				{I32(1), I32(1)},
				{I32(3), I32(4)},
				{I32(4), I32(7)},
			}),
			in2: MakeTable([]Column{{"c", TypeI32}, {"d", TypeI32}}, []Row{
				{I32(7), I32(3)},
				{I32(9), I32(3)},
				{I32(5), I32(1)},
				{I32(8), I32(7)},
			}),
			conds:    []JoinCondition{{"a", "d"}},
			joinType: JoinRight,
			exp: MakeTable([]Column{{"a", TypeI32}, {"b", TypeI32}, {"c", TypeI32}, {"d", TypeI32}}, []Row{
				{I32(1), I32(2), I32(5), I32(1)},
				{I32(1), I32(1), I32(5), I32(1)},
				{I32(3), I32(4), I32(7), I32(3)},
				{I32(3), I32(4), I32(9), I32(3)},
				{None{}, None{}, I32(8), I32(7)},
			}),
		},
		{
			desc: "full join",
			in1: MakeTable([]Column{{"a", TypeI32}, {"b", TypeI32}}, []Row{
				{I32(1), I32(2)},
				{I32(1), I32(1)},
				{I32(3), I32(4)},
				{I32(4), I32(7)},
			}),
			in2: MakeTable([]Column{{"c", TypeI32}, {"d", TypeI32}}, []Row{
				{I32(7), I32(3)},
				{I32(9), I32(3)},
				{I32(5), I32(1)},
				{I32(8), I32(7)},
			}),
			conds:    []JoinCondition{{"a", "d"}},
			joinType: JoinFull,
			exp: MakeTable([]Column{{"a", TypeI32}, {"b", TypeI32}, {"c", TypeI32}, {"d", TypeI32}}, []Row{
				{I32(1), I32(2), I32(5), I32(1)},
				{I32(1), I32(1), I32(5), I32(1)},
				{I32(3), I32(4), I32(7), I32(3)},
				{I32(3), I32(4), I32(9), I32(3)},
				{I32(4), I32(7), None{}, None{}},
				{None{}, None{}, I32(8), I32(7)},
			}),
		},
		{
			desc: "join on multiple columns",
			in1: MakeTable([]Column{{"a", TypeI32}, {"b", TypeI32}, {"c", TypeI32}}, []Row{
				{I32(1), I32(2), I32(4)},
				{I32(2), I32(4), I32(3)},
				{I32(3), I32(8), I32(2)},
				{I32(4), I32(16), I32(1)},
			}),
			in2: MakeTable([]Column{{"d", TypeI32}, {"e", TypeI32}, {"f", TypeI32}}, []Row{
				{I32(1), I32(3), I32(-1)},
				{I32(2), I32(4), I32(-2)},
				{I32(3), I32(7), I32(-3)},
			}),
			conds:    []JoinCondition{{"a", "d"}, {"b", "e"}},
			joinType: JoinInner,
			exp: MakeTable([]Column{
				{"a", TypeI32},
				{"b", TypeI32},
				{"c", TypeI32},
				{"d", TypeI32},
				{"e", TypeI32},
				{"f", TypeI32},
			}, []Row{
				{I32(2), I32(4), I32(3), I32(2), I32(4), I32(-2)},
			}),
		},
	}
	for _, ts := range tests {
		ds1 := StartProducer(ts.in1)
		ds2 := StartProducer(ts.in2)
		ds, err := Join(ds1, ds2, ts.conds, ts.joinType)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		res := RunConsumer(ds)
		AssertEqualTables(t, res, ts.exp)
	}
}

func TestJoinRenameColumnsIfNeeded(t *testing.T) {
	in1 := MakeTable([]Column{{"a", TypeI32}, {"b", TypeI32}}, []Row{
		{I32(1), I32(2)},
	})
	in2 := MakeTable([]Column{{"a", TypeI32}, {"c", TypeI32}}, []Row{
		{I32(1), I32(3)},
	})

	ds1 := StartProducer(in1)
	ds2 := StartProducer(in2)
	ds, err := Join(ds1, ds2, []JoinCondition{{"a", "a"}}, JoinFull)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	res := RunConsumer(ds)

	AssertEqualHeaders(t, res.Header, []Column{{"a", TypeI32}, {"b", TypeI32}, {"$a", TypeI32}, {"c", TypeI32}})
}

func TestJoinCanStopWhileStreamingResults(t *testing.T) {
	in1 := MakeTable([]Column{{"a", TypeI32}, {"b", TypeI32}}, []Row{
		{I32(3), I32(2)},
		{I32(3), I32(7)},
	})
	in2 := MakeTable([]Column{{"c", TypeI32}, {"d", TypeI32}}, []Row{
		{I32(9), I32(3)},
	})

	ds1 := StartProducer(in1)
	ds2 := StartProducer(in2)
	ds, err := Join(ds1, ds2, []JoinCondition{{"a", "d"}}, JoinFull)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	RunConsumerWithLimit(ds, 1)
}

func TestJoinCanStopAfterFullMatchesProcessed(t *testing.T) {
	in1 := MakeTable([]Column{{"a", TypeI32}, {"b", TypeI32}}, []Row{
		{I32(1), I32(2)},
	})
	in2 := MakeTable([]Column{{"c", TypeI32}, {"d", TypeI32}}, []Row{
		{I32(9), I32(3)},
	})

	ds1 := StartProducer(in1)
	ds2 := StartProducer(in2)
	ds, err := Join(ds1, ds2, []JoinCondition{{"a", "d"}}, JoinFull)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	RunConsumerWithLimit(ds, 1)
}

func TestJoinCanStopWhileWaitingForInput(t *testing.T) {
	t.Parallel()
	ds1 := StartBlockingProducer([]Column{{"a", TypeI32}})
	ds2 := StartBlockingProducer([]Column{{"a", TypeI32}})
	ds, err := Join(ds1, ds2, []JoinCondition{{"a", "a"}}, JoinFull)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	RunConsumerWithTimeout(ds, time.Millisecond*50)
}

func TestJoinReturnsErrorWhenCantIndexLeftPartOfCondition(t *testing.T) {
	ds1 := StartBlockingProducer([]Column{{"a", TypeI32}})
	ds2 := StartBlockingProducer([]Column{{"b", TypeI32}})
	_, err := Join(ds1, ds2, []JoinCondition{{"c", "b"}}, JoinFull)
	if err == nil {
		t.Errorf("Error expected")
	}
}

func TestJoinReturnsErrorWhenCantIndexRightPartOfCondition(t *testing.T) {
	ds1 := StartBlockingProducer([]Column{{"a", TypeI32}})
	ds2 := StartBlockingProducer([]Column{{"b", TypeI32}})
	_, err := Join(ds1, ds2, []JoinCondition{{"a", "c"}}, JoinFull)
	if err == nil {
		t.Errorf("Error expected")
	}
}
