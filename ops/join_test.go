package ops

import (
	. "github.com/lionell/aqua/column"
	. "github.com/lionell/aqua/data"
	. "github.com/lionell/aqua/testutil"
	"testing"
)


func TestInnerJoinWhenThereIsNoMatch(t *testing.T) {
	in1 := MakeTable([]string{"a", "b"}, []Row{
		{I32(1), I32(2)},
	})
	in2 := MakeTable([]string{"c", "d"}, []Row{
		{I32(9), I32(3)},
	})

	ds1 := StartProducer(in1)
	ds2 := StartProducer(in2)
	ds := Join(ds1, ds2, []JoinCondition{{"a", "d"}}, JoinInner)
	res := RunConsumer(ds)

	AssertEqualRows(t, res.Rows, nil)
}

func TestInnerJoin(t *testing.T) {
	in1 := MakeTable([]string{"a", "b"}, []Row{
		{I32(1), I32(2)},
		{I32(1), I32(1)},
		{I32(3), I32(4)},
		{I32(4), I32(7)},
	})
	in2 := MakeTable([]string{"c", "d"}, []Row{
		{I32(7), I32(3)},
		{I32(9), I32(3)},
		{I32(5), I32(1)},
		{I32(8), I32(7)},
	})
	exp := MakeTable([]string{"a", "b", "c", "d"}, []Row{
		{I32(1), I32(2), I32(5), I32(1)},
		{I32(1), I32(1), I32(5), I32(1)},
		{I32(3), I32(4), I32(7), I32(3)},
		{I32(3), I32(4), I32(9), I32(3)},
	})

	ds1 := StartProducer(in1)
	ds2 := StartProducer(in2)
	ds := Join(ds1, ds2, []JoinCondition{{"a", "d"}}, JoinInner)
	res := RunConsumer(ds)

	AssertEqualTables(t, res, exp)
}

func TestLeftJoin(t *testing.T) {
	in1 := MakeTable([]string{"a", "b"}, []Row{
		{I32(1), I32(2)},
		{I32(1), I32(1)},
		{I32(3), I32(4)},
		{I32(4), I32(7)},
	})
	in2 := MakeTable([]string{"c", "d"}, []Row{
		{I32(7), I32(3)},
		{I32(9), I32(3)},
		{I32(5), I32(1)},
		{I32(8), I32(7)},
	})
	exp := MakeTable([]string{"a", "b", "c", "d"}, []Row{
		{I32(1), I32(2), I32(5), I32(1)},
		{I32(1), I32(1), I32(5), I32(1)},
		{I32(3), I32(4), I32(7), I32(3)},
		{I32(3), I32(4), I32(9), I32(3)},
		{I32(4), I32(7), None{}, None{}},
	})

	ds1 := StartProducer(in1)
	ds2 := StartProducer(in2)
	ds := Join(ds1, ds2, []JoinCondition{{"a", "d"}}, JoinLeft)
	res := RunConsumer(ds)

	AssertEqualTables(t, res, exp)
}

func TestRightJoin(t *testing.T) {
	in1 := MakeTable([]string{"a", "b"}, []Row{
		{I32(1), I32(2)},
		{I32(1), I32(1)},
		{I32(3), I32(4)},
		{I32(4), I32(7)},
	})
	in2 := MakeTable([]string{"c", "d"}, []Row{
		{I32(7), I32(3)},
		{I32(9), I32(3)},
		{I32(5), I32(1)},
		{I32(8), I32(7)},
	})
	exp := MakeTable([]string{"a", "b", "c", "d"}, []Row{
		{I32(1), I32(2), I32(5), I32(1)},
		{I32(1), I32(1), I32(5), I32(1)},
		{I32(3), I32(4), I32(7), I32(3)},
		{I32(3), I32(4), I32(9), I32(3)},
		{None{}, None{}, I32(8), I32(7)},
	})

	ds1 := StartProducer(in1)
	ds2 := StartProducer(in2)
	ds := Join(ds1, ds2, []JoinCondition{{"a", "d"}}, JoinRight)
	res := RunConsumer(ds)

	AssertEqualTables(t, res, exp)
}

func TestFullJoin(t *testing.T) {
	in1 := MakeTable([]string{"a", "b"}, []Row{
		{I32(1), I32(2)},
		{I32(1), I32(1)},
		{I32(3), I32(4)},
		{I32(4), I32(7)},
	})
	in2 := MakeTable([]string{"c", "d"}, []Row{
		{I32(7), I32(3)},
		{I32(9), I32(3)},
		{I32(5), I32(1)},
		{I32(8), I32(7)},
	})
	exp := MakeTable([]string{"a", "b", "c", "d"}, []Row{
		{I32(1), I32(2), I32(5), I32(1)},
		{I32(1), I32(1), I32(5), I32(1)},
		{I32(3), I32(4), I32(7), I32(3)},
		{I32(3), I32(4), I32(9), I32(3)},
		{I32(4), I32(7), None{}, None{}},
		{None{}, None{}, I32(8), I32(7)},
	})

	ds1 := StartProducer(in1)
	ds2 := StartProducer(in2)
	ds := Join(ds1, ds2, []JoinCondition{{"a", "d"}}, JoinFull)
	res := RunConsumer(ds)

	AssertEqualTables(t, res, exp)
}

func TestJoinRenameColumnsIfNeeded(t *testing.T) {
	in1 := MakeTable([]string{"a", "b"}, []Row{
		{I32(1), I32(2)},
	})
	in2 := MakeTable([]string{"a", "c"}, []Row{
		{I32(1), I32(3)},
	})

	ds1 := StartProducer(in1)
	ds2 := StartProducer(in2)
	ds := Join(ds1, ds2, []JoinCondition{{"a", "a"}}, JoinFull)
	res := RunConsumer(ds)

	AssertEqualHeaders(t, res.Header, []string{"a", "b", "$a", "c"})
}

func TestJoinCanStop(t *testing.T) {
	in1 := MakeTable([]string{"a", "b"}, []Row{
		{I32(3), I32(2)},
		{I32(3), I32(7)},
	})
	in2 := MakeTable([]string{"c", "d"}, []Row{
		{I32(9), I32(3)},
	})

	ds1 := StartProducer(in1)
	ds2 := StartProducer(in2)
	ds := Join(ds1, ds2, []JoinCondition{{"a", "d"}}, JoinFull)
	RunConsumerWithLimit(ds, 1)
}

func TestJoinCanStopAfterMatchesProcessed(t *testing.T) {
	in1 := MakeTable([]string{"a", "b"}, []Row{
		{I32(1), I32(2)},
	})
	in2 := MakeTable([]string{"c", "d"}, []Row{
		{I32(9), I32(3)},
	})

	ds1 := StartProducer(in1)
	ds2 := StartProducer(in2)
	ds := Join(ds1, ds2, []JoinCondition{{"a", "d"}}, JoinFull)
	RunConsumerWithLimit(ds, 1)
}
