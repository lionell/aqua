package ops

import (
	. "github.com/lionell/aqua/data"
	. "github.com/lionell/aqua/testutil"
	"testing"
)

// TODO(lionell): Test for errors.

func TestWhereTakeFirstRows(t *testing.T) {
	in := MakeTable([]string{"a"}, []Row{
		{I32(1)},
		{I32(2)},
	})

	ds := StartProducer(in)
	ds = Where(ds, NewTrueConditionWithLimit(1))
	res := RunConsumer(ds)

	AssertEqualRowsInOrder(t, res.Rows, in.Rows[:1])
}

func TestWhereWithAlwaysFalseCondition(t *testing.T) {
	in := MakeTable([]string{"a"}, []Row{
		{I32(1)},
		{I32(2)},
	})

	ds := StartProducer(in)
	ds = Where(ds, NewTrueConditionWithLimit(0))
	res := RunConsumer(ds)

	AssertEqualRowsInOrder(t, res.Rows, nil)
}

func TestWhereMapsHeaderCorrectly(t *testing.T) {
	in := MakeTable([]string{"a", "b"}, []Row{
		{I32(2), I32(8)},
		{I32(1), I32(7)},
		{I32(2), I32(9)},
	})

	ds := StartProducer(in)
	ds = Where(ds, NewOddCondition("a"))
	res := RunConsumer(ds)

	AssertEqualRowsInOrder(t, res.Rows, in.Rows[1:2])
}

func TestWhereCanStop(t *testing.T) {
	in := MakeTable([]string{"a"}, []Row{
		{I32(1)},
	})
	exp := []Row{
		{I32(1)},
		{I32(1)},
	}

	ds := StartInfiniteProducer(in)
	ds = Where(ds, NewTrueConditionWithLimit(5))
	res := RunConsumerWithLimit(ds, 2)

	AssertEqualRowsInOrder(t, res.Rows, exp)
}

func TestWherePreservesHeader(t *testing.T) {
	ds := StartProducer(MakeTable([]string{"a", "b"}, nil))
	ds = Where(ds, NewOddCondition("a"))
	res := RunConsumer(ds)

	AssertEqualHeaders(t, res.Header, []string{"a", "b"})
}
