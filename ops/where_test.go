package ops

import (
	. "github.com/lionell/aqua/data"
	. "github.com/lionell/aqua/testutil"
	"testing"
)

// TODO(lionell): Test for errors
// TODO(lionell): Test header

func TestWhereTakeFirstRows(t *testing.T) {
	rows := []Row{
		{I32(1)},
		{I32(2)},
	}

	ds := StartProducer(rows, "a")
	ds = Where(ds, NewTrueConditionWithLimit(1))
	res := RunConsumer(ds)

	AssertEquals(t, res, rows[:1])
}

func TestWhereWithAlwaysFalseCondition(t *testing.T) {
	rows := []Row{
		{I32(1)},
		{I32(2)},
	}

	ds := StartProducer(rows, "a")
	ds = Where(ds, NewTrueConditionWithLimit(0))
	res := RunConsumer(ds)

	AssertEquals(t, res, nil)
}

func TestWhereMapsHeaderCorrectly(t *testing.T) {
	rows := []Row{
		{I32(2), I32(8)},
		{I32(1), I32(7)},
		{I32(2), I32(9)},
	}

	ds := StartProducer(rows, "a", "ignored")
	ds = Where(ds, NewOddCondition("a"))
	res := RunConsumer(ds)

	AssertEquals(t, res, rows[1:2])
}

func TestWhereCanStop(t *testing.T) {
	rows := []Row{
		{I32(1)},
	}
	exp := []Row{
		{I32(1)},
		{I32(1)},
	}

	ds := StartInfiniteProducer(rows, "a")
	ds = Where(ds, NewTrueConditionWithLimit(5))
	res := RunConsumerWithLimit(ds, 2)

	AssertEquals(t, res, exp)
}
