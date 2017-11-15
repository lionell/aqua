package ops

import (
	. "github.com/lionell/aqua/data"
	. "github.com/lionell/aqua/testutils"
	"testing"
)

func TestDistinctWithEmptySource(t *testing.T) {
	ds := StartProducer(nil)
	ds = Distinct(ds)
	res := RunConsumer(ds)

	AssertEquals(t, res, nil)
}

func TestDistinctWhenRowsAreDifferent(t *testing.T) {
	rows := []Row{
		{I32(1), I32(2)},
		{I32(3), I32(4)},
	}

	ds := StartProducer(rows)
	ds = Distinct(ds)
	res := RunConsumer(ds)

	AssertEquals(t, res, rows)
}

func TestDistinctWithEqualRows(t *testing.T) {
	rows := []Row{
		{I32(1), I32(2)},
		{I32(3), I32(4)},
		{I32(1), I32(2)},
	}

	ds := StartProducer(rows)
	ds = Distinct(ds)
	res := RunConsumer(ds)

	AssertEquals(t, res, rows[:2])
}

func TestDistinctCanStop(t *testing.T) {
	rows := []Row{
		{I32(1), I32(2)},
		{I32(3), I32(4)},
	}
	exp := []Row{
		{I32(1), I32(2)},
	}

	ds := StartInfiniteProducer(rows)
	ds = Distinct(ds)
	res := RunConsumerWithLimit(ds, 1)

	AssertEquals(t, res, exp)
}
