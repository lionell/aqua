package ops

import (
	. "github.com/lionell/aqua/data"
	. "github.com/lionell/aqua/testutil"
	"testing"
)

func TestTakeStopsWhenSourceIsEmpty(t *testing.T) {
	ds := StartProducer(nil)
	ds = Take(ds, 10)
	res := RunConsumer(ds)

	AssertEquals(t, res, nil)
}

func TestTakeWhenRowsLessThanLimit(t *testing.T) {
	rows := []Row{
		{I32(1), I32(2)},
	}

	ds := StartProducer(rows)
	ds = Take(ds, 10)
	res := RunConsumer(ds)

	AssertEquals(t, res, rows)
}

func TestTakeWhenDataSizeEqualsToLimit(t *testing.T) {
	rows := []Row{
		{I32(1), I32(2)},
		{I32(3), I32(4)},
	}

	ds := StartProducer(rows)
	ds = Take(ds, 2)
	res := RunConsumer(ds)

	AssertEquals(t, res, rows)
}

func TestTakeWhenRowsMoreThanLimit(t *testing.T) {
	rows := []Row{
		{I32(1), I32(2)},
		{I32(3), I32(4)},
	}

	ds := StartProducer(rows)
	ds = Take(ds, 1)
	res := RunConsumer(ds)

	AssertEquals(t, res, rows[:1])
}

func TestTakeCanStop(t *testing.T) {
	rows := []Row{
		{I32(1), I32(2)},
		{I32(4), I32(8)},
	}

	ds := StartInfiniteProducer(rows)
	ds = Take(ds, 10)
	res := RunConsumerWithLimit(ds, 1)

	AssertEquals(t, res, rows[:1])
}
