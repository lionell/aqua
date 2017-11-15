package ops

import (
	"github.com/lionell/aqua/column"
	. "github.com/lionell/aqua/data"
	. "github.com/lionell/aqua/testutils"
	"testing"
	"time"
)

func TestSortWithoutOrder(t *testing.T) {
	rows := []Row{
		{I32(1), I32(2)},
		{I32(3), I32(4)},
	}

	ds := StartProducer(rows)
	ds = Sort(ds, nil)
	res := RunConsumer(ds)

	AssertEquals(t, res, rows)
}

func TestSortWithEmptySource(t *testing.T) {
	ds := StartProducer(nil)
	ds = Sort(ds, nil)
	res := RunConsumer(ds)

	AssertEquals(t, res, nil)
}

func TestSortByOneColumn(t *testing.T) {
	rows := []Row{
		{I32(1), I32(2)},
		{I32(7), I32(7)},
		{I32(3), I32(4)},
		{I32(0), I32(1)},
	}
	exp := []Row{
		{I32(0), I32(1)},
		{I32(1), I32(2)},
		{I32(3), I32(4)},
		{I32(7), I32(7)},
	}

	ds := StartProducer(rows)
	ds = Sort(ds, []column.Order{{0, column.ASC}})
	res := RunConsumer(ds)

	AssertEquals(t, res, exp)
}

func TestSortByTwoColumns(t *testing.T) {
	rows := []Row{
		{I32(3), I32(8), I32(4)},
		{I32(1), I32(1), I32(-21)},
		{I32(3), I32(2), I32(-1)},
		{I32(3), I32(4), I32(14)},
		{I32(7), I32(7), I32(3)},
	}
	exp := []Row{
		{I32(7), I32(7), I32(3)},
		{I32(3), I32(2), I32(-1)},
		{I32(3), I32(4), I32(14)},
		{I32(3), I32(8), I32(4)},
		{I32(1), I32(1), I32(-21)},
	}

	ds := StartProducer(rows)
	ds = Sort(ds, []column.Order{{0, column.DESC}, {1, column.ASC}})
	res := RunConsumer(ds)

	AssertEquals(t, res, exp)
}

func TestSortWithEqualRows(t *testing.T) {
	rows := []Row{
		{I32(1), I32(2)},
		{I32(7), I32(7)},
		{I32(0), I32(1)},
		{I32(1), I32(2)},
	}
	exp := []Row{
		{I32(0), I32(1)},
		{I32(1), I32(2)},
		{I32(1), I32(2)},
		{I32(7), I32(7)},
	}

	ds := StartProducer(rows)
	ds = Sort(ds, []column.Order{{0, column.ASC}})
	res := RunConsumer(ds)

	AssertEquals(t, res, exp)
}

func TestSortCanStopOnReceivingData(t *testing.T) {
	rows := []Row{
		{I32(1), I32(2)},
	}

	ds := StartInfiniteProducer(rows)
	ds = Sort(ds, []column.Order{{0, column.ASC}})
	RunConsumerWithTimeout(ds, time.Millisecond*100)
}

func TestSortCanStopOnSendingResults(t *testing.T) {
	rows := []Row{
		{I32(1), I32(2)},
		{I32(7), I32(7)},
		{I32(0), I32(1)},
		{I32(1), I32(2)},
	}

	ds := StartProducer(rows)
	ds = Sort(ds, []column.Order{{0, column.ASC}})
	RunConsumerWithLimit(ds, 2)
}