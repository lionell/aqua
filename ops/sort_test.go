package ops

import (
	. "github.com/lionell/aqua/column"
	. "github.com/lionell/aqua/data"
	. "github.com/lionell/aqua/testutil"
	"testing"
	"time"
)

func TestSortWithEmptySource(t *testing.T) {
	ds := StartProducer(nil)
	ds = Sort(ds, nil)
	_, res := RunConsumer(ds)

	AssertEqualRows(t, res, nil)
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

	ds := StartProducer(rows, "a", "b")
	ds = Sort(ds, []SortBy{{"a", OrderAsc}})
	_, res := RunConsumer(ds)

	AssertEqualRows(t, res, exp)
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

	ds := StartProducer(rows, "a", "b", "c")
	ds = Sort(ds, []SortBy{{"a", OrderDesc}, {"b", OrderAsc}})
	_, res := RunConsumer(ds)

	AssertEqualRows(t, res, exp)
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

	ds := StartProducer(rows, "a", "b")
	ds = Sort(ds, []SortBy{{"a", OrderAsc}})
	_, res := RunConsumer(ds)

	AssertEqualRows(t, res, exp)
}

func TestSortCanStopOnReceivingData(t *testing.T) {
	rows := []Row{
		{I32(1), I32(2)},
	}

	ds := StartInfiniteProducer(rows, "a", "b")
	ds = Sort(ds, []SortBy{{"a", OrderAsc}})
	RunConsumerWithTimeout(ds, time.Millisecond*100)
}

func TestSortCanStopOnSendingResults(t *testing.T) {
	rows := []Row{
		{I32(1), I32(2)},
		{I32(7), I32(7)},
		{I32(0), I32(1)},
		{I32(1), I32(2)},
	}

	ds := StartProducer(rows, "a", "b")
	ds = Sort(ds, []SortBy{{"a", OrderAsc}})
	res := RunConsumerWithLimit(ds, 1)

	AssertEqualRows(t, res, rows[2:3])
}

func TestSortPreservesHeader(t *testing.T) {
	ds := StartProducer(nil, "a", "b")
	ds = Sort(ds, []SortBy{{"a", OrderAsc}})
	h, _ := RunConsumer(ds)

	AssertEqualHeaders(t, h, []string{"a", "b"})
}
