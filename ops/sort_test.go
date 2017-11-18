package ops

import (
	. "github.com/lionell/aqua/column"
	. "github.com/lionell/aqua/data"
	. "github.com/lionell/aqua/testutil"
	"testing"
	"time"
)

func TestSortWithEmptySource(t *testing.T) {
	ds := StartProducer(MakeTable([]string{"a"}, nil))
	ds = Sort(ds, nil)
	res := RunConsumer(ds)

	AssertEqualRowsInOrder(t, res.Rows, nil)
}

func TestSortByOneColumn(t *testing.T) {
	in := MakeTable([]string{"a", "b"}, []Row{
		{I32(1), I32(2)},
		{I32(7), I32(7)},
		{I32(3), I32(4)},
		{I32(0), I32(1)},
	})
	exp := []Row{
		{I32(0), I32(1)},
		{I32(1), I32(2)},
		{I32(3), I32(4)},
		{I32(7), I32(7)},
	}

	ds := StartProducer(in)
	ds = Sort(ds, []SortingOrder{{"a", OrderAsc}})
	res := RunConsumer(ds)

	AssertEqualRowsInOrder(t, res.Rows, exp)
}

func TestSortByTwoColumns(t *testing.T) {
	in := MakeTable([]string{"a", "b", "c"}, []Row{
		{I32(3), I32(8), I32(4)},
		{I32(1), I32(1), I32(-21)},
		{I32(3), I32(2), I32(-1)},
		{I32(3), I32(4), I32(14)},
		{I32(7), I32(7), I32(3)},
	})
	exp := []Row{
		{I32(7), I32(7), I32(3)},
		{I32(3), I32(2), I32(-1)},
		{I32(3), I32(4), I32(14)},
		{I32(3), I32(8), I32(4)},
		{I32(1), I32(1), I32(-21)},
	}

	ds := StartProducer(in)
	ds = Sort(ds, []SortingOrder{{"a", OrderDesc}, {"b", OrderAsc}})
	res := RunConsumer(ds)

	AssertEqualRowsInOrder(t, res.Rows, exp)
}

func TestSortWithEqualRows(t *testing.T) {
	in := MakeTable([]string{"a", "b"}, []Row{
		{I32(1), I32(2)},
		{I32(7), I32(7)},
		{I32(0), I32(1)},
		{I32(1), I32(2)},
	})
	exp := []Row{
		{I32(0), I32(1)},
		{I32(1), I32(2)},
		{I32(1), I32(2)},
		{I32(7), I32(7)},
	}

	ds := StartProducer(in)
	ds = Sort(ds, []SortingOrder{{"a", OrderAsc}})
	res := RunConsumer(ds)

	AssertEqualRowsInOrder(t, res.Rows, exp)
}

func TestSortCanStopOnReceivingData(t *testing.T) {
	in := MakeTable([]string{"a"}, []Row{
		{I32(1)},
	})

	ds := StartInfiniteProducer(in)
	ds = Sort(ds, []SortingOrder{{"a", OrderAsc}})
	RunConsumerWithTimeout(ds, time.Millisecond*100)
}

func TestSortCanStopOnSendingResults(t *testing.T) {
	in := MakeTable([]string{"a", "b"}, []Row{
		{I32(1), I32(2)},
		{I32(7), I32(7)},
		{I32(0), I32(1)},
		{I32(1), I32(2)},
	})

	ds := StartProducer(in)
	ds = Sort(ds, []SortingOrder{{"a", OrderAsc}})
	res := RunConsumerWithLimit(ds, 1)

	AssertEqualRowsInOrder(t, res.Rows, in.Rows[2:3])
}

func TestSortPreservesHeader(t *testing.T) {
	ds := StartProducer(MakeTable([]string{"a", "b"}, nil))
	ds = Sort(ds, []SortingOrder{{"a", OrderAsc}})
	res := RunConsumer(ds)

	AssertEqualHeaders(t, res.Header, []string{"a", "b"})
}
