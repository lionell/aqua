package ops

import (
	. "github.com/lionell/aqua/data"
	. "github.com/lionell/aqua/testutil"
	"testing"
)

func TestTakeStopsWhenSourceIsEmpty(t *testing.T) {
	ds := StartProducer(MakeTable([]string{"a"}, nil))
	ds = Take(ds, 10)
	res := RunConsumer(ds)

	AssertEqualRowsInOrder(t, res.Rows, nil)
}

func TestTakeWhenRowsLessThanLimit(t *testing.T) {
	in := MakeTable([]string{"a", "b"}, []Row{
		{I32(1), I32(2)},
	})

	ds := StartProducer(in)
	ds = Take(ds, 10)
	res := RunConsumer(ds)

	AssertEqualRowsInOrder(t, res.Rows, in.Rows)
}

func TestTakeWhenDataSizeEqualsToLimit(t *testing.T) {
	in := MakeTable([]string{"a", "b"}, []Row{
		{I32(1), I32(2)},
		{I32(3), I32(4)},
	})

	ds := StartProducer(in)
	ds = Take(ds, 2)
	res := RunConsumer(ds)

	AssertEqualRowsInOrder(t, res.Rows, in.Rows)
}

func TestTakeWhenRowsMoreThanLimit(t *testing.T) {
	in := MakeTable([]string{"a", "b"}, []Row{
		{I32(1), I32(2)},
		{I32(3), I32(4)},
	})

	ds := StartProducer(in)
	ds = Take(ds, 1)
	res := RunConsumer(ds)

	AssertEqualRowsInOrder(t, res.Rows, in.Rows[:1])
}

func TestTakeCanStop(t *testing.T) {
	in := MakeTable([]string{"a", "b"}, []Row{
		{I32(1), I32(2)},
		{I32(4), I32(8)},
	})

	ds := StartInfiniteProducer(in)
	ds = Take(ds, 10)
	res := RunConsumerWithLimit(ds, 1)

	AssertEqualRowsInOrder(t, res.Rows, in.Rows[:1])
}

func TestTakePreservesHeader(t *testing.T) {
	ds := StartProducer(MakeTable([]string{"a", "b"}, nil))
	ds = Take(ds, 10)
	res := RunConsumer(ds)

	AssertEqualHeaders(t, res.Header, []string{"a", "b"})
}
