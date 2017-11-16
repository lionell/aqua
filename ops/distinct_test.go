package ops

import (
	. "github.com/lionell/aqua/data"
	. "github.com/lionell/aqua/testutil"
	"testing"
)

func TestDistinctWithEmptySource(t *testing.T) {
	ds := StartProducer(NewTable([]string{"a"}, nil))
	ds = Distinct(ds)
	res := RunConsumer(ds)

	AssertEqualRows(t, res.Rows, nil)
}

func TestDistinctWhenRowsAreDifferent(t *testing.T) {
	in := NewTable([]string{"a", "b"}, []Row{
		{I32(1), I32(2)},
		{I32(3), I32(4)},
	})

	ds := StartProducer(in)
	ds = Distinct(ds)
	res := RunConsumer(ds)

	AssertEqualRows(t, res.Rows, in.Rows)
}

func TestDistinctWithEqualRows(t *testing.T) {
	in := NewTable([]string{"a", "b"}, []Row{
		{I32(1), I32(2)},
		{I32(3), I32(4)},
		{I32(1), I32(2)},
	})

	ds := StartProducer(in)
	ds = Distinct(ds)
	res := RunConsumer(ds)

	AssertEqualRows(t, res.Rows, in.Rows[:2])
}

func TestDistinctCanStop(t *testing.T) {
	in := NewTable([]string{"a", "b"}, []Row{
		{I32(1), I32(2)},
		{I32(3), I32(4)},
	})
	exp := []Row{
		{I32(1), I32(2)},
	}

	ds := StartInfiniteProducer(in)
	ds = Distinct(ds)
	res := RunConsumerWithLimit(ds, 1)

	AssertEqualRows(t, res.Rows, exp)
}

func TestDistinctPreservesHeader(t *testing.T) {
	ds := StartProducer(NewTable([]string{"a", "b"}, nil))
	ds = Distinct(ds)
	res := RunConsumer(ds)

	AssertEqualHeaders(t, res.Header, []string{"a", "b"})
}
