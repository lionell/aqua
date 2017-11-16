package ops

import (
	. "github.com/lionell/aqua/data"
	. "github.com/lionell/aqua/testutil"
	"testing"
)

// TODO(lionell): Test union throws error when headers don't match.

func TestUnionWhenFirstSourceIsEmpty(t *testing.T) {
	in := NewTable([]string{"a", "b"}, []Row{
		{I32(1), I32(2)},
		{I32(3), I32(4)},
	})

	ds1 := StartProducer(NewTable([]string{"a", "b"}, nil))
	ds2 := StartProducer(in)
	ds := Union(ds1, ds2)
	res := RunConsumer(ds)

	AssertEqualRows(t, res.Rows, in.Rows)
}

func TestUnionWhenSecondSourceIsEmpty(t *testing.T) {
	in := NewTable([]string{"a", "b"}, []Row{
		{I32(1), I32(2)},
		{I32(3), I32(4)},
	})

	ds1 := StartProducer(in)
	ds2 := StartProducer(NewTable([]string{"a", "b"}, nil))
	ds := Union(ds1, ds2)
	res := RunConsumer(ds)

	AssertEqualRows(t, res.Rows, in.Rows)
}

func TestUnionWhenBothSourcesAreEmpty(t *testing.T) {
	ds1 := StartProducer(NewTable([]string{"a"}, nil))
	ds2 := StartProducer(NewTable([]string{"a"}, nil))
	ds := Union(ds1, ds2)
	res := RunConsumer(ds)

	AssertEqualRows(t, res.Rows, nil)
}

func TestUnionReturnsDataInCorrectOrder(t *testing.T) {
	in1 := NewTable([]string{"a", "b"}, []Row{
		{I32(1), I32(2)},
		{I32(3), I32(4)},
	})
	in2 := NewTable([]string{"a", "b"}, []Row{
		{I32(5), I32(6)},
		{I32(7), I32(8)},
	})
	var exp []Row
	exp = append(exp, in1.Rows...)
	exp = append(exp, in2.Rows...)

	ds1 := StartProducer(in1)
	ds2 := StartProducer(in2)
	ds := Union(ds1, ds2)
	res := RunConsumer(ds)

	AssertEqualRows(t, res.Rows, exp)
}

func TestUnionCanStopWhileProcessingFirstSource(t *testing.T) {
	in := NewTable([]string{"a", "b"}, []Row{
		{I32(5), I32(6)},
		{I32(7), I32(8)},
	})

	ds1 := StartInfiniteProducer(in)
	ds2 := StartProducer(NewTable([]string{"a", "b"}, nil))
	ds := Union(ds1, ds2)
	res := RunConsumerWithLimit(ds, 2)

	AssertEqualRows(t, res.Rows, in.Rows)
}

func TestUnionCanStopWhileProcessingSecondSource(t *testing.T) {
	in := NewTable([]string{"a", "b"}, []Row{
		{I32(5), I32(6)},
		{I32(7), I32(8)},
	})

	ds1 := StartProducer(NewTable([]string{"a", "b"}, nil))
	ds2 := StartInfiniteProducer(in)
	ds := Union(ds1, ds2)
	res := RunConsumerWithLimit(ds, 2)

	AssertEqualRows(t, res.Rows, in.Rows)
}

func TestUnionPreservesHeader(t *testing.T) {
	ds1 := StartProducer(NewTable([]string{"a", "b"}, nil))
	ds2 := StartProducer(NewTable([]string{"a", "b"}, nil))
	ds := Union(ds1, ds2)
	res := RunConsumer(ds)

	AssertEqualHeaders(t, res.Header, []string{"a", "b"})
}
