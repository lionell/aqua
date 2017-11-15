package ops

import (
	. "github.com/lionell/aqua/data"
	. "github.com/lionell/aqua/testutil"
	"testing"
)

func TestUnionWhenFirstSourceIsEmpty(t *testing.T) {
	rows := []Row{
		{I32(1), I32(2)},
		{I32(3), I32(4)},
	}

	ds1 := StartProducer(nil)
	ds2 := StartProducer(rows)
	ds := Union(ds1, ds2)
	res := RunConsumer(ds)

	AssertEquals(t, res, rows)
}

func TestUnionWhenSecondSourceIsEmpty(t *testing.T) {
	rows := []Row{
		{I32(1), I32(2)},
		{I32(3), I32(4)},
	}

	ds1 := StartProducer(rows)
	ds2 := StartProducer(nil)
	ds := Union(ds1, ds2)
	res := RunConsumer(ds)

	AssertEquals(t, res, rows)
}

func TestUnionWhenBothSourcesAreEmpty(t *testing.T) {
	ds1 := StartProducer(nil)
	ds2 := StartProducer(nil)
	ds := Union(ds1, ds2)
	res := RunConsumer(ds)

	AssertEquals(t, res, nil)
}

func TestUnionReturnsDataInCorrectOrder(t *testing.T) {
	rows1 := []Row{
		{I32(1), I32(2)},
		{I32(3), I32(4)},
	}
	rows2 := []Row{
		{I32(5), I32(6)},
		{I32(7), I32(8)},
	}
	var exp []Row
	exp = append(exp, rows1...)
	exp = append(exp, rows2...)

	ds1 := StartProducer(rows1)
	ds2 := StartProducer(rows2)
	ds := Union(ds1, ds2)
	res := RunConsumer(ds)

	AssertEquals(t, res, exp)
}

func TestUnionCanStopWhileProcessingFirstSource(t *testing.T) {
	rows := []Row{
		{I32(5), I32(6)},
		{I32(7), I32(8)},
	}

	ds1 := StartInfiniteProducer(rows)
	ds2 := StartProducer(nil)
	ds := Union(ds1, ds2)
	res := RunConsumerWithLimit(ds, 2)

	AssertEquals(t, res, rows)
}

func TestUnionCanStopWhileProcessingSecondSource(t *testing.T) {
	rows := []Row{
		{I32(5), I32(6)},
		{I32(7), I32(8)},
	}

	ds1 := StartProducer(nil)
	ds2 := StartInfiniteProducer(rows)
	ds := Union(ds1, ds2)
	res := RunConsumerWithLimit(ds, 2)

	AssertEquals(t, res, rows)
}
