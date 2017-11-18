package ops

import (
	. "github.com/lionell/aqua/column"
	. "github.com/lionell/aqua/data"
	. "github.com/lionell/aqua/testutil"
	"testing"
)

// TODO(lionell): Test for errors.

func TestProjectColumnOnItself(t *testing.T) {
	in := MakeTable([]string{"a"}, []Row{
		{I32(1)},
		{I32(2)},
	})

	ds := StartProducer(in)
	ds = Project(ds, []Definition{{"a", NewSumExpression("a")}})
	res := RunConsumer(ds)

	AssertEqualTablesInOrder(t, res, in)
}

func TestProjectSumOfColumns(t *testing.T) {
	in := MakeTable([]string{"a", "b"}, []Row{
		{I32(1), I32(8)},
		{I32(2), I32(-1)},
	})
	exp := MakeTable([]string{"c"}, []Row{
		{I32(9)},
		{I32(1)},
	})

	ds := StartProducer(in)
	ds = Project(ds, []Definition{{"c", NewSumExpression("a", "b")}})
	res := RunConsumer(ds)

	AssertEqualTablesInOrder(t, res, exp)
}

func TestProjectDoesNotIncludeUnnecessaryColumns(t *testing.T) {
	in := MakeTable([]string{"a", "b"}, []Row{
		{I32(1), I32(8)},
		{I32(2), I32(-1)},
	})
	exp := MakeTable([]string{"c"}, []Row{
		{I32(1)},
		{I32(2)},
	})

	ds := StartProducer(in)
	ds = Project(ds, []Definition{{"c", NewSumExpression("a")}})
	res := RunConsumer(ds)

	AssertEqualTablesInOrder(t, res, exp)
}

func TestProjectCanStop(t *testing.T) {
	in := MakeTable([]string{"a"}, []Row{
		{I32(1)},
	})
	exp := []Row{
		{I32(1)},
		{I32(1)},
	}

	ds := StartInfiniteProducer(in)
	ds = Project(ds, []Definition{{"a", NewSumExpression("a")}})
	res := RunConsumerWithLimit(ds, 2)

	AssertEqualRowsInOrder(t, res.Rows, exp)
}
