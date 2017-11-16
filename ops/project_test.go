package ops

import (
	. "github.com/lionell/aqua/column"
	. "github.com/lionell/aqua/data"
	. "github.com/lionell/aqua/testutil"
	"testing"
)

// TODO(lionell): Test for errors.

func TestProjectColumnOnItself(t *testing.T) {
	rows := []Row{
		{I32(1)},
		{I32(2)},
	}

	ds := StartProducer(rows, "a")
	ds = Project(ds, []Definition{{"a", NewSumExpression("a")}})
	_, res := RunConsumer(ds)

	AssertEqualRows(t, res, rows)
}

func TestProjectSumOfColumns(t *testing.T) {
	rows := []Row{
		{I32(1), I32(8)},
		{I32(2), I32(-1)},
	}
	exp := []Row{
		{I32(9)},
		{I32(1)},
	}

	ds := StartProducer(rows, "a", "b")
	ds = Project(ds, []Definition{{"c", NewSumExpression("a", "b")}})
	_, res := RunConsumer(ds)

	AssertEqualRows(t, res, exp)
}

func TestProjectDoesNotIncludeUnnecessaryColumns(t *testing.T) {
	rows := []Row{
		{I32(1), I32(8)},
		{I32(2), I32(-1)},
	}
	exp := []Row{
		{I32(1)},
		{I32(2)},
	}

	ds := StartProducer(rows, "a", "b")
	ds = Project(ds, []Definition{{"c", NewSumExpression("a")}})
	_, res := RunConsumer(ds)

	AssertEqualRows(t, res, exp)
}

func TestProjectCanStop(t *testing.T) {
	rows := []Row{
		{I32(1)},
	}
	exp := []Row{
		{I32(1)},
		{I32(1)},
	}

	ds := StartInfiniteProducer(rows, "a")
	ds = Project(ds, []Definition{{"a", NewSumExpression("a")}})
	res := RunConsumerWithLimit(ds, 2)

	AssertEqualRows(t, res, exp)
}

func TestProjectPreservesHeader(t *testing.T) {
	ds := StartProducer(nil, "a", "b")
	ds = Project(ds, []Definition{{"c", NewSumExpression("a")}})
	h, _ := RunConsumer(ds)

	AssertEqualHeaders(t, h, []string{"c"})
}
