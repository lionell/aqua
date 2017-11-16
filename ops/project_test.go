package ops

import (
	. "github.com/lionell/aqua/column"
	. "github.com/lionell/aqua/data"
	. "github.com/lionell/aqua/testutil"
	"testing"
)

// TODO(lionell): Test for errors.

func TestProjectColumnOnItself(t *testing.T) {
	in := NewTable([]string{"a"}, []Row{
		{I32(1)},
		{I32(2)},
	})

	ds := StartProducer(in)
	ds = Project(ds, []Definition{{"a", NewSumExpression("a")}})
	res := RunConsumer(ds)

	AssertEqualRows(t, res.Rows, in.Rows)
}

func TestProjectSumOfColumns(t *testing.T) {
	in := NewTable([]string{"a", "b"}, []Row{
		{I32(1), I32(8)},
		{I32(2), I32(-1)},
	})
	exp := []Row{
		{I32(9)},
		{I32(1)},
	}

	ds := StartProducer(in)
	ds = Project(ds, []Definition{{"c", NewSumExpression("a", "b")}})
	res := RunConsumer(ds)

	AssertEqualRows(t, res.Rows, exp)
}

func TestProjectDoesNotIncludeUnnecessaryColumns(t *testing.T) {
	in := NewTable([]string{"a", "b"}, []Row{
		{I32(1), I32(8)},
		{I32(2), I32(-1)},
	})
	exp := []Row{
		{I32(1)},
		{I32(2)},
	}

	ds := StartProducer(in)
	ds = Project(ds, []Definition{{"c", NewSumExpression("a")}})
	res := RunConsumer(ds)

	AssertEqualRows(t, res.Rows, exp)
}

func TestProjectCanStop(t *testing.T) {
	in := NewTable([]string{"a"}, []Row{
		{I32(1)},
	})
	exp := []Row{
		{I32(1)},
		{I32(1)},
	}

	ds := StartInfiniteProducer(in)
	ds = Project(ds, []Definition{{"a", NewSumExpression("a")}})
	res := RunConsumerWithLimit(ds, 2)

	AssertEqualRows(t, res.Rows, exp)
}

func TestProjectPreservesHeader(t *testing.T) {
	ds := StartProducer(NewTable([]string{"a", "b"}, nil))
	ds = Project(ds, []Definition{{"c", NewSumExpression("a")}})
	res := RunConsumer(ds)

	AssertEqualHeaders(t, res.Header, []string{"c"})
}
