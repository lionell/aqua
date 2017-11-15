package ops

import (
	. "github.com/lionell/aqua/column"
	. "github.com/lionell/aqua/data"
	. "github.com/lionell/aqua/testutil"
	"testing"
)

// TODO(lionell): Test for errors

func TestProjectColumnOnItself(t *testing.T) {
	rows := []Row{
		{I32(1)},
		{I32(2)},
	}

	ds := StartProducer(rows)
	ds = Project(ds, []string{"test"}, []Expression{NewSumExpression("test")})
	res := RunConsumer(ds)

	AssertEquals(t, res, rows)
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

	ds := StartProducer(rows)
	ds = Project(ds, []string{"test1", "test2"}, []Expression{NewSumExpression("test1", "test2")})
	res := RunConsumer(ds)

	AssertEquals(t, res, exp)
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

	ds := StartProducer(rows)
	ds = Project(ds, []string{"test1", "test2"}, []Expression{NewSumExpression("test1")})
	res := RunConsumer(ds)

	AssertEquals(t, res, exp)
}

func TestProjectCanStop(t *testing.T) {
	rows := []Row{
		{I32(1)},
	}
	exp := []Row{
		{I32(1)},
		{I32(1)},
	}

	ds := StartInfiniteProducer(rows)
	ds = Project(ds, []string{"test"}, []Expression{NewSumExpression("test")})
	res := RunConsumerWithLimit(ds, 2)

	AssertEquals(t, res, exp)
}
