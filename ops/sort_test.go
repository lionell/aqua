package ops

import (
	. "github.com/lionell/aqua/column"
	. "github.com/lionell/aqua/data"
	. "github.com/lionell/aqua/testutil"
	"testing"
	"time"
)

func TestSort(t *testing.T) {
	tests := []struct {
		desc  string
		in    Table
		order []SortingOrder
		exp   []Row
	}{
		{
			desc:  "empty source",
			in:    MakeTable([]Column{{"a", TypeI32}}, nil),
			order: []SortingOrder{{"a", OrderAsc}},
			exp:   nil,
		},
		{
			desc: "sort by one column",
			in: MakeTable([]Column{{"a", TypeI32}, {"b", TypeI32}}, []Row{
				{I32(1), I32(2)},
				{I32(7), I32(7)},
				{I32(3), I32(4)},
				{I32(0), I32(1)},
			}),
			order: []SortingOrder{{"a", OrderAsc}},
			exp: []Row{
				{I32(0), I32(1)},
				{I32(1), I32(2)},
				{I32(3), I32(4)},
				{I32(7), I32(7)},
			},
		},
		{
			desc: "sort by two columns",
			in: MakeTable([]Column{{"a", TypeI32}, {"b", TypeI32}, {"c", TypeI32}}, []Row{
				{I32(3), I32(8), I32(4)},
				{I32(1), I32(1), I32(-21)},
				{I32(3), I32(2), I32(-1)},
				{I32(3), I32(4), I32(14)},
				{I32(7), I32(7), I32(3)},
			}),
			order: []SortingOrder{{"a", OrderDesc}, {"b", OrderAsc}},
			exp: []Row{
				{I32(7), I32(7), I32(3)},
				{I32(3), I32(2), I32(-1)},
				{I32(3), I32(4), I32(14)},
				{I32(3), I32(8), I32(4)},
				{I32(1), I32(1), I32(-21)},
			},
		},
		{
			desc: "sort data with equal rows",
			in: MakeTable([]Column{{"a", TypeI32}, {"b", TypeI32}}, []Row{
				{I32(1), I32(2)},
				{I32(7), I32(7)},
				{I32(0), I32(1)},
				{I32(1), I32(2)},
			}),
			order: []SortingOrder{{"a", OrderAsc}},
			exp: []Row{
				{I32(0), I32(1)},
				{I32(1), I32(2)},
				{I32(1), I32(2)},
				{I32(7), I32(7)},
			},
		},
	}
	for _, ts := range tests {
		ds := StartProducer(ts.in)
		ds, err := Sort(ds, ts.order)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		res := RunConsumer(ds)
		AssertEqualRowsInOrder(t, res.Rows, ts.exp)
	}
}

func TestSortCanStopOnReceivingData(t *testing.T) {
	in := MakeTable([]Column{{"a", TypeI32}}, []Row{
		{I32(1)},
	})

	ds := StartInfiniteProducer(in)
	ds, err := Sort(ds, []SortingOrder{{"a", OrderAsc}})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	RunConsumerWithTimeout(ds, time.Millisecond*100)
}

func TestSortCanStopOnSendingResults(t *testing.T) {
	in := MakeTable([]Column{{"a", TypeI32}}, []Row{
		{I32(2)},
		{I32(7)},
		{I32(0)},
		{I32(1)},
	})

	ds := StartProducer(in)
	ds, err := Sort(ds, []SortingOrder{{"a", OrderAsc}})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	res := RunConsumerWithLimit(ds, 1)

	AssertEqualRowsInOrder(t, res.Rows, in.Rows[2:3])
}

func TestSortPreservesHeader(t *testing.T) {
	ds := StartProducer(MakeTable([]Column{{"a", TypeI32}, {"b", TypeI32}}, nil))
	ds, err := Sort(ds, []SortingOrder{{"a", OrderAsc}})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	res := RunConsumer(ds)

	AssertEqualHeaders(t, res.Header, []Column{{"a", TypeI32}, {"b", TypeI32}})
}
