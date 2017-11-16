package ops

//import (
//	. "github.com/lionell/aqua/column"
//	. "github.com/lionell/aqua/data"
//	. "github.com/lionell/aqua/testutil"
//	"testing"
//)
//
//func TestJoin(t *testing.T) {
//	rows1 := []Row{
//		{I32(1), I32(2)},
//		{I32(3), I32(4)},
//	}
//	rows2 := []Row{
//		{I32(1), I32(6)},
//		{I32(3), I32(8)},
//	}
//	exp := []Row{
//		{I32(5), I32(6)},
//		{I32(7), I32(8)},
//	}
//
//	ds1 := StartProducer(rows1)
//	ds2 := StartProducer(rows2)
//	ds := Join(ds1, ds2, []JoinCondition{{0, 0}}, JoinInner)
//	_, res := RunConsumer(ds)
//
//	AssertEqualRows(t, res, exp)
//}
