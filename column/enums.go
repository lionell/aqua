package column

type Order int

const (
	_ Order = iota
	OrderAsc
	OrderDesc
)

type SortBy struct {
	Column string
	Order
}

type JoinType int

const (
	_ JoinType = iota
	JoinInner
	JoinLeft
	JoinRight
	JoinFull
)

type JoinCondition struct {
	LeftColumn, RightColumn int
}
