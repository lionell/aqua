package column

type order int

const (
	_ order = iota
	OrderAsc
	OrderDesc
)

type Order struct {
	Column int
	Order  order
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