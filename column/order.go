package column

const (
	ASC = iota
	DESC
)

type Order struct {
	Column int
	Order  int
}
