package token

type Type int

const (
	Illegal Type = iota
	Eof

	Ident
	Str
	Num

	Plus
	Minus
	Asterisk
	Slash

	Lt
	Gt
	Eq
	NotEq

	Assign
	Comma
	Pipe

	LParen
	RParen

	Distinct
	Join
	Order
	Project
	Take
	Union
	Where
)

type Token struct {
	Value string
	Type
}
