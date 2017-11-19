package data

type Type int

const (
	TypeNone Type = iota
	TypeI32
)

type Value interface {
	Less(v Value) bool
	Equals(v Value) bool
	Type() Type
}
