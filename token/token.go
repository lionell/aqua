package token

const (
	Illegal = "ILLEGAL"
	Eof     = "EOF"

	// Identifiers + literals
	Ident  = "IDENT" // add, foobar, x, y, ...
	Num    = "NUM"   // 1343456
	String = "STRING"

	// Operators
	Assign   = "="
	Plus     = "+"
	Minus    = "-"
	Bang     = "!"
	Asterisk = "*"
	Slash    = "/"

	Lt = "<"
	Gt = ">"

	Eq    = "=="
	NotEq = "!="

	// Delimiters
	Comma  = ","
	Pipe   = "|"
	LParen = "("
	RParen = ")"

	// Keywords
	Distinct = "DISTINCT"
	Join     = "JOIN"
	Order    = "ORDER"
	Project  = "PROJECT"
	Take     = "TAKE"
	Union    = "UNION"
	Where    = "WHERE"
)

type Type string

type Token struct {
	Type
	Literal string
}

var keywords = map[string]Type{
	"distinct": Distinct,
	"join":     Join,
	"order":    Order,
	"project":  Project,
	"take":     Take,
	"union":    Union,
	"where":    Where,
}

func LookupIdentifier(ident string) Type {
	if tok, ok := keywords[ident]; ok {
		return tok
	}
	return Ident
}
