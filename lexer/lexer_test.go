package lexer

import (
	. "github.com/lionell/aqua/token"
	"testing"
)

func TestLexer_ScanTokenType(t *testing.T) {
	in := ` +-*/<>==!= =,| () distinct join order project take union where `
	exp := []Type{
		Plus, Minus, Asterisk, Slash, Lt, Gt, Eq, NotEq, Assign, Comma, Pipe,
		LParen, RParen, Distinct, Join, Order, Project, Take, Union, Where, Eof,
	}
	l := New(in)
	for _, e := range exp {
		tok := l.NextToken()
		if tok.Type != e {
			t.Errorf("Expected token type %#v, got %#v", e, tok.Type)
		}
	}
}

func TestLexer_ScanIdentifier(t *testing.T) {
	in := ` where ident1 == ident2 `
	exp := []Token{{Type: Where, Literal: "where"}, {Type: Ident, Literal: "ident1"}, {Type: Eq}, {Type: Ident, Literal: "ident2"}}
	l := New(in)
	for _, e := range exp {
		tok := l.NextToken()
		if tok != e {
			t.Errorf("Expected token %#v, got %#v", e, tok)
		}
	}
}

func TestLexer_ScanString(t *testing.T) {
	in := ` where ident == "string" `
	exp := []Token{{Type: Where, Literal: "where"}, {Type: Ident, Literal: "ident"}, {Type: Eq}, {Type: String, Literal: "string"}}
	l := New(in)
	for _, e := range exp {
		tok := l.NextToken()
		if tok != e {
			t.Errorf("Expected token %#v, got %#v", e, tok)
		}
	}
}

func TestLexer_ScanNumber(t *testing.T) {
	in := ` where ident == 4321 `
	exp := []Token{{Type: Where, Literal: "where"}, {Type: Ident, Literal: "ident"}, {Type: Eq}, {Type: Num, Literal: "4321"}}
	l := New(in)
	for _, e := range exp {
		tok := l.NextToken()
		if tok != e {
			t.Errorf("Expected token %#v, got %#v", e, tok)
		}
	}
}
