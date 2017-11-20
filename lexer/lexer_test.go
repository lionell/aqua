package lexer

import (
	"testing"
	. "github.com/lionell/aqua/lexer/token"
	"strings"
)

func TestLexer_ScanTokenType(t *testing.T) {
	in := ` +-*/<>==!= =,| () distinct join order project take union where `
	exp := []Type{
		Plus, Minus, Asterisk, Slash, Lt, Gt, Eq, NotEq, Assign, Comma, Pipe,
		LParen, RParen, Distinct, Join, Order, Project, Take, Union, Where, Eof,
	}
	l := NewLexer(strings.NewReader(in))
	for _, e := range exp {
		tok, err := l.Scan()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if tok.Type != e {
			t.Errorf("Expected token type %#v, got %#v", e, tok.Type)
		}
	}
}

func TestLexer_ScanIdentifier(t *testing.T) {
	in := ` where ident1 == ident2 `
	exp := []Token{{Type: Where}, {Type: Ident, Value: "ident1"}, {Type: Eq}, {Type: Ident, Value: "ident2"}}
	l := NewLexer(strings.NewReader(in))
	for _, e := range exp {
		tok, err := l.Scan()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if tok != e {
			t.Errorf("Expected token %#v, got %#v", e, tok)
		}
	}
}

func TestLexer_ScanString(t *testing.T) {
	in := ` where ident == "string" `
	exp := []Token{{Type: Where}, {Type: Ident, Value: "ident"}, {Type: Eq}, {Type: Str, Value: "string"}}
	l := NewLexer(strings.NewReader(in))
	for _, e := range exp {
		tok, err := l.Scan()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if tok != e {
			t.Errorf("Expected token %#v, got %#v", e, tok)
		}
	}
}

func TestLexer_ScanNumber(t *testing.T) {
	in := ` where ident == 4321 `
	exp := []Token{{Type: Where}, {Type: Ident, Value: "ident"}, {Type: Eq}, {Type: Num, Value: "4321"}}
	l := NewLexer(strings.NewReader(in))
	for _, e := range exp {
		tok, err := l.Scan()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if tok != e {
			t.Errorf("Expected token %#v, got %#v", e, tok)
		}
	}
}
