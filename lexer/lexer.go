package lexer

import (
	"github.com/lionell/aqua/token"
)

const eof byte = 0

type Lexer struct {
	input        string
	position     int  // current position in input (points to current char)
	readPosition int  // current reading position in input (after current char)
	ch           byte // current char under examination
}

func New(input string) *Lexer {
	l := &Lexer{input: input}
	l.readChar()
	return l
}

func (l *Lexer) readChar() {
	if l.readPosition >= len(l.input) {
		l.ch = eof
	} else {
		l.ch = l.input[l.readPosition]
	}

	l.position = l.readPosition
	l.readPosition += 1
}

func (l *Lexer) peekChar() byte {
	if l.readPosition >= len(l.input) {
		return eof
	}

	return l.input[l.readPosition]
}


func (l *Lexer) NextToken() token.Token {
	var tok token.Token

	l.skipWhitespace()

	switch l.ch {
	case '=':
		if l.peekChar() == '=' {
			l.readChar()
			tok = token.Token{Type: token.Eq}
		} else {
			tok = token.Token{Type: token.Assign}
		}
	case '+':
		tok = token.Token{Type: token.Plus}
	case '-':
		tok = token.Token{Type: token.Minus}
	case '!':
		if l.peekChar() == '=' {
			l.readChar()
			tok = token.Token{Type: token.NotEq}
		} else {
			tok = token.Token{Type: token.Bang}
		}
	case '/':
		tok = token.Token{Type: token.Slash}
	case '*':
		tok = token.Token{Type: token.Asterisk}
	case '<':
		tok = token.Token{Type: token.Lt}
	case '>':
		tok = token.Token{Type: token.Gt}
	case ',':
		tok = token.Token{Type: token.Comma}
	case '|':
		tok = token.Token{Type: token.Pipe}
	case '(':
		tok = token.Token{Type: token.LParen}
	case ')':
		tok = token.Token{Type: token.RParen}
	case '"':
		tok = token.Token{Type: token.String, Literal: l.readString()}
	case eof:
		tok = token.Token{Type: token.Eof}
	default:
		if isLetter(l.ch) {
			tok.Literal = l.readIdentifier()
			tok.Type = token.LookupIdentifier(tok.Literal)
			return tok
		} else if isDigit(l.ch) {
			return token.Token{Type: token.Num, Literal: l.readNumber()}
		} else {
			tok = token.Token{Type: token.Illegal, Literal: string(l.ch)}
		}
	}

	l.readChar()
	return tok
}

func (l *Lexer) readIdentifier() string {
	pos := l.position
	for isLetter(l.ch) || isDigit(l.ch) {
		l.readChar()
	}

	return l.input[pos:l.position]
}

func (l *Lexer) readNumber() string {
	position := l.position
	for isDigit(l.ch) {
		l.readChar()
	}
	return l.input[position:l.position]
}

func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

func (l *Lexer) readString() string {
	position := l.position + 1
	for {
		l.readChar()
		if l.ch == '"' {
			break
		}
	}

	return l.input[position:l.position]
}

func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

func isLetter(ch byte) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_'
}
