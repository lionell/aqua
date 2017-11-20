package lexer

import (
	"bufio"
	"io"
	"bytes"
	"strings"
	"github.com/lionell/aqua/lexer/token"
	"github.com/pkg/errors"
	"unicode"
	"fmt"
)

const eof = rune(0)

type Lexer struct {
	r *bufio.Reader
}

func NewLexer(r io.Reader) *Lexer {
	return &Lexer{bufio.NewReader(r)}
}

func (l Lexer) Scan() (token.Token, error) {
	l.skipWhitespaces()

	ch := l.read()
	switch ch {
	case 0:
		return token.Token{Type: token.Eof}, nil
	case '"':
		l.unread()
		return l.scanString()
	case '+':
		if unicode.IsDigit(l.read()) {
			l.unread()
			l.unread()
			return l.scanNumber()
		}
		l.unread()
		return token.Token{Type: token.Plus}, nil
	case '-':
		if unicode.IsDigit(l.read()) {
			l.unread()
			l.unread()
			return l.scanNumber()
		}
		l.unread()
		return token.Token{Type: token.Minus}, nil
	case '*':
		return token.Token{Type: token.Asterisk}, nil
	case '/':
		return token.Token{Type: token.Slash}, nil
	case '<':
		return token.Token{Type: token.Lt}, nil
	case '>':
		return token.Token{Type: token.Gt}, nil
	case '=':
		if l.read() == '=' {
			return token.Token{Type: token.Eq}, nil
		} else {
			l.unread()
			return token.Token{Type: token.Assign}, nil
		}
	case '!':
		l.eat('=')
		return token.Token{Type: token.NotEq}, nil
	case ',':
		return token.Token{Type: token.Comma}, nil
	case '|':
		return token.Token{Type: token.Pipe}, nil
	case '(':
		return token.Token{Type: token.LParen}, nil
	case ')':
		return token.Token{Type: token.RParen}, nil
	default:
		if unicode.IsLetter(ch) {
			l.unread()
			return l.scanIdentifier()
		} else if unicode.IsDigit(ch) {
			l.unread()
			return l.scanNumber()
		} else {
			return token.Token{}, fmt.Errorf("unexpected character %v", ch)
		}
	}
}

func (l Lexer) scanIdentifier() (token.Token, error) {
	var buf bytes.Buffer

	for ch := l.read(); ch != eof; ch = l.read() {
		if !unicode.IsLetter(ch) && !unicode.IsDigit(ch) && ch != '_' {
			l.unread()
			break
		}
		buf.WriteRune(ch)
	}

	switch strings.ToLower(buf.String()) {
	case "distinct":
		return token.Token{Type: token.Distinct}, nil
	case "join":
		return token.Token{Type: token.Join}, nil
	case "order":
		return token.Token{Type: token.Order}, nil
	case "project":
		return token.Token{Type: token.Project}, nil
	case "take":
		return token.Token{Type: token.Take}, nil
	case "union":
		return token.Token{Type: token.Union}, nil
	case "where":
		return token.Token{Type: token.Where}, nil
	default:
		return token.Token{Value: buf.String(), Type: token.Ident}, nil
	}
}

func (l Lexer) scanNumber() (token.Token, error) {
	// TODO(lionell): Properly implement this.
	var buf bytes.Buffer
	for ch := l.read(); ch != eof; ch = l.read() {
		if !unicode.IsDigit(ch) {
			l.unread()
			break
		}
		buf.WriteRune(ch)
	}

	return token.Token{Value: buf.String(), Type: token.Num}, nil
}

func (l Lexer) scanString() (token.Token, error) {
	if err := l.eat('"'); err != nil {
		return token.Token{}, errors.Wrap(err, "wrong string literal")
	}

	var buf bytes.Buffer
	for ch := l.read(); ch != eof; ch = l.read() {
		if ch == '"' {
			l.unread()
			break
		}
		buf.WriteRune(ch)
	}

	if err := l.eat('"'); err != nil {
		return token.Token{}, errors.Wrap(err, "wrong string literal")
	}
	return token.Token{Value: buf.String(), Type: token.Str}, nil
}

func (l Lexer) skipWhitespaces() {
	for unicode.IsSpace(l.read()) {}
	l.unread()
}

func (l Lexer) eat(ch rune) error {
	if r := l.read(); r != ch {
		return fmt.Errorf("expected character %v got %v", ch, r)
	}
	return nil
}

func (l Lexer) read() rune {
	ch, _, err := l.r.ReadRune()
	if err != nil {
		return eof
	}
	return ch
}

func (l Lexer) unread() { _ = l.r.UnreadRune() }
