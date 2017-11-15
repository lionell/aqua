package jobs

import (
	"bytes"
	"fmt"
	"github.com/lionell/aqua/data"
	"io"
	"text/tabwriter"
)

func RunTabularWriter(out io.Writer, ds data.Source, h data.Header) {
	w := tabwriter.NewWriter(out, 0, 0, 3, ' ', tabwriter.AlignRight)
	defer w.Flush()

	writeHeader(w, h)
Loop:
	for {
		select {
		case r := <-ds.Data:
			writeRow(w, r)
		case <-ds.Done:
			break Loop
		}
	}
}

func writeRow(w io.Writer, r data.Row) {
	var buf bytes.Buffer
	for _, v := range r {
		buf.WriteString(fmt.Sprintf("%v\t", v))
	}
	buf.WriteString("\n")
	buf.WriteTo(w)
}

func writeHeader(w io.Writer, h data.Header) {
	var buf bytes.Buffer
	for _, v := range h {
		buf.WriteString(fmt.Sprintf("%v\t", v))
	}
	buf.WriteString("\n")
	buf.WriteTo(w)
}
