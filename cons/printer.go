package cons

import (
	"bytes"
	"fmt"
	"github.com/lionell/aqua/data"
	"io"
	"text/tabwriter"
)

func RunTabularWriter(ds data.Source, out io.Writer) {
	w := tabwriter.NewWriter(out, 0, 0, 3, ' ', tabwriter.AlignRight)
	defer w.Flush()
	writeHeader(w, ds.Header)
	for goOn := true; goOn; {
		select {
		case r := <-ds.Data:
			writeRow(w, r)
		case <-ds.Done:
			goOn = false
		}
	}
}

func writeRow(w io.Writer, r data.Row) {
	var buf bytes.Buffer
	defer buf.WriteTo(w)
	for _, v := range r {
		buf.WriteString(fmt.Sprintf("%v\t", v))
	}
	buf.WriteString("\n")
}

func writeHeader(w io.Writer, h data.Header) {
	var buf bytes.Buffer
	defer buf.WriteTo(w)
	for _, v := range h {
		buf.WriteString(fmt.Sprintf("%v\t", v))
	}
	buf.WriteString("\n")
}
