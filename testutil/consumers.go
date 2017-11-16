package testutil

import (
	"github.com/lionell/aqua/data"
	"time"
)

func RunConsumer(in data.Source) (data.Header, []data.Row) {
	var out []data.Row
	for goOn := true; goOn; {
		select {
		case r := <-in.Data:
			out = append(out, r)
		case <-in.Done:
			goOn = false
		}
	}
	return in.Header, out
}

func RunConsumerWithLimit(in data.Source, limit int) []data.Row {
	var out []data.Row

Loop:
	for {
		select {
		case r := <-in.Data:
			out = append(out, r)
			limit--
			if limit == 0 {
				break Loop
			}
		case <-in.Done:
			in.IsFinalized()
			break Loop
		}
	}

	in.Finalize()
	return out
}

func RunConsumerWithTimeout(in data.Source, timeout time.Duration) []data.Row {
	var out []data.Row

Loop:
	for {
		select {
		case r := <-in.Data:
			out = append(out, r)
		case <-in.Done:
			in.IsFinalized()
			break Loop
		case <-time.After(timeout):
			break Loop
		}
	}

	in.Finalize()
	return out
}
