package testutil

import (
	"github.com/lionell/aqua/data"
	"time"
)

func RunConsumer(in data.Source) data.Table {
	var rows []data.Row
	for goOn := true; goOn; {
		select {
		case r := <-in.Data:
			rows = append(rows, r)
		case <-in.Done:
			goOn = false
		}
	}
	return data.MakeTable(in.Header, rows)
}

func RunConsumerWithLimit(in data.Source, limit int) data.Table {
	var rows []data.Row
	for goOn := true; goOn; {
		select {
		case r := <-in.Data:
			rows = append(rows, r)
			limit--
			if limit == 0 {
				goOn = false
			}
		case <-in.Done:
			in.IsFinalized()
			goOn = false
		}
	}
	in.Finalize()
	return data.MakeTable(in.Header, rows)
}

func RunConsumerWithTimeout(in data.Source, timeout time.Duration) data.Table {
	var rows []data.Row
	for goOn := true; goOn; {
		select {
		case r := <-in.Data:
			rows = append(rows, r)
		case <-in.Done:
			in.IsFinalized()
			goOn = false
		case <-time.After(timeout):
			goOn = false
		}
	}
	in.Finalize()
	return data.MakeTable(in.Header, rows)
}
