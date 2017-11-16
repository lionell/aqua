package testutil

import (
	"github.com/lionell/aqua/data"
)

func StartProducer(rows []data.Row, header ...string) data.Source {
	out := data.NewSource(header)

	go func() {
	Loop:
		for _, r := range rows {
			select {
			case out.Data <- r:
			case <-out.Stop:
				break Loop
			}
		}
		out.Signal()
	}()

	return out
}

func StartInfiniteProducer(rows []data.Row, header ...string) data.Source {
	out := data.NewSource(header)

	go func() {
	Loop:
		for {
			for _, r := range rows {
				select {
				case out.Data <- r:
				case <-out.Stop:
					break Loop
				}
			}
		}
		out.Signal()
	}()

	return out
}
