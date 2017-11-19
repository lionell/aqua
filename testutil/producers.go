package testutil

import "github.com/lionell/aqua/data"

func StartProducer(t data.Table) data.Source {
	out := data.NewSource(t.Header)
	go func() {
	Loop:
		for _, r := range t.Rows {
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

func StartLoopingProducer(t data.Table) data.Source {
	out := data.NewSource(t.Header)
	go func() {
	Loop:
		for {
			for _, r := range t.Rows {
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

func StartBlockingProducer(h data.Header) data.Source {
	out := data.NewSource(h)
	go func() {
		<-out.Stop
		out.Signal()
	}()
	return out
}
