package data

type Source struct {
	Header
	Data chan Row
	Stop chan struct{}
	Done chan struct{}
}

func NewSource(header Header) Source {
	return Source{header, make(chan Row), make(chan struct{}), make(chan struct{}, 1)}
}

func (s Source) Finalize() {
	if s.Stop == nil {
		return
	}
	select {
	case s.Stop <- struct{}{}:
		<-s.Done
	case <-s.Done:
	}
}

func (s Source) IsFinalized() bool {
	return s.Stop == nil
}

func (s *Source) MarkFinalized() {
	s.Stop = nil
}

func (s Source) Signal() {
	s.Done <- struct{}{}
}

func (s Source) Send(r Row) bool {
	select {
	case s.Data <- r:
		return true
	case <-s.Stop:
		return false
	}
}
