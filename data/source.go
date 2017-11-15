package data

type Source struct {
	Data chan Row
	Stop chan struct{}
	Done chan struct{}
}

func NewSource() Source {
	return Source{make(chan Row), make(chan struct{}), make(chan struct{})}
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

func (s *Source) SetFinalized() {
	s.Stop = nil
}

func (s Source) Signal() {
	s.Done <- struct{}{}
}

func (s Source) TrySend(r Row) bool {
	select {
	case s.Data <- r:
		return true
	case <-s.Stop:
		return false
	}
}
