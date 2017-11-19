package data

type Table struct {
	Header
	Rows []Row
}

func MakeTable(h Header, r []Row) Table {
	return Table{h, r}
}
