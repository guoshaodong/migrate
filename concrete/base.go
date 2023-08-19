package concrete

type baseHandler struct {
	index int
}

func (b *baseHandler) GetIndex() int {
	return b.index
}
