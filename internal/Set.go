package internal

type Set[T comparable] map[T]struct{}

func NewSet[T comparable]() Set[T] {
	return make(Set[T])
}
func (set Set[T]) Has(data T) bool {
	_, ok := set[data]
	return ok
}

func (set Set[T]) Add(data T) {
	set[data] = struct{}{}
}

func (set Set[T]) Delete(data T) {
	delete(set, data)
}

func SetFromList[T comparable](items []T) Set[T] {
	set := NewSet[T]()
	for _, item := range items {
		set[item] = struct{}{}
	}
	return set
}

func (set Set[T]) AsList() []T {
	items := make([]T, 0, len(set))
	for item := range set {
		items = append(items, item)
	}
	return items
}
