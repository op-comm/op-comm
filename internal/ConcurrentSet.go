package internal

import (
	"sync"
)

type Set[T comparable] map[T]struct{}
type ConcurrentSet[T comparable] struct {
	set   Set[T]
	mutex sync.RWMutex
}

func NewConcurrentSet[T comparable]() *ConcurrentSet[T] {
	return &ConcurrentSet[T]{
		set: make(Set[T]),
		mutex: sync.RWMutex{},
	}
}

func (cs *ConcurrentSet[T]) Has(data T) bool {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	_, ok := cs.set[data]
	return ok
}

func (cs *ConcurrentSet[T]) Add(data T) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	cs.set[data] = struct{}{}
}

func (cs *ConcurrentSet[T]) Delete(data T)  {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()

	delete(cs.set, data)
}

func ConcurrentSetFromList[T comparable](items []T)  *ConcurrentSet[T] {
	cs := NewConcurrentSet[T]()
	for _, item := range items {
		cs.set[item] = struct{}{}
	}
	return cs
}

func (cs *ConcurrentSet[T]) AsList() []T {
	cs.mutex.RLock() 
	defer cs.mutex.RUnlock()
	items := make([]T, 0, len(cs.set))
	for item := range cs.set {
		items = append(items, item)
	}
	return items
}

