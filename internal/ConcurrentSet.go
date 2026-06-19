package internal

import (
	"sync"
)

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
	return cs.set.Has(data)
}

func (cs *ConcurrentSet[T]) Add(data T) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	cs.set.Add(data)
}

func (cs *ConcurrentSet[T]) Delete(data T)  {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	cs.set.Delete(data)
}

func ConcurrentSetFromList[T comparable](items []T)  *ConcurrentSet[T] {
	cs := NewConcurrentSet[T]()
	cs.set = SetFromList(items)
	return cs
}

func (cs *ConcurrentSet[T]) AsList() []T {
	cs.mutex.RLock() 
	defer cs.mutex.RUnlock()
	
	return cs.set.AsList()
}

