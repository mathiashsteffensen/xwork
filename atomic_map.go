package xwork

import "sync"

type AtomicMap[T any] struct {
	m     map[string]T
	mutex sync.Mutex
}

func NewAtomicMap[T any]() AtomicMap[T] {
	return AtomicMap[T]{
		m:     make(map[string]T),
		mutex: sync.Mutex{},
	}
}

func (a *AtomicMap[T]) Get(key string) (T, bool) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	v, ok := a.m[key]
	return v, ok
}

func (a *AtomicMap[T]) Set(key string, value T) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.m[key] = value
}

func (a *AtomicMap[T]) Delete(key string) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	delete(a.m, key)
}

func (a *AtomicMap[T]) Each(f func(key string, value T)) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	for k, v := range a.m {
		f(k, v)
	}
}
