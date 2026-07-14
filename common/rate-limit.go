package common

import (
	"sync"
	"time"
)

type InMemoryRateLimiter struct {
	store              map[string]*[]int64
	pending            map[string]int
	mutex              sync.Mutex
	expirationDuration time.Duration
}

func (l *InMemoryRateLimiter) Init(expirationDuration time.Duration) {
	l.mutex.Lock()
	startCleanup := l.store == nil
	if l.store == nil {
		l.store = make(map[string]*[]int64)
		l.expirationDuration = expirationDuration
	}
	if l.pending == nil {
		l.pending = make(map[string]int)
	}
	l.mutex.Unlock()
	if startCleanup && expirationDuration > 0 {
		go l.clearExpiredItems()
	}
}

func (l *InMemoryRateLimiter) clearExpiredItems() {
	for {
		time.Sleep(l.expirationDuration)
		l.mutex.Lock()
		now := time.Now().Unix()
		for key := range l.store {
			queue := l.store[key]
			size := len(*queue)
			if size == 0 || now-(*queue)[size-1] > int64(l.expirationDuration.Seconds()) {
				delete(l.store, key)
			}
		}
		l.mutex.Unlock()
	}
}

// Request parameter duration's unit is seconds
func (l *InMemoryRateLimiter) Request(key string, maxRequestNum int, duration int64) bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	_, allowed := l.reserveLocked(key, maxRequestNum, duration)
	return allowed
}

// Reserve atomically consumes a slot and returns a completion callback. A
// failed operation releases the slot; a successful operation keeps it and
// refreshes its timestamp to completion time.
func (l *InMemoryRateLimiter) Reserve(key string, maxRequestNum int, duration int64) (func(bool), bool) {
	if maxRequestNum == 0 {
		return func(bool) {}, true
	}
	l.mutex.Lock()
	if l.store == nil {
		l.store = make(map[string]*[]int64)
	}
	if l.pending == nil {
		l.pending = make(map[string]int)
	}
	completed := l.pruneCompletedLocked(key, duration, time.Now().Unix())
	allowed := completed+l.pending[key] < maxRequestNum
	if allowed {
		l.pending[key]++
	}
	l.mutex.Unlock()
	if !allowed {
		return nil, false
	}
	var once sync.Once
	return func(success bool) {
		once.Do(func() {
			l.finishReservation(key, maxRequestNum, duration, success)
		})
	}, true
}

func (l *InMemoryRateLimiter) pruneCompletedLocked(key string, duration, now int64) int {
	queue, ok := l.store[key]
	if !ok || queue == nil || len(*queue) == 0 {
		delete(l.store, key)
		return 0
	}
	firstActive := 0
	for firstActive < len(*queue) && now-(*queue)[firstActive] >= duration {
		firstActive++
	}
	if firstActive > 0 {
		*queue = (*queue)[firstActive:]
	}
	if len(*queue) == 0 {
		delete(l.store, key)
		return 0
	}
	return len(*queue)
}

func (l *InMemoryRateLimiter) reserveLocked(key string, maxRequestNum int, duration int64) (int64, bool) {
	if maxRequestNum == 0 {
		return 0, true
	}
	// [old <-- new]
	queue, ok := l.store[key]
	now := time.Now().Unix()
	if ok {
		if len(*queue) < maxRequestNum {
			*queue = append(*queue, now)
			return now, true
		} else {
			if now-(*queue)[0] >= duration {
				*queue = (*queue)[1:]
				*queue = append(*queue, now)
				return now, true
			} else {
				return 0, false
			}
		}
	} else {
		s := make([]int64, 0, maxRequestNum)
		l.store[key] = &s
		*(l.store[key]) = append(*(l.store[key]), now)
	}
	return now, true
}

func (l *InMemoryRateLimiter) finishReservation(key string, maxRequestNum int, duration int64, success bool) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if pending := l.pending[key]; pending <= 1 {
		delete(l.pending, key)
	} else {
		l.pending[key] = pending - 1
	}
	if !success {
		return
	}
	now := time.Now().Unix()
	l.pruneCompletedLocked(key, duration, now)
	queue, ok := l.store[key]
	if !ok || queue == nil {
		s := make([]int64, 0, maxRequestNum)
		queue = &s
		l.store[key] = queue
	}
	*queue = append(*queue, now)
	if len(*queue) > maxRequestNum {
		*queue = (*queue)[len(*queue)-maxRequestNum:]
	}
}

// Check reports whether a request would be allowed without recording it.
// The duration parameter's unit is seconds.
func (l *InMemoryRateLimiter) Check(key string, maxRequestNum int, duration int64) bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if maxRequestNum == 0 {
		return true
	}
	now := time.Now().Unix()
	completed := l.pruneCompletedLocked(key, duration, now)
	return completed+l.pending[key] < maxRequestNum
}
