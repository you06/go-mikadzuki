package util

import (
	"sync"
	"time"
)

type Ticker struct {
	sync.RWMutex
	duration time.Duration
	last     time.Time
	end      bool
}

func NewTicker(duration time.Duration) Ticker {
	return Ticker{
		duration: duration,
	}
}

func (t *Ticker) Go(f func()) {
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		for range ticker.C {
			t.RLock()
			if t.end {
				return
			}
			if time.Since(t.last) > t.duration {
				f()
			}
			t.RUnlock()
		}
	}()
}

func (t *Ticker) Tick() {
	t.Lock()
	defer t.Unlock()
	t.last = time.Now()
	t.end = false
}

func (t *Ticker) Stop() {
	t.Lock()
	defer t.Unlock()
	t.end = true
}
