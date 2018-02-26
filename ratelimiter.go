package ratelimiter

import (
	"log"
	"math"
	"sync"
	"time"
)

type limiter struct {
	qps     int64
	current int64
	lock    sync.RWMutex
	debug   bool
}


func New(qps int64) *limiter {
	rl := &limiter{
		qps: qps,
	}

	// launch a goroutine to reset the counter every second
	go rl.reset()

	return rl
}

// reset the counter of consumed QPS every second
func (rl *limiter) reset() {
	ticker := time.NewTicker(time.Second * 1)

	for range ticker.C {
		rl.lock.RLock()
		refresh := rl.current - rl.qps
		// negative qps makes no sense
		if refresh < 0 {
			refresh = 0
		}
		rl.log("Resetting rate limiter: %d-%d = %d\n", rl.current, rl.qps, refresh)
		rl.lock.RUnlock()

		rl.lock.Lock()
		rl.current = refresh
		rl.lock.Unlock()
	}
}

// Acquires requested QPS from the RateLimiter, blocking until the request can be granted.
func (rl *limiter) Acquire(requested int64) {
	rl.TryAcquire(requested, -1)
}

// Acquires requested QPS from the RateLimiter blocking at most t milliseconds. -1 will block forever.
//
// Returns true iff the request can be fulfilled without throttling
func (rl *limiter) TryAcquire(requested int64, t int64) bool {
	i := 0

	for {
		rl.lock.RLock()
		throttle := (rl.current + requested) > rl.qps
		rl.lock.RUnlock()

		if !throttle {
			rl.lock.Lock()
			rl.current += requested
			rl.lock.Unlock()
			return true
		} else {
			wait := math.Pow(2, float64(i))
			// our base unit of time is 1 second; there's no point on waiting more than that
			// as the counter will definitely be reset
			if wait > 1000 {
				wait = 1000
			}
			// -1 == forever
			if t != -1 && wait >= float64(t) {
				rl.log("Timeout exceeded")
				return false
			}
			rl.lock.RLock()
			rl.log("(%d+%d) > %d -> blocking for %.2f ms", rl.current, requested, rl.qps, wait)
			rl.lock.RUnlock()
			time.Sleep(time.Duration(wait) * time.Millisecond)
		}
	}
}

// Updates the rate (QPS) of the RateLimiter
func (rl *limiter) SetRate(n int64) {
	rl.lock.Lock()
	rl.qps = n
	rl.lock.Unlock()
}

// Returns the rate (QPS) with which the RateLimiter is configured
func (rl *limiter) GetRate() int64 {
	rl.lock.RLock()
	n := rl.qps
	rl.lock.RUnlock()

	return n
}

// Enable or disable debug logging
func (rl *limiter) Debug(enable bool) {
	rl.debug = enable
}

func (rl *limiter) log(format string, a ...interface{}) {
	if rl.debug {
		log.Printf(format, a...)
	}
}
