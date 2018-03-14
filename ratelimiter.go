// Package ratelimiter implements a simple rate limiter (loosely inspired in Guava's rate limiter class).
//
// Rate limiting algorithms are typically used to distribute permits that grant access to a given resource
// at a configurable rate.
//
// A rate limiter instance is defined by the (fixed) rate at which permits are issued (often referred to as queries
// per second). Permits are distributed smoothly, with a delay between individual requests being imposed in order
// to ensure that the configured rate is maintained.

package ratelimiter

import (
	"errors"
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

// New creates a new rate limiter instance with the specified QPS
//
// Returns nil if qps is negative or zero
func New(qps int64) *limiter {
	if qps <= 0 {
		return nil
	}

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
		current := rl.current
		rl.lock.RUnlock()

		refresh := current - rl.qps
		// negative qps makes no sense
		if refresh < 0 {
			refresh = 0
		}

		rl.debugLog("Resetting rate limiter: %d-%d = %d\n", current, rl.qps, refresh)

		rl.lock.Lock()
		rl.current = refresh
		rl.lock.Unlock()
	}
}

// Acquires requested QPS from the rate limiter, blocking until the request can be granted.
func (rl *limiter) Acquire(requested int64) {
	rl.TryAcquire(requested, -1)
}

// Acquires requested QPS from the rate limiter blocking at most t milliseconds. -1 will block forever.
//
// Returns true iff the request can be fulfilled without throttling
func (rl *limiter) TryAcquire(requested int64, t int64) bool {
	i := 0
	totalWait := float64(0)

	for {
		rl.lock.RLock()
		current := rl.current
		rl.lock.RUnlock()

		throttle := (current + requested) > rl.qps

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
			// -1 == wait forever
			if t != -1 && (totalWait+wait) >= float64(t) {
				rl.debugLog("Timeout exceeded")
				return false
			}
			totalWait += wait
			rl.debugLog("(%d+%d) > %d -> blocking for %.2f ms", current, requested, rl.qps, wait)
			time.Sleep(time.Duration(wait) * time.Millisecond)
		}

		i++
	}
}

// SetRate updates the rate (QPS) of the rate limiter
//
// Returns an error if qps is negative or zero
func (rl *limiter) SetRate(qps int64) error {
	if qps <= 0 {
		return errors.New("qps must be greater than zero")
	}

	rl.lock.Lock()
	rl.qps = qps
	rl.lock.Unlock()

	return nil
}

// Returns the rate (QPS) with which the rate limiter is configured
func (rl *limiter) GetRate() int64 {
	rl.lock.RLock()
	qps := rl.qps
	rl.lock.RUnlock()

	return qps
}

// Enable or disable debugLog logging
func (rl *limiter) Debug(enable bool) {
	rl.debug = enable
}

func (rl *limiter) debugLog(format string, a ...interface{}) {
	if rl.debug {
		log.Printf(format, a...)
	}
}
