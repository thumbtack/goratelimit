// Package ratelimiter implements a simple rate limiter (loosely based on Guava's rate limiter class).
//
// Rate limiting algorithms are typically used to distribute permits that grant access to a given resource
// at a configurable rate.
//
// A rate Limiter instance is defined by the (fixed) rate at which permits are issued (often referred to as queries
// per second) to a given client. Permits are distributed smoothly, with a delay between individual requests being
// imposed in order to ensure that the configured rate is maintained.

package goratelimit

import (
	"errors"
	"log"
	"math"
	"sync"
	"time"
)

type Limiter struct {
	qps     int64
	current map[string]int64
	lock    sync.RWMutex
	debug   bool
}

// New creates a new rate Limiter instance with the specified QPS
//
// Returns nil if qps is negative or zero
func New(qps int64) *Limiter {
	if qps <= 0 {
		return nil
	}

	rl := &Limiter{
		qps: qps,
	}
	rl.current = make(map[string]int64, 0)

	// launch a goroutine to reset the counter every second
	go rl.reset()

	return rl
}

// reset the counter of consumed QPS -- 100ms resolution
func (rl *Limiter) reset() {
	ticker := time.NewTicker(time.Millisecond * 100)

	for range ticker.C {
		rl.lock.RLock()
		for client := range rl.current {
			// we're refreshing every 100ms; 1/10th of the time, grant 1/10th of the qps
			refresh := rl.current[client] - rl.qps/10
			// negative qps makes no sense
			if refresh < 0 {
				refresh = 0
			}
			rl.current[client] = refresh
			rl.debugLog("'%s': resetting rate Limiter: %d-%d = %d\n", client, rl.current[client], rl.qps, refresh)
		}
		rl.lock.RUnlock()
	}
}

// Acquires requested QPS from the rate Limiter, blocking until the request can be granted.
func (rl *Limiter) Acquire(client string, requested int64) {
	rl.TryAcquire(client, requested, -1)
}

// Acquires requested QPS from the rate Limiter blocking at most t milliseconds. -1 will block forever.
//
// Returns true iff the request can be fulfilled without throttling
func (rl *Limiter) TryAcquire(client string, requested int64, t int64) bool {
	i := 0
	totalWait := float64(0)

	for {
		var current int64
		rl.lock.Lock()
		if _, ok := rl.current[client]; !ok {
			// first time this client is being used, initialize
			rl.current[client] = 0
		}
		current = rl.current[client]
		rl.lock.Unlock()

		throttle := (current + requested) > rl.qps

		if !throttle {
			rl.lock.Lock()
			rl.current[client] += requested
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
				rl.debugLog("'%s': timeout exceeded", client)
				return false
			}
			totalWait += wait
			rl.debugLog("'%s': (%d+%d) > %d -> blocking for %.2f ms", client, current, requested, rl.qps, wait)
			time.Sleep(time.Duration(wait) * time.Millisecond)
		}

		i++
	}
}

// SetRate updates the rate (QPS) of the rate Limiter
//
// Returns an error if qps is negative or zero
func (rl *Limiter) SetRate(qps int64) error {
	if qps <= 0 {
		return errors.New("qps must be greater than zero")
	}

	rl.lock.Lock()
	rl.qps = qps
	rl.lock.Unlock()

	return nil
}

// Returns the rate (QPS) with which the rate Limiter is configured
func (rl *Limiter) GetRate() int64 {
	rl.lock.RLock()
	qps := rl.qps
	rl.lock.RUnlock()

	return qps
}

// Enable or disable debugLog logging
func (rl *Limiter) Debug(enable bool) {
	rl.debug = enable
}

func (rl *Limiter) debugLog(format string, a ...interface{}) {
	if rl.debug {
		log.Printf(format, a...)
	}
}
