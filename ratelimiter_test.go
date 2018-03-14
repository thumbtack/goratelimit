package ratelimiter

import (
	"testing"
)

func TestNew(t *testing.T) {
	// make sure to fail with invalid qps
	for _, qps := range []int64{0, -1} {
		rl := New(qps)

		if rl != nil {
			t.Fatal("Expected nil, got", rl)
		}
	}

	// make sure to succeed with valid qps
	for _, qps := range []int64{1, 10000} {
		rl := New(qps)

		if rl == nil {
			t.Fatal("Unexpected nil return")
		}
	}
}

func TestLimiter_UpdateRate(t *testing.T) {
	init := int64(5)
	rl := New(init)

	current := rl.GetRate()
	if current != init {
		t.Fatal("Expected", init, "got", current)
	}

	// make sure to fail with invalid qps
	for _, qps := range []int64{0, -1} {
		err := rl.SetRate(qps)
		if err == nil {
			t.Fatal("Expected nil, got", rl)
		}
	}

	// make sure to succeed with valid qps
	for _, qps := range []int64{1, 10000} {
		err := rl.SetRate(qps)
		if err != nil {
			t.Fatal("Unexpected nil return")
		}

		current := rl.GetRate()
		if current != qps {
			t.Fatal("Expected", qps, "got", current)
		}
	}
}

func TestLimiter_TryAcquire(t *testing.T) {
	rl := New(1)
	rl.Debug(true)

	// fail with a low timeout
	success := rl.TryAcquire(10, 5)
	if success {
		t.Fatal("Expected to fail")
	}

	// fail with a high timeout to make sure the counter is reset
	success = rl.TryAcquire(10, 3000)
	if success {
		t.Fatal("Expected to fail")
	}

	// succeed
	success = rl.TryAcquire(1, 1)
	if !success {
		t.Fatal("Expected to succeed")
	}
}

func TestLimiter_Acquire(t *testing.T) {
	rl := New(5)

	// we can't really test for failure here as it would just block forever
	// it's covered above though and we can touch the edge case of requesting the maximum qps
	rl.Acquire(5)
}