package domain

import (
	"math"
	"math/rand"
	"time"
)

type RetryPolicy interface {
	NextDelay(attempt int) time.Duration
}

type FixedRetryPolicy struct {
	Delay time.Duration
}

func NewFixedRetryPolicy(delay time.Duration) *FixedRetryPolicy {
	return &FixedRetryPolicy{
		Delay: delay,
	}
}

func (p *FixedRetryPolicy) NextDelay(attempt int) time.Duration {
	return p.Delay
}

type ExponentialRetryPolicy struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Factor       float64
	Jitter       bool
}

func NewExponentialRetryPolicy(initial, max time.Duration, factor float64, jitter bool) *ExponentialRetryPolicy {
	return &ExponentialRetryPolicy{
		InitialDelay: initial,
		MaxDelay:     max,
		Factor:       factor,
		Jitter:       jitter,
	}
}

func (p *ExponentialRetryPolicy) NextDelay(attempt int) time.Duration {
	if attempt <= 0 {
		return p.InitialDelay
	}

	delay := float64(p.InitialDelay) * math.Pow(p.Factor, float64(attempt))

	if p.MaxDelay > 0 && delay > float64(p.MaxDelay) {
		delay = float64(p.MaxDelay)
	}

	if p.Jitter {
		delay = delay * (0.5 + rand.Float64())
	}

	return time.Duration(delay)
}
