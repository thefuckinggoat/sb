package utils

import (
	"log"
	"math"
	"sync"
	"time"
)

// RateBucket holds rate limiting state for a single bucket
type RateBucket struct {
	mu                         sync.Mutex
	Actions                    int
	LastAction                 time.Time
	InitialLimit               int
	Cooldown                   float64
	ResetAfter                 float64
	MinCooldown                float64
	RateLimitedCount           int
	CooldownReductionThreshold int
	CurrentCooldown            float64
}

func NewRateBucket() *RateBucket {
	return &RateBucket{
		Actions:                    0,
		LastAction:                 time.Now(),
		InitialLimit:               30,
		Cooldown:                   2.5,
		ResetAfter:                 30,
		MinCooldown:                2.0,
		RateLimitedCount:           0,
		CooldownReductionThreshold: 5,
		CurrentCooldown:            2.5,
	}
}

// RateLimiter manages per-instance and per-command rate limiting
type RateLimiter struct {
	mu              sync.RWMutex
	instanceBuckets map[string]map[string]*RateBucket // instanceID -> commandKey -> bucket
	globalBuckets   map[string]*RateBucket            // instanceID -> global bucket
}

var (
	globalRateLimiter     *RateLimiter
	globalRateLimiterOnce sync.Once
)

func GetRateLimiter() *RateLimiter {
	globalRateLimiterOnce.Do(func() {
		globalRateLimiter = &RateLimiter{
			instanceBuckets: make(map[string]map[string]*RateBucket),
			globalBuckets:   make(map[string]*RateBucket),
		}
	})
	return globalRateLimiter
}

func (rl *RateLimiter) getInstanceBucket(instanceID, commandKey string) *RateBucket {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if _, ok := rl.instanceBuckets[instanceID]; !ok {
		rl.instanceBuckets[instanceID] = make(map[string]*RateBucket)
	}
	if _, ok := rl.instanceBuckets[instanceID][commandKey]; !ok {
		rl.instanceBuckets[instanceID][commandKey] = NewRateBucket()
	}
	return rl.instanceBuckets[instanceID][commandKey]
}

func (rl *RateLimiter) getGlobalBucket(instanceID string) *RateBucket {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if _, ok := rl.globalBuckets[instanceID]; !ok {
		rl.globalBuckets[instanceID] = NewRateBucket()
	}
	return rl.globalBuckets[instanceID]
}

// CheckGlobalLimit checks and applies global rate limiting. Returns sleep duration if rate limited.
func (rl *RateLimiter) CheckGlobalLimit(instanceID string) time.Duration {
	bucket := rl.getGlobalBucket(instanceID)

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	now := time.Now()
	timeSinceLast := now.Sub(bucket.LastAction).Seconds()

	if timeSinceLast >= bucket.ResetAfter {
		bucket.Actions = 0
		bucket.RateLimitedCount = 0
		bucket.CurrentCooldown = bucket.Cooldown
		bucket.LastAction = now
	}

	if bucket.Actions >= bucket.InitialLimit {
		bucket.RateLimitedCount++

		if bucket.RateLimitedCount >= bucket.CooldownReductionThreshold {
			reductionFactor := math.Min(
				float64(bucket.RateLimitedCount-bucket.CooldownReductionThreshold)/7.0,
				1.0,
			)
			bucket.CurrentCooldown = math.Max(
				bucket.MinCooldown,
				bucket.Cooldown-(reductionFactor*(bucket.Cooldown-bucket.MinCooldown)),
			)
		}

		bucket.Actions++
		bucket.LastAction = now
		return time.Duration(bucket.CurrentCooldown * float64(time.Second))
	}

	bucket.Actions++
	bucket.LastAction = now
	return 0
}

// CheckCommandLimit checks and applies per-command rate limiting. Returns sleep duration if rate limited.
func (rl *RateLimiter) CheckCommandLimit(instanceID, commandKey string) time.Duration {
	bucket := rl.getInstanceBucket(instanceID, commandKey)

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	now := time.Now()
	timeSinceLast := now.Sub(bucket.LastAction).Seconds()

	if timeSinceLast >= bucket.ResetAfter {
		bucket.Actions = 0
		bucket.RateLimitedCount = 0
		bucket.CurrentCooldown = bucket.Cooldown
		bucket.LastAction = now
	}

	if bucket.Actions >= bucket.InitialLimit {
		bucket.RateLimitedCount++

		if bucket.RateLimitedCount >= bucket.CooldownReductionThreshold {
			reductionFactor := math.Min(
				float64(bucket.RateLimitedCount-bucket.CooldownReductionThreshold)/7.0,
				1.0,
			)
			bucket.CurrentCooldown = math.Max(
				bucket.MinCooldown,
				bucket.Cooldown-(reductionFactor*(bucket.Cooldown-bucket.MinCooldown)),
			)
		}

		bucket.Actions++
		bucket.LastAction = now
		return time.Duration(bucket.CurrentCooldown * float64(time.Second))
	}

	bucket.Actions++
	bucket.LastAction = now
	return 0
}

// WrapRateLimit is a helper that applies both global and command rate limiting
func (rl *RateLimiter) WrapRateLimit(instanceID, commandKey string, globalOnly, commandOnly bool) {
	if globalOnly || (!globalOnly && !commandOnly) {
		if sleep := rl.CheckGlobalLimit(instanceID); sleep > 0 {
			log.Printf("[ratelimit] Global rate limited for %s, sleeping %v", instanceID, sleep)
			time.Sleep(sleep)
		}
	}

	if commandOnly || (!globalOnly && !commandOnly) {
		if sleep := rl.CheckCommandLimit(instanceID, commandKey); sleep > 0 {
			log.Printf("[ratelimit] Command rate limited for %s:%s, sleeping %v", instanceID, commandKey, sleep)
			time.Sleep(sleep)
		}
	}
}