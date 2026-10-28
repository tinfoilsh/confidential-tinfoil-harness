package search

import (
	"context"
	"sync"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"golang.org/x/time/rate"
)

// The inference service is rationed in inputs per second and in calls in
// flight, for the process as a whole and for each caller, so one caller's
// rebuild cannot starve every other's queries.
const (
	globalInputsPerSecond = 100
	globalInputBurst      = 256
	globalConcurrentCalls = 16
	callerInputsPerSecond = 10
	callerInputBurst      = 32
	callerConcurrentCalls = 2
	// A caller's limiter is forgotten after limiterIdle without use, and
	// at most limiterMaxCallers are tracked; past that, the longest idle
	// one goes.
	limiterIdle       = 10 * time.Minute
	limiterMaxCallers = 10_000
	// Background work that is over its rate asks again every
	// limiterRetryInterval.
	limiterRetryInterval = 100 * time.Millisecond
	// A query or an inline index update waits foregroundEmbedTimeout for
	// its vectors before going on without them; a rebuild waits
	// backgroundEmbedTimeout.
	foregroundEmbedTimeout = 3 * time.Second
	backgroundEmbedTimeout = 10 * time.Second
)

type inferenceLimits struct {
	globalRate       rate.Limit
	globalBurst      int
	globalConcurrent int
	callerRate       rate.Limit
	callerBurst      int
	callerConcurrent int
}

type inferenceTimeouts struct {
	foreground time.Duration
	background time.Duration
}

var defaultInferenceLimits = inferenceLimits{
	globalRate:       globalInputsPerSecond,
	globalBurst:      globalInputBurst,
	globalConcurrent: globalConcurrentCalls,
	callerRate:       callerInputsPerSecond,
	callerBurst:      callerInputBurst,
	callerConcurrent: callerConcurrentCalls,
}

var defaultInferenceTimeouts = inferenceTimeouts{foreground: foregroundEmbedTimeout, background: backgroundEmbedTimeout}

// errInferenceLimited refuses an embedding a caller, or the process, has
// no room for now.
var errInferenceLimited = api.Err(429, "RATE_LIMITED", "The search inference limit is exceeded")

// callerLimit is one caller's share: a rate for the foreground and one
// for the background, so a rebuild cannot use up what queries need, and
// the calls they may have in flight.
type callerLimit struct {
	foreground *rate.Limiter
	background *rate.Limiter
	slots      chan struct{}
	lastUsed   time.Time
	active     int
}

// inferenceGate admits embedding calls within the limits.
type inferenceGate struct {
	mu          sync.Mutex
	rateMu      sync.Mutex
	limits      inferenceLimits
	global      *rate.Limiter
	globalSlots chan struct{}
	callers     map[string]*callerLimit
}

func newInferenceGate(limits inferenceLimits) *inferenceGate {
	return &inferenceGate{
		limits:      limits,
		global:      rate.NewLimiter(limits.globalRate, limits.globalBurst),
		globalSlots: make(chan struct{}, limits.globalConcurrent),
		callers:     map[string]*callerLimit{},
	}
}

// begin is the caller's limit, made on first use; nil when no more
// callers can be tracked.
func (g *inferenceGate) begin(owner string, now time.Time) *callerLimit {
	g.mu.Lock()
	defer g.mu.Unlock()
	if limit, ok := g.callers[owner]; ok {
		limit.lastUsed = now
		limit.active++
		return limit
	}
	if len(g.callers) >= limiterMaxCallers {
		var oldestID string
		var oldest time.Time
		for id, limit := range g.callers {
			if limit.active != 0 {
				continue
			}
			if now.Sub(limit.lastUsed) >= limiterIdle {
				delete(g.callers, id)
				continue
			}
			if oldestID == "" || limit.lastUsed.Before(oldest) {
				oldestID, oldest = id, limit.lastUsed
			}
		}
		if len(g.callers) >= limiterMaxCallers && oldestID != "" {
			delete(g.callers, oldestID)
		}
		if len(g.callers) >= limiterMaxCallers {
			return nil
		}
	}
	limit := &callerLimit{
		foreground: rate.NewLimiter(g.limits.callerRate, g.limits.callerBurst),
		background: rate.NewLimiter(g.limits.callerRate, g.limits.callerBurst),
		slots:      make(chan struct{}, g.limits.callerConcurrent),
		lastUsed:   now,
		active:     1,
	}
	g.callers[owner] = limit
	return limit
}

// acquire admits one call of inputs for owner, and is what releases it.
// Foreground work is refused at once when over a limit; background work
// waits its turn until ctx ends.
func (g *inferenceGate) acquire(ctx context.Context, owner string, inputs int, wait bool) (func(), error) {
	if inputs <= 0 {
		return func() {}, nil
	}
	limit := g.begin(owner, time.Now())
	if limit == nil {
		return nil, errInferenceLimited
	}
	finish := func() {
		g.mu.Lock()
		limit.active--
		limit.lastUsed = time.Now()
		g.mu.Unlock()
	}
	limiter := limit.foreground
	if wait {
		limiter = limit.background
		if !g.waitForRate(ctx, limiter, inputs) {
			finish()
			return nil, errInferenceLimited
		}
	}
	if !takeSlot(ctx, limit.slots, wait) {
		finish()
		return nil, errInferenceLimited
	}
	if !takeSlot(ctx, g.globalSlots, wait) {
		<-limit.slots
		finish()
		return nil, errInferenceLimited
	}
	if !wait && !g.reserveRate(limiter, inputs) {
		<-g.globalSlots
		<-limit.slots
		finish()
		return nil, errInferenceLimited
	}
	return func() {
		<-g.globalSlots
		<-limit.slots
		finish()
	}, nil
}

// waitForRate waits until inputs fit the caller's and the global rate,
// or ctx ends; more inputs than either burst never fit.
func (g *inferenceGate) waitForRate(ctx context.Context, limiter *rate.Limiter, inputs int) bool {
	if inputs > limiter.Burst() || inputs > g.global.Burst() {
		return false
	}
	ticker := time.NewTicker(limiterRetryInterval)
	defer ticker.Stop()
	for {
		if g.reserveRate(limiter, inputs) {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}

// reserveRate takes inputs from the caller's and the global rate when
// both have them now, and from neither otherwise.
func (g *inferenceGate) reserveRate(limiter *rate.Limiter, inputs int) bool {
	now := time.Now()
	g.rateMu.Lock()
	defer g.rateMu.Unlock()
	callerReservation := limiter.ReserveN(now, inputs)
	if !callerReservation.OK() || callerReservation.DelayFrom(now) > 0 {
		if callerReservation.OK() {
			callerReservation.CancelAt(now)
		}
		return false
	}
	globalReservation := g.global.ReserveN(now, inputs)
	if !globalReservation.OK() || globalReservation.DelayFrom(now) > 0 {
		if globalReservation.OK() {
			globalReservation.CancelAt(now)
		}
		callerReservation.CancelAt(now)
		return false
	}
	return true
}

func takeSlot(ctx context.Context, slots chan struct{}, wait bool) bool {
	if wait {
		select {
		case slots <- struct{}{}:
			return true
		case <-ctx.Done():
			return false
		}
	}
	select {
	case slots <- struct{}{}:
		return true
	default:
		return false
	}
}
