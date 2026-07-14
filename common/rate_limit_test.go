package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryRateLimiterCheckDoesNotConsume(t *testing.T) {
	limiter := &InMemoryRateLimiter{}
	limiter.Init(time.Minute)

	require.True(t, limiter.Check("user", 1, 60))
	require.True(t, limiter.Check("user", 1, 60))
	require.True(t, limiter.Request("user", 1, 60))
	assert.False(t, limiter.Check("user", 1, 60))
}

func TestInMemoryRateLimiterZeroLimitIsDisabled(t *testing.T) {
	limiter := &InMemoryRateLimiter{}
	limiter.Init(time.Minute)

	require.True(t, limiter.Check("user", 0, 60))
	require.True(t, limiter.Request("user", 0, 60))
	assert.Empty(t, limiter.store)
}

func TestInMemoryRateLimiterReservationIsAtomicAndReleasable(t *testing.T) {
	limiter := &InMemoryRateLimiter{}
	limiter.Init(time.Minute)

	finish, allowed := limiter.Reserve("user", 1, 60)
	require.True(t, allowed)
	require.NotNil(t, finish)
	_, allowed = limiter.Reserve("user", 1, 60)
	assert.False(t, allowed, "an in-flight request must hold the final success slot")

	finish(false)
	finish(false)
	finish, allowed = limiter.Reserve("user", 1, 60)
	require.True(t, allowed, "a failed request must release its reservation exactly once")
	finish(true)

	_, allowed = limiter.Reserve("user", 1, 60)
	assert.False(t, allowed, "a successful request must retain the slot")
}

func TestInMemoryRateLimiterActiveReservationOutlivesCompletedWindow(t *testing.T) {
	limiter := &InMemoryRateLimiter{}
	limiter.Init(time.Minute)

	finish, allowed := limiter.Reserve("long-turn", 1, 0)
	require.True(t, allowed)
	_, allowed = limiter.Reserve("long-turn", 1, 0)
	assert.False(t, allowed, "the completed-entry window must not expire an active turn")

	finish(false)
	finish, allowed = limiter.Reserve("long-turn", 1, 0)
	assert.True(t, allowed)
	finish(false)
}
