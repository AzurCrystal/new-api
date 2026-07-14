package middleware

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/common/limiter"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/setting"
	"github.com/QuantumNous/new-api/types"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

const (
	ModelRequestRateLimitCountMark        = "MRRL"
	ModelRequestRateLimitSuccessCountMark = "MRRLS"
	// OpenAI Responses WebSockets have a 60-minute hard lifetime. Keep pending
	// reservations well beyond that while still self-healing after a process
	// dies between reserve and completion.
	modelRequestSuccessReservationLease = 2 * time.Hour
)

type ModelRequestRateLimitCommit func(success bool)

const reserveRedisModelRequestSuccessScript = `
local cutoff_text = ARGV[1]
local max_count = tonumber(ARGV[2])
local now_millis = ARGV[3]
local lease_expires_millis = ARGV[4]
local token = ARGV[5]
local ttl_seconds = tonumber(ARGV[6])

local completed_count = 0
local completed = redis.call("LRANGE", KEYS[1], 0, -1)
for _, value in ipairs(completed) do
    if string.len(value) < string.len(cutoff_text) then
        completed_count = completed_count + 1
    else
        local timestamp = string.sub(value, 1, string.len(cutoff_text))
        if not string.match(timestamp, "^%d%d%d%d%-%d%d%-%d%dT%d%d:%d%d:%d%d%.%d%d%dZ$") or timestamp > cutoff_text then
            completed_count = completed_count + 1
        end
    end
end

redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", now_millis)
if completed_count + redis.call("ZCARD", KEYS[2]) >= max_count then
    return 0
end
redis.call("ZADD", KEYS[2], lease_expires_millis, token)
redis.call("EXPIRE", KEYS[2], ttl_seconds)
return 1
`

const finishRedisModelRequestSuccessScript = `
redis.call("ZREM", KEYS[2], ARGV[1])
if ARGV[2] == "1" then
    redis.call("LPUSH", KEYS[1], ARGV[3])
    redis.call("LTRIM", KEYS[1], 0, tonumber(ARGV[4]) - 1)
    redis.call("EXPIRE", KEYS[1], tonumber(ARGV[5]))
end
return 1
`

// 检查Redis中的请求限制
func checkRedisRateLimit(ctx context.Context, rdb *redis.Client, key string, maxCount int, duration int64) (bool, error) {
	// 如果maxCount为0，表示不限制
	if maxCount == 0 {
		return true, nil
	}

	// 获取当前计数
	length, err := rdb.LLen(ctx, key).Result()
	if err != nil {
		return false, err
	}

	// 如果未达到限制，允许请求
	if length < int64(maxCount) {
		return true, nil
	}

	// 检查时间窗口
	oldTimeStr, _ := rdb.LIndex(ctx, key, -1).Result()
	oldTime, err := time.Parse(timeFormat, oldTimeStr)
	if err != nil {
		return false, err
	}

	nowTimeStr := time.Now().Format(timeFormat)
	nowTime, err := time.Parse(timeFormat, nowTimeStr)
	if err != nil {
		return false, err
	}
	// 如果在时间窗口内已达到限制，拒绝请求
	subTime := nowTime.Sub(oldTime).Seconds()
	if int64(subTime) < duration {
		rdb.Expire(ctx, key, time.Duration(setting.ModelRequestRateLimitDurationMinutes)*time.Minute)
		return false, nil
	}

	return true, nil
}

// 记录Redis请求
func recordRedisRequest(ctx context.Context, rdb *redis.Client, key string, maxCount int) {
	// 如果maxCount为0，不记录请求
	if maxCount == 0 {
		return
	}

	now := time.Now().Format(timeFormat)
	rdb.LPush(ctx, key, now)
	rdb.LTrim(ctx, key, 0, int64(maxCount-1))
	rdb.Expire(ctx, key, time.Duration(setting.ModelRequestRateLimitDurationMinutes)*time.Minute)
}

func modelRequestRateLimitConfig(c *gin.Context) (duration int64, totalMaxCount int, successMaxCount int) {
	duration = int64(setting.ModelRequestRateLimitDurationMinutes * 60)
	totalMaxCount = setting.ModelRequestRateLimitCount
	successMaxCount = setting.ModelRequestRateLimitSuccessCount

	group := common.GetContextKeyString(c, constant.ContextKeyTokenGroup)
	if group == "" {
		group = common.GetContextKeyString(c, constant.ContextKeyUserGroup)
	}
	if groupTotalCount, groupSuccessCount, found := setting.GetGroupRateLimit(group); found {
		totalMaxCount = groupTotalCount
		successMaxCount = groupSuccessCount
	}
	return duration, totalMaxCount, successMaxCount
}

func newModelRateLimitError(message string, statusCode int) *types.NewAPIError {
	return types.NewErrorWithStatusCode(
		fmt.Errorf("%s", message),
		types.ErrorCodeInvalidRequest,
		statusCode,
		types.ErrOptionWithSkipRetry(),
		types.ErrOptionWithNoRecordErrorLog(),
	)
}

// reserveRedisModelRequestSuccess holds one potential-success slot across a
// long-running request. A Lua script makes the legacy completed list and the
// pending set one admission decision; completion atomically converts or
// releases the reservation.
func reserveRedisModelRequestSuccess(
	ctx context.Context,
	rdb *redis.Client,
	completedKey string,
	maxCount int,
	duration int64,
) (ModelRequestRateLimitCommit, bool, error) {
	if maxCount == 0 {
		return func(bool) {}, true, nil
	}
	if duration <= 0 {
		duration = 1
	}
	pendingKey := completedKey + ":pending"
	token := uuid.NewString()
	now := time.Now()
	cutoff := now.Add(-time.Duration(duration) * time.Second)
	leaseExpires := now.Add(modelRequestSuccessReservationLease)
	leaseSeconds := int64(modelRequestSuccessReservationLease / time.Second)
	allowed, err := rdb.Eval(
		ctx,
		reserveRedisModelRequestSuccessScript,
		[]string{completedKey, pendingKey},
		cutoff.Format(timeFormat),
		maxCount,
		now.UnixMilli(),
		leaseExpires.UnixMilli(),
		token,
		leaseSeconds,
	).Int()
	if err != nil {
		return nil, false, err
	}
	if allowed != 1 {
		return nil, false, nil
	}

	var once sync.Once
	commit := func(success bool) {
		once.Do(func() {
			commitCtx := context.Background()
			successFlag := 0
			if success {
				successFlag = 1
			}
			if err := rdb.Eval(
				commitCtx,
				finishRedisModelRequestSuccessScript,
				[]string{completedKey, pendingKey},
				token,
				successFlag,
				time.Now().Format(timeFormat),
				maxCount,
				duration,
			).Err(); err != nil {
				logger.LogError(commitCtx, "model request rate limit reservation commit failed: "+err.Error())
			}
		})
	}
	return commit, true, nil
}

// CheckModelRequestRateLimit reserves a total-request slot and returns a
// callback that records the success counter after the turn reaches a terminal
// state. WebSocket warmups do not call this function.
func CheckModelRequestRateLimit(c *gin.Context) (ModelRequestRateLimitCommit, *types.NewAPIError) {
	if !setting.ModelRequestRateLimitEnabled {
		return func(bool) {}, nil
	}

	duration, totalMaxCount, successMaxCount := modelRequestRateLimitConfig(c)
	userID := strconv.Itoa(c.GetInt("id"))
	if common.RedisEnabled {
		ctx := context.Background()
		rdb := common.RDB
		successKey := fmt.Sprintf("rateLimit:%s:%s", ModelRequestRateLimitSuccessCountMark, userID)
		commit, allowed, err := reserveRedisModelRequestSuccess(ctx, rdb, successKey, successMaxCount, duration)
		if err != nil {
			return nil, newModelRateLimitError("rate_limit_check_failed", http.StatusInternalServerError)
		}
		if !allowed {
			return nil, newModelRateLimitError(fmt.Sprintf("您已达到请求数限制：%d分钟内最多请求%d次", setting.ModelRequestRateLimitDurationMinutes, successMaxCount), http.StatusTooManyRequests)
		}

		if totalMaxCount > 0 {
			totalKey := fmt.Sprintf("rateLimit:%s", userID)
			tb := limiter.New(ctx, rdb)
			allowed, err = tb.Allow(
				ctx,
				totalKey,
				limiter.WithCapacity(int64(totalMaxCount)*duration),
				limiter.WithRate(int64(totalMaxCount)),
				limiter.WithRequested(duration),
			)
			if err != nil {
				commit(false)
				return nil, newModelRateLimitError("rate_limit_check_failed", http.StatusInternalServerError)
			}
			if !allowed {
				commit(false)
				return nil, newModelRateLimitError(fmt.Sprintf("您已达到总请求数限制：%d分钟内最多请求%d次，包括失败次数，请检查您的请求是否正确", setting.ModelRequestRateLimitDurationMinutes, totalMaxCount), http.StatusTooManyRequests)
			}
		}

		return commit, nil
	}

	inMemoryRateLimiter.Init(time.Duration(setting.ModelRequestRateLimitDurationMinutes) * time.Minute)
	totalKey := ModelRequestRateLimitCountMark + userID
	successKey := ModelRequestRateLimitSuccessCountMark + userID
	if totalMaxCount > 0 && !inMemoryRateLimiter.Request(totalKey, totalMaxCount, duration) {
		return nil, newModelRateLimitError(fmt.Sprintf("您已达到总请求数限制：%d分钟内最多请求%d次，包括失败次数，请检查您的请求是否正确", setting.ModelRequestRateLimitDurationMinutes, totalMaxCount), http.StatusTooManyRequests)
	}
	commit, allowed := inMemoryRateLimiter.Reserve(successKey, successMaxCount, duration)
	if !allowed {
		return nil, newModelRateLimitError(fmt.Sprintf("您已达到请求数限制：%d分钟内最多请求%d次", setting.ModelRequestRateLimitDurationMinutes, successMaxCount), http.StatusTooManyRequests)
	}

	return commit, nil
}

func isResponsesWebSocketHandshake(c *gin.Context) bool {
	return c != nil &&
		c.Request != nil &&
		c.Request.Method == http.MethodGet &&
		c.Request.URL != nil &&
		c.Request.URL.Path == "/v1/responses" &&
		strings.EqualFold(c.Request.Header.Get("Upgrade"), "websocket")
}

// Redis限流处理器
func redisRateLimitHandler(duration int64, totalMaxCount, successMaxCount int) gin.HandlerFunc {
	return func(c *gin.Context) {
		userId := strconv.Itoa(c.GetInt("id"))
		ctx := context.Background()
		rdb := common.RDB

		// 1. 检查成功请求数限制
		successKey := fmt.Sprintf("rateLimit:%s:%s", ModelRequestRateLimitSuccessCountMark, userId)
		allowed, err := checkRedisRateLimit(ctx, rdb, successKey, successMaxCount, duration)
		if err != nil {
			fmt.Println("检查成功请求数限制失败:", err.Error())
			abortWithOpenAiMessage(c, http.StatusInternalServerError, "rate_limit_check_failed")
			return
		}
		if !allowed {
			abortWithOpenAiMessage(c, http.StatusTooManyRequests, fmt.Sprintf("您已达到请求数限制：%d分钟内最多请求%d次", setting.ModelRequestRateLimitDurationMinutes, successMaxCount))
			return
		}

		//2.检查总请求数限制并记录总请求（当totalMaxCount为0时会自动跳过，使用令牌桶限流器
		if totalMaxCount > 0 {
			totalKey := fmt.Sprintf("rateLimit:%s", userId)
			// 初始化
			tb := limiter.New(ctx, rdb)
			allowed, err = tb.Allow(
				ctx,
				totalKey,
				limiter.WithCapacity(int64(totalMaxCount)*duration),
				limiter.WithRate(int64(totalMaxCount)),
				limiter.WithRequested(duration),
			)

			if err != nil {
				fmt.Println("检查总请求数限制失败:", err.Error())
				abortWithOpenAiMessage(c, http.StatusInternalServerError, "rate_limit_check_failed")
				return
			}

			if !allowed {
				abortWithOpenAiMessage(c, http.StatusTooManyRequests, fmt.Sprintf("您已达到总请求数限制：%d分钟内最多请求%d次，包括失败次数，请检查您的请求是否正确", setting.ModelRequestRateLimitDurationMinutes, totalMaxCount))
			}
		}

		// 4. 处理请求
		c.Next()

		// 5. 如果请求成功，记录成功请求
		if c.Writer.Status() < 400 {
			recordRedisRequest(ctx, rdb, successKey, successMaxCount)
		}
	}
}

// 内存限流处理器
func memoryRateLimitHandler(duration int64, totalMaxCount, successMaxCount int) gin.HandlerFunc {
	inMemoryRateLimiter.Init(time.Duration(setting.ModelRequestRateLimitDurationMinutes) * time.Minute)

	return func(c *gin.Context) {
		userId := strconv.Itoa(c.GetInt("id"))
		totalKey := ModelRequestRateLimitCountMark + userId
		successKey := ModelRequestRateLimitSuccessCountMark + userId

		// 1. 检查总请求数限制（当totalMaxCount为0时跳过）
		if totalMaxCount > 0 && !inMemoryRateLimiter.Request(totalKey, totalMaxCount, duration) {
			c.Status(http.StatusTooManyRequests)
			c.Abort()
			return
		}

		// 2. 检查成功请求数限制
		// 使用一个临时key来检查限制，这样可以避免实际记录
		checkKey := successKey + "_check"
		if !inMemoryRateLimiter.Request(checkKey, successMaxCount, duration) {
			c.Status(http.StatusTooManyRequests)
			c.Abort()
			return
		}

		// 3. 处理请求
		c.Next()

		// 4. 如果请求成功，记录到实际的成功请求计数中
		if c.Writer.Status() < 400 {
			inMemoryRateLimiter.Request(successKey, successMaxCount, duration)
		}
	}
}

// ModelRequestRateLimit 模型请求限流中间件
func ModelRequestRateLimit() func(c *gin.Context) {
	return func(c *gin.Context) {
		if isResponsesWebSocketHandshake(c) {
			c.Next()
			return
		}
		commit, apiErr := CheckModelRequestRateLimit(c)
		if apiErr != nil {
			abortWithOpenAiMessage(c, apiErr.StatusCode, apiErr.Error(), apiErr.GetErrorCode())
			return
		}
		completed := false
		defer func() {
			commit(completed && c.Writer.Status() < http.StatusBadRequest)
		}()
		c.Next()
		completed = true
	}
}
