package openai

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	relayconstant "github.com/QuantumNous/new-api/relay/constant"
	"github.com/QuantumNous/new-api/types"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newOaiStreamHandlerTestContext(body string, channelType int, model string) (*gin.Context, *httptest.ResponseRecorder, *http.Response, *relaycommon.RelayInfo) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	c.Set(common.RequestIdKey, "opencode-cache-usage-test")

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
	}
	info := &relaycommon.RelayInfo{
		ChannelMeta: &relaycommon.ChannelMeta{
			ChannelType:       channelType,
			UpstreamModelName: model,
		},
		IsStream:           true,
		RelayMode:          relayconstant.RelayModeChatCompletions,
		RelayFormat:        types.RelayFormatOpenAI,
		ShouldIncludeUsage: true,
		DisablePing:        true,
	}
	return c, recorder, resp, info
}

func TestOaiStreamHandlerPreservesUsageBeforeTrailingMetadata(t *testing.T) {
	oldMode := gin.Mode()
	gin.SetMode(gin.TestMode)
	t.Cleanup(func() { gin.SetMode(oldMode) })

	oldTimeout := constant.StreamingTimeout
	constant.StreamingTimeout = 30
	t.Cleanup(func() { constant.StreamingTimeout = oldTimeout })

	usageChunk := `data: {"id":"chatcmpl_cache","object":"chat.completion.chunk","created":1710000000,"model":"deepseek-v4-flash","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":6090,"completion_tokens":4,"total_tokens":6094,"prompt_cache_hit_tokens":6016,"prompt_cache_miss_tokens":74,"prompt_tokens_details":{"cached_tokens":6016}}}`
	tests := []struct {
		name string
		tail []string
	}{
		{
			name: "cost chunk before done",
			tail: []string{
				`data: {"choices":[],"cost":"0.00003"}`,
				`data: [DONE]`,
			},
		},
		{
			name: "multiple metadata chunks before done",
			tail: []string{
				`data: {"choices":[],"x-opencode-type":"cost"}`,
				`data: {"choices":[],"cost":"0.00003"}`,
				`data: [DONE]`,
			},
		},
		{
			name: "cost chunk before eof",
			tail: []string{
				`data: {"choices":[],"cost":"0.00003"}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lines := []string{
				`data: {"id":"chatcmpl_cache","object":"chat.completion.chunk","created":1710000000,"model":"deepseek-v4-flash","choices":[{"index":0,"delta":{"content":"OK"},"finish_reason":null}]}`,
				usageChunk,
			}
			lines = append(lines, tt.tail...)
			lines = append(lines, "")

			c, recorder, resp, info := newOaiStreamHandlerTestContext(strings.Join(lines, "\n"), constant.ChannelTypeAdvancedCustom, "deepseek-v4-flash")

			usage, err := OaiStreamHandler(c, info, resp)

			require.Nil(t, err)
			require.NotNil(t, usage)
			assert.Equal(t, 6090, usage.PromptTokens)
			assert.Equal(t, 4, usage.CompletionTokens)
			assert.Equal(t, 6094, usage.TotalTokens)
			assert.Equal(t, 6016, usage.PromptTokensDetails.CachedTokens)
			assert.False(t, common.GetContextKeyBool(c, constant.ContextKeyLocalCountTokens))

			got := recorder.Body.String()
			assert.Contains(t, got, `"content":"OK"`)
			assert.Contains(t, got, `"cached_tokens":6016`)
			assert.Contains(t, got, `"cost":"0.00003"`)
			assert.Contains(t, got, `data: [DONE]`)
			assert.Equal(t, 1, strings.Count(got, `"usage":`))
			requireOrderedSubstrings(t, got, `"cached_tokens":6016`, `"cost":"0.00003"`, `data: [DONE]`)
		})
	}
}

func TestOaiStreamHandlerRetainsTailUsagePostProcessing(t *testing.T) {
	oldMode := gin.Mode()
	gin.SetMode(gin.TestMode)
	t.Cleanup(func() { gin.SetMode(oldMode) })

	oldTimeout := constant.StreamingTimeout
	constant.StreamingTimeout = 30
	t.Cleanup(func() { constant.StreamingTimeout = oldTimeout })

	body := strings.Join([]string{
		`data: {"id":"chatcmpl_cache","object":"chat.completion.chunk","created":1710000000,"model":"llama-test","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":100,"completion_tokens":4,"total_tokens":104}}`,
		`data: {"choices":[],"timings":{"cache_n":64}}`,
		`data: [DONE]`,
		``,
	}, "\n")
	c, _, resp, info := newOaiStreamHandlerTestContext(body, constant.ChannelTypeOpenAI, "llama-test")

	usage, err := OaiStreamHandler(c, info, resp)

	require.Nil(t, err)
	require.NotNil(t, usage)
	assert.Equal(t, 100, usage.PromptTokens)
	assert.Equal(t, 64, usage.PromptTokensDetails.CachedTokens)
	assert.False(t, common.GetContextKeyBool(c, constant.ContextKeyLocalCountTokens))
}
