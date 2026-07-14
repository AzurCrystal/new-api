package service

import (
	"testing"

	"github.com/QuantumNous/new-api/dto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplyResponsesUsagePreservesTokenDetails(t *testing.T) {
	src := &dto.Usage{
		InputTokens:  13,
		OutputTokens: 8,
		TotalTokens:  21,
		InputTokensDetails: &dto.InputTokenDetails{
			CachedTokens:     5,
			CacheWriteTokens: 2,
		},
		OutputTokensDetails: &dto.OutputTokenDetails{
			ReasoningTokens: 6,
			TextTokens:      2,
		},
	}
	dst := &dto.Usage{}

	ApplyResponsesUsage(dst, src)

	assert.Equal(t, 13, dst.PromptTokens)
	assert.Equal(t, 8, dst.CompletionTokens)
	assert.Equal(t, 21, dst.TotalTokens)
	assert.Equal(t, 5, dst.PromptTokensDetails.CachedTokens)
	assert.Equal(t, 2, dst.PromptTokensDetails.CacheWriteTokens)
	assert.Equal(t, 6, dst.CompletionTokenDetails.ReasoningTokens)
	require.NotNil(t, dst.OutputTokensDetails)
	assert.Equal(t, 2, dst.OutputTokensDetails.TextTokens)

	src.InputTokensDetails.CachedTokens = 99
	src.OutputTokensDetails.ReasoningTokens = 99
	assert.Equal(t, 5, dst.InputTokensDetails.CachedTokens)
	assert.Equal(t, 6, dst.OutputTokensDetails.ReasoningTokens)
}
