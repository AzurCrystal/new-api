package service

import "github.com/QuantumNous/new-api/dto"

// ApplyResponsesUsage normalizes a Responses API usage object into the fields
// consumed by new-api's shared text billing path.
func ApplyResponsesUsage(dst *dto.Usage, src *dto.Usage) {
	if dst == nil || src == nil {
		return
	}
	dst.PromptTokens = src.InputTokens
	dst.CompletionTokens = src.OutputTokens
	dst.TotalTokens = src.TotalTokens
	dst.InputTokens = src.InputTokens
	dst.OutputTokens = src.OutputTokens
	if src.InputTokensDetails != nil {
		details := *src.InputTokensDetails
		dst.InputTokensDetails = &details
		dst.PromptTokensDetails = details
	} else if src.PromptTokensDetails != (dto.InputTokenDetails{}) {
		dst.PromptTokensDetails = src.PromptTokensDetails
	}
	if src.OutputTokensDetails != nil {
		details := *src.OutputTokensDetails
		dst.CompletionTokenDetails = details
		dst.OutputTokensDetails = &details
	} else if src.CompletionTokenDetails != (dto.OutputTokenDetails{}) {
		dst.CompletionTokenDetails = src.CompletionTokenDetails
	}
	dst.PromptCacheHitTokens = src.PromptCacheHitTokens
	dst.UsageSemantic = src.UsageSemantic
	dst.UsageSource = src.UsageSource
	dst.BillingUsage = src.BillingUsage
}
