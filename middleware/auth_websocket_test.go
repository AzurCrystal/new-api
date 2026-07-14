package middleware

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplyWebSocketSubprotocolAuthorization(t *testing.T) {
	tests := []struct {
		name          string
		protocols     string
		authorization string
		wantApplied   bool
		wantAuth      string
	}{
		{
			name:          "embedded api key wins",
			protocols:     "responses, openai-insecure-api-key.sk-ws-token",
			authorization: "Bearer old-token",
			wantApplied:   true,
			wantAuth:      "Bearer sk-ws-token",
		},
		{
			name:          "ordinary responses protocol preserves bearer",
			protocols:     "responses",
			authorization: "Bearer header-token",
			wantApplied:   false,
			wantAuth:      "Bearer header-token",
		},
		{
			name:          "empty embedded key preserves bearer",
			protocols:     "responses, openai-insecure-api-key.",
			authorization: "Bearer header-token",
			wantApplied:   false,
			wantAuth:      "Bearer header-token",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			header := make(http.Header)
			header.Set("Sec-WebSocket-Protocol", test.protocols)
			header.Set("Authorization", test.authorization)

			applied := applyWebSocketSubprotocolAuthorization(header)

			assert.Equal(t, test.wantApplied, applied)
			require.Equal(t, test.wantAuth, header.Get("Authorization"))
		})
	}
}
