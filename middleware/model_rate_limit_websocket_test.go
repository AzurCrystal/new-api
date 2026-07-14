package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

func TestIsResponsesWebSocketHandshake(t *testing.T) {
	tests := []struct {
		name    string
		method  string
		path    string
		upgrade string
		want    bool
	}{
		{name: "responses websocket", method: http.MethodGet, path: "/v1/responses", upgrade: "WebSocket", want: true},
		{name: "ordinary responses get", method: http.MethodGet, path: "/v1/responses", want: false},
		{name: "responses post", method: http.MethodPost, path: "/v1/responses", upgrade: "websocket", want: false},
		{name: "realtime websocket", method: http.MethodGet, path: "/v1/realtime", upgrade: "websocket", want: false},
	}

	gin.SetMode(gin.TestMode)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
			ctx.Request = httptest.NewRequest(test.method, test.path, nil)
			if test.upgrade != "" {
				ctx.Request.Header.Set("Upgrade", test.upgrade)
			}

			assert.Equal(t, test.want, isResponsesWebSocketHandshake(ctx))
		})
	}
}
