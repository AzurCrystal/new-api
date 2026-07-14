package relay

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	appconstant "github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/dto"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	relayconstant "github.com/QuantumNous/new-api/relay/constant"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseResponsesWSCreateRequiresBooleanGenerate(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
	}{
		{name: "null", value: "null"},
		{name: "number", value: "1"},
		{name: "string", value: `"false"`},
		{name: "object", value: `{}`},
		{name: "array", value: `[]`},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload := []byte(fmt.Sprintf(`{"type":"response.create","model":"gpt-5","input":"hi","generate":%s}`, test.value))
			_, err := parseResponsesWSCreate(payload)
			require.EqualError(t, err, "generate must be a boolean")
		})
	}

	create, err := parseResponsesWSCreate([]byte(`{"type":"response.create","model":"gpt-5","input":"hi","generate":false}`))
	require.NoError(t, err)
	assert.True(t, create.hasGenerate)
	assert.False(t, create.generate)

	_, err = parseResponsesWSCreate([]byte(`{"type":"response.create","generate":true,"response":{"model":"gpt-5","input":"hi","generate":null}}`))
	require.EqualError(t, err, "generate must be a boolean")

	_, err = parseResponsesWSCreate([]byte(`{"type":"response.create","response":null}`))
	require.EqualError(t, err, "response must be an object")
}

func TestBuildResponsesWSPayloadFlattensLegacyShapeAndPreservesUnknownFields(t *testing.T) {
	payload := []byte(`{
		"type":"response.create",
		"event_id":"outer-event",
		"outer_unknown":{"keep":true},
		"collision":"outer",
		"background":true,
		"response":{
			"model":"gpt-5-high",
			"input":"hello",
			"generate":false,
			"inner_unknown":[1,2,3],
			"collision":"inner",
			"event_id":"inner-event",
			"background":true,
			"stream":true,
			"stream_options":{"include_usage":true}
		}
	}`)
	create, err := parseResponsesWSCreate(payload)
	require.NoError(t, err)
	require.NoError(t, (&responsesWSSession{}).normalizeCreateRequest(&create))

	c, info := newResponsesWSBuildContext("gpt-5-high")
	result, apiErr := buildResponsesWSPayload(c, info, create)
	require.Nil(t, apiErr)

	var got map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(result, &got))
	for _, field := range responsesWSUnsupportedCreateFields {
		assert.NotContains(t, got, field)
	}
	assert.JSONEq(t, `{"keep":true}`, string(got["outer_unknown"]))
	assert.JSONEq(t, `[1,2,3]`, string(got["inner_unknown"]))
	assert.JSONEq(t, `"inner"`, string(got["collision"]))
	assert.JSONEq(t, `false`, string(got["generate"]))
	assert.JSONEq(t, `"gpt-5"`, string(got["model"]))
	assert.JSONEq(t, `{"effort":"high"}`, string(got["reasoning"]))
	assert.Equal(t, "high", info.ReasoningEffort)
}

func TestNormalizeResponsesWSCreatePayloadRejectsOverrideOfProtocolFields(t *testing.T) {
	result, err := normalizeResponsesWSCreatePayload([]byte(`{
		"type":"not-response-create",
		"model":"gpt-5",
		"input":"hi",
		"unknown":"kept",
		"response":{},
		"event_id":"evt",
		"background":true,
		"stream":true,
		"stream_options":{}
	}`))
	require.NoError(t, err)

	var got map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(result, &got))
	assert.JSONEq(t, `"response.create"`, string(got["type"]))
	assert.JSONEq(t, `"kept"`, string(got["unknown"]))
	for _, field := range responsesWSUnsupportedCreateFields {
		assert.NotContains(t, got, field)
	}

	_, err = normalizeResponsesWSCreatePayload([]byte(`{"type":"response.create","generate":null}`))
	require.EqualError(t, err, "generate must be a boolean")
}

func TestResponsesWSSessionFollowupCreateCanOmitModel(t *testing.T) {
	create, err := parseResponsesWSCreate([]byte(`{
		"type":"response.create",
		"input":"follow up",
		"previous_response_id":"resp_1",
		"future_field":{"keep":1}
	}`))
	require.NoError(t, err)
	session := &responsesWSSession{lockedModel: "gpt-client"}
	require.NoError(t, session.normalizeCreateRequest(&create))
	assert.Equal(t, "gpt-client", create.request.Model)

	c, info := newResponsesWSBuildContext("gpt-client")
	c.Set("model_mapping", `{"gpt-client":"gpt-upstream"}`)
	result, apiErr := buildResponsesWSPayload(c, info, create)
	require.Nil(t, apiErr)

	var got map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(result, &got))
	assert.JSONEq(t, `"gpt-upstream"`, string(got["model"]))
	assert.JSONEq(t, `{"keep":1}`, string(got["future_field"]))

	first, err := parseResponsesWSCreate([]byte(`{"type":"response.create","input":"first"}`))
	require.NoError(t, err)
	require.EqualError(t, (&responsesWSSession{}).normalizeCreateRequest(&first), "model is required on the first response.create")

	different, err := parseResponsesWSCreate([]byte(`{"type":"response.create","model":"other","input":"follow up"}`))
	require.NoError(t, err)
	require.ErrorContains(t, session.normalizeCreateRequest(&different), `locked to model "gpt-client"`)
}

func TestResponsesWSValidationAllowsStateOnlyFrames(t *testing.T) {
	t.Run("warmup without input", func(t *testing.T) {
		create, err := parseResponsesWSCreate([]byte(`{"type":"response.create","model":"gpt-5","generate":false}`))
		require.NoError(t, err)
		require.NoError(t, (&responsesWSSession{}).normalizeCreateRequest(&create))

		c, info := newResponsesWSBuildContext("gpt-5")
		payload, apiErr := buildResponsesWSPayload(c, info, create)
		require.Nil(t, apiErr)
		require.NoError(t, validateResponsesWSUpstreamPayload(payload))
	})

	t.Run("continuation without input", func(t *testing.T) {
		create, err := parseResponsesWSCreate([]byte(`{"type":"response.create","previous_response_id":"resp_1"}`))
		require.NoError(t, err)
		require.NoError(t, (&responsesWSSession{lockedModel: "gpt-5"}).normalizeCreateRequest(&create))

		c, info := newResponsesWSBuildContext("gpt-5")
		payload, apiErr := buildResponsesWSPayload(c, info, create)
		require.Nil(t, apiErr)
		require.NoError(t, validateResponsesWSUpstreamPayload(payload))
	})

	create, err := parseResponsesWSCreate([]byte(`{"type":"response.create","model":"gpt-5"}`))
	require.NoError(t, err)
	require.EqualError(t, (&responsesWSSession{}).normalizeCreateRequest(&create), "input is required")
}

func TestResponsesWSConcurrentCreateClosesClientAndDrainsCurrentTurn(t *testing.T) {
	assertResponsesWSConcurrentCreateCloses(t, `{"type":"response.create","model":"gpt-5","input":"second"}`, "")
}

func TestResponsesWSConcurrentCreateClosesBeforePayloadValidation(t *testing.T) {
	for _, test := range []struct {
		name    string
		payload string
		eventID string
	}{
		{name: "different model", payload: `{"type":"response.create","model":"other","input":"second","event_id":"evt-model"}`, eventID: "evt-model"},
		{name: "missing input", payload: `{"type":"response.create","model":"gpt-5","event_id":"evt-input"}`, eventID: "evt-input"},
		{name: "invalid generate", payload: `{"type":"response.create","model":"gpt-5","input":"second","generate":null,"event_id":"evt-generate"}`, eventID: "evt-generate"},
	} {
		t.Run(test.name, func(t *testing.T) {
			assertResponsesWSConcurrentCreateCloses(t, test.payload, test.eventID)
		})
	}
}

func assertResponsesWSConcurrentCreateCloses(t *testing.T, payload, eventID string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &responsesWSPeer{
		ctx:      ctx,
		outbound: make(chan responsesWSWrite, 2),
	}
	turn := &responsesWSTurn{}
	session := &responsesWSSession{
		client:      client,
		current:     turn,
		lockedModel: "gpt-5",
	}
	writes := make(chan responsesWSWrite, 2)
	go func() {
		for i := 0; i < 2; i++ {
			write := <-client.outbound
			write.payload = append([]byte(nil), write.payload...)
			writes <- write
			write.result <- nil
		}
	}()

	closeSession := session.handleClientFrame(responsesWSFrame{
		messageType: websocket.TextMessage,
		payload:     []byte(payload),
	})
	assert.False(t, closeSession, "the upstream turn must remain alive for usage drain")
	assert.True(t, session.clientGone)
	assert.Same(t, turn, session.current)
	require.NotNil(t, session.drainTimer)
	defer session.drainTimer.Stop()

	localError := <-writes
	assert.False(t, localError.control)
	var errorEvent responsesWSErrorEvent
	require.NoError(t, json.Unmarshal(localError.payload, &errorEvent))
	assert.Equal(t, "error", errorEvent.Type)
	assert.Equal(t, http.StatusConflict, errorEvent.Status)
	assert.Equal(t, eventID, errorEvent.EventID)
	require.NotNil(t, errorEvent.Error)
	assert.Equal(t, "invalid_request", fmt.Sprint(errorEvent.Error.Code))
	closeFrame := <-writes
	assert.True(t, closeFrame.control)
	require.GreaterOrEqual(t, len(closeFrame.payload), 2)
	assert.Equal(t, websocket.ClosePolicyViolation, int(binary.BigEndian.Uint16(closeFrame.payload[:2])))
}

func TestResponsesWSTerminalEventsPreserveUsage(t *testing.T) {
	terminalTypes := []string{
		"response.completed",
		"response.done",
		"response.failed",
		"response.incomplete",
		"response.cancelled",
		"response.canceled",
	}
	for _, eventType := range terminalTypes {
		t.Run(eventType, func(t *testing.T) {
			var rateSuccess bool
			var rateCalls int
			turn := &responsesWSTurn{
				info:  &relaycommon.RelayInfo{},
				usage: &dto.Usage{},
				commitRate: func(success bool) {
					rateSuccess = success
					rateCalls++
				},
			}
			session := &responsesWSSession{current: turn}
			payload := []byte(fmt.Sprintf(`{
				"type":%q,
				"response":{"usage":{
					"input_tokens":11,
					"output_tokens":7,
					"total_tokens":18,
					"input_tokens_details":{"cached_tokens":3,"cache_write_tokens":2},
					"output_tokens_details":{"reasoning_tokens":5,"text_tokens":2}
				}}
			}`, eventType))

			terminal, closeSession := session.observeUpstreamFrame(payload)
			assert.True(t, terminal)
			assert.False(t, closeSession)
			assert.Nil(t, session.current)
			assert.True(t, session.upstreamTerminalForwarded)
			assert.Equal(t, 1, rateCalls)
			assert.Equal(t, eventType == "response.completed" || eventType == "response.done", rateSuccess)
			assert.Equal(t, 11, turn.usage.PromptTokens)
			assert.Equal(t, 7, turn.usage.CompletionTokens)
			assert.Equal(t, 18, turn.usage.TotalTokens)
			assert.Equal(t, 3, turn.usage.PromptTokensDetails.CachedTokens)
			assert.Equal(t, 2, turn.usage.PromptTokensDetails.CacheWriteTokens)
			assert.Equal(t, 5, turn.usage.CompletionTokenDetails.ReasoningTokens)
			require.NotNil(t, turn.usage.OutputTokensDetails)
			assert.Equal(t, 2, turn.usage.OutputTokensDetails.TextTokens)
		})
	}
}

func TestResponsesWSSessionHandlesIdleUpstreamErrorsWithoutDuplicateCloseError(t *testing.T) {
	for _, test := range []struct {
		code      string
		wantClose bool
	}{
		{code: "websocket_connection_limit_reached", wantClose: true},
		{code: "previous_response_not_found", wantClose: true},
		{code: "rate_limit_exceeded", wantClose: false},
	} {
		t.Run(test.code, func(t *testing.T) {
			session := &responsesWSSession{}
			payload := []byte(fmt.Sprintf(`{"type":"error","status":429,"error":{"message":"upstream","type":"invalid_request_error","param":"","code":%q}}`, test.code))
			terminal, closeSession := session.observeUpstreamFrame(payload)
			assert.True(t, terminal)
			assert.Equal(t, test.wantClose, closeSession)
			assert.True(t, session.upstreamTerminalForwarded)
			assert.False(t, session.shouldReportUpstreamClose(), "a close after the forwarded error must not create bad_response")
		})
	}

	turn := &responsesWSTurn{info: &relaycommon.RelayInfo{}, usage: &dto.Usage{}}
	session := &responsesWSSession{current: turn}
	terminal, closeSession := session.observeUpstreamFrame([]byte(`{"type":"error","status":429,"error":{"message":"upstream","type":"invalid_request_error","param":"","code":"rate_limit_exceeded"}}`))
	assert.True(t, terminal)
	assert.False(t, closeSession)
	assert.Nil(t, session.current)
	assert.False(t, session.shouldReportUpstreamClose())

	session.current = &responsesWSTurn{}
	assert.True(t, session.shouldReportUpstreamClose(), "an in-flight close still requires a local error")
}

func TestResponsesWSSessionForwardsUpstreamErrorOnceBeforeExpectedClose(t *testing.T) {
	for _, test := range []struct {
		name       string
		code       string
		withTurn   bool
		closeFrame bool
	}{
		{name: "idle soft limit", code: "websocket_connection_limit_reached"},
		{name: "turn lineage error", code: "previous_response_not_found", withTurn: true},
		{name: "turn rate limit then close", code: "rate_limit_exceeded", withTurn: true, closeFrame: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := &responsesWSPeer{
				ctx:      ctx,
				inbound:  make(chan responsesWSFrame),
				outbound: make(chan responsesWSWrite, 2),
			}
			target := &responsesWSPeer{
				ctx:     ctx,
				inbound: make(chan responsesWSFrame, 2),
			}
			session := &responsesWSSession{ctx: ctx, cancel: cancel, client: client, target: target}
			if test.withTurn {
				session.current = &responsesWSTurn{info: &relaycommon.RelayInfo{}, usage: &dto.Usage{}}
			}

			payload := []byte(fmt.Sprintf(`{"type":"error","status":429,"error":{"message":"upstream","type":"invalid_request_error","param":"","code":%q}}`, test.code))
			target.inbound <- responsesWSFrame{messageType: websocket.TextMessage, payload: payload}
			if test.closeFrame {
				target.inbound <- responsesWSFrame{err: errors.New("expected upstream close")}
			}

			writes := make(chan responsesWSWrite, 2)
			writerDone := make(chan struct{})
			go func() {
				defer close(writerDone)
				for {
					select {
					case <-ctx.Done():
						return
					case write := <-client.outbound:
						record := write
						record.payload = append([]byte(nil), write.payload...)
						writes <- record
						write.result <- nil
					}
				}
			}()

			require.Nil(t, session.run())
			cancel()
			<-writerDone
			close(writes)
			var got []responsesWSWrite
			for write := range writes {
				got = append(got, write)
			}
			require.Len(t, got, 2, "the session should emit one data error and one close control")
			assert.False(t, got[0].control)
			assert.Equal(t, websocket.TextMessage, got[0].messageType)
			assert.Equal(t, payload, got[0].payload, "the upstream error must be forwarded byte-for-byte")
			assert.True(t, got[1].control)
			assert.Equal(t, websocket.CloseMessage, got[1].messageType)
			require.GreaterOrEqual(t, len(got[1].payload), 2)
			wantCode := websocket.CloseInternalServerErr
			if test.code == "websocket_connection_limit_reached" {
				wantCode = websocket.CloseTryAgainLater
			}
			assert.Equal(t, wantCode, int(binary.BigEndian.Uint16(got[1].payload[:2])))
		})
	}
}

func TestResponsesWSWarmupSkipsSettlement(t *testing.T) {
	billing := &responsesWSTestBilling{}
	turn := &responsesWSTurn{
		info:     &relaycommon.RelayInfo{Billing: billing},
		usage:    &dto.Usage{PromptTokens: 100, CompletionTokens: 50},
		generate: false,
	}
	session := &responsesWSSession{current: turn}
	session.finishCurrent(turn, true)

	assert.Nil(t, session.current)
	assert.Zero(t, billing.settleCalls.Load())
	assert.Zero(t, billing.refundCalls.Load())
}

func TestResponsesWSClientDisconnectDrainsTerminalUsage(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	turn := &responsesWSTurn{info: &relaycommon.RelayInfo{}, usage: &dto.Usage{}}
	session := &responsesWSSession{baseCtx: c, current: turn}
	session.markClientGone(&websocket.CloseError{Code: websocket.CloseNormalClosure})
	require.True(t, session.clientGone)
	require.NotNil(t, session.drainTimer)
	assert.Same(t, turn, session.current)

	terminal, _ := session.observeUpstreamFrame([]byte(`{
		"type":"response.completed",
		"response":{"usage":{"input_tokens":4,"output_tokens":2,"total_tokens":6}}
	}`))
	assert.True(t, terminal)
	assert.Nil(t, session.current)
	assert.Equal(t, 6, turn.usage.TotalTokens)
	session.drainTimer.Stop()
}

func TestResponsesWSShutdownReleasesCurrentRateReservation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var calls int
	var success bool
	turn := &responsesWSTurn{
		commitRate: func(value bool) {
			calls++
			success = value
		},
	}
	session := &responsesWSSession{ctx: ctx, cancel: cancel, current: turn}

	session.shutdown()

	assert.Nil(t, session.current)
	assert.Equal(t, 1, calls)
	assert.False(t, success)
}

func TestSendResponsesWSCreateDoesNotReplayAmbiguousWrite(t *testing.T) {
	sentinel := errors.New("ambiguous write")
	sender := &responsesWSTestSender{err: sentinel}
	billing := &responsesWSTestBilling{}
	turn := &responsesWSTurn{info: &relaycommon.RelayInfo{Billing: billing}}

	err := sendResponsesWSCreate(sender, turn, []byte(`{"type":"response.create"}`))
	require.ErrorIs(t, err, sentinel)
	assert.Equal(t, 1, sender.calls)
	assert.True(t, turn.writeAttempted)

	session := &responsesWSSession{current: turn}
	session.abandonCurrent(false)
	assert.Zero(t, billing.refundCalls.Load(), "a possibly sent turn must not be refunded or replayed")
}

func TestResponsesWSQueuesApplyOrderedBackpressure(t *testing.T) {
	assert.Equal(t, 8, responsesWSClientQueueSize)
	assert.Equal(t, 8, responsesWSUpstreamQueueSize)
	assert.Equal(t, 64<<20, responsesWSMaxFrameBytes)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	peer := &responsesWSPeer{ctx: ctx, inbound: make(chan responsesWSFrame, 1)}
	peer.inbound <- responsesWSFrame{payload: []byte("first")}

	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		peer.report(responsesWSFrame{payload: []byte("second")})
		close(done)
	}()
	<-started
	select {
	case <-done:
		t.Fatal("report bypassed the full bounded queue")
	case <-time.After(20 * time.Millisecond):
	}

	assert.Equal(t, "first", string((<-peer.inbound).payload))
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("report did not resume after queue capacity became available")
	}
	assert.Equal(t, "second", string((<-peer.inbound).payload))
}

func TestResponsesWSPeerTransportWatchdogUsesTransportActivityOnly(t *testing.T) {
	now := time.Now()
	peer := &responsesWSPeer{}
	peer.lastSeen.Store(now.Add(-responsesWSPongTimeout - time.Second).UnixNano())
	assert.True(t, peer.transportStale(now))
	peer.lastSeen.Store(now.Add(-responsesWSPongTimeout + time.Second).UnixNano())
	assert.False(t, peer.transportStale(now))
}

func TestResponsesWSPeerClearsFirstApplicationFrameDeadline(t *testing.T) {
	clientConn, serverConn := newResponsesWSTestPair(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	peer := newResponsesWSPeer(ctx, clientConn, 2, 250*time.Millisecond)
	defer peer.shutdown()

	require.NoError(t, serverConn.WriteMessage(websocket.TextMessage, []byte("first")))
	first := <-peer.inbound
	require.NoError(t, first.err)
	assert.Equal(t, "first", string(first.payload))

	time.Sleep(400 * time.Millisecond)
	require.NoError(t, serverConn.WriteMessage(websocket.TextMessage, []byte("second")))
	select {
	case second := <-peer.inbound:
		require.NoError(t, second.err)
		assert.Equal(t, "second", string(second.payload))
	case <-time.After(time.Second):
		t.Fatal("peer retained an application-data idle deadline after the first frame")
	}
}

func TestCopyResponsesWSClientHeadersIncludesSessionAliasesOnly(t *testing.T) {
	source := make(http.Header)
	source.Set("Session-Id", "hyphen-session")
	source.Set("Conversation-Id", "hyphen-conversation")
	source.Set("X-Codex-Trace", "trace")
	source.Set("Authorization", "Bearer client-secret")
	source.Set("Sec-WebSocket-Protocol", "responses, openai-insecure-api-key.client-secret")
	target := make(http.Header)

	copyResponsesWSClientHeaders(source, target)

	assert.Equal(t, "hyphen-session", target.Get("Session-Id"))
	assert.Equal(t, "hyphen-session", target.Get("session_id"))
	assert.Equal(t, "hyphen-conversation", target.Get("Conversation-Id"))
	assert.Equal(t, "hyphen-conversation", target.Get("conversation_id"))
	assert.Equal(t, "trace", target.Get("X-Codex-Trace"))
	assert.Empty(t, target.Get("Authorization"))
	assert.Empty(t, target.Get("Sec-WebSocket-Protocol"))
}

func TestResponsesWSChannelSupportIsExplicit(t *testing.T) {
	assert.True(t, isResponsesWSChannelSupported(appconstant.ChannelTypeOpenAI))
	assert.False(t, isResponsesWSChannelSupported(appconstant.ChannelTypeCodex))
	assert.False(t, isResponsesWSChannelSupported(appconstant.ChannelTypeCustom))
}

func newResponsesWSBuildContext(model string) (*gin.Context, *relaycommon.RelayInfo) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
	return c, &relaycommon.RelayInfo{
		RelayMode:       relayconstant.RelayModeResponses,
		OriginModelName: model,
		ChannelMeta: &relaycommon.ChannelMeta{
			ChannelType:       appconstant.ChannelTypeOpenAI,
			ApiType:           appconstant.APITypeOpenAI,
			UpstreamModelName: model,
		},
	}
}

type responsesWSTestBilling struct {
	settleCalls atomic.Int32
	refundCalls atomic.Int32
}

func (b *responsesWSTestBilling) Settle(int) error {
	b.settleCalls.Add(1)
	return nil
}

func (b *responsesWSTestBilling) Refund(*gin.Context) {
	b.refundCalls.Add(1)
}

func (b *responsesWSTestBilling) NeedsRefund() bool        { return true }
func (b *responsesWSTestBilling) GetPreConsumedQuota() int { return 1 }
func (b *responsesWSTestBilling) Reserve(int) error        { return nil }

type responsesWSTestSender struct {
	calls int
	err   error
}

func (s *responsesWSTestSender) send(int, []byte) error {
	s.calls++
	return s.err
}

func newResponsesWSTestPair(t *testing.T) (*websocket.Conn, *websocket.Conn) {
	t.Helper()
	serverConnC := make(chan *websocket.Conn, 1)
	release := make(chan struct{})
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		serverConnC <- conn
		<-release
		_ = conn.Close()
	}))
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	clientConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)
	serverConn := <-serverConnC
	t.Cleanup(func() {
		_ = clientConn.Close()
		close(release)
		server.Close()
	})
	return clientConn, serverConn
}
