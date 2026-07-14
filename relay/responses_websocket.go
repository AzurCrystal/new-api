package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/QuantumNous/new-api/common"
	appconstant "github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/middleware"
	appmodel "github.com/QuantumNous/new-api/model"
	relaychannel "github.com/QuantumNous/new-api/relay/channel"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relay/helper"
	"github.com/QuantumNous/new-api/service"
	"github.com/QuantumNous/new-api/setting"
	"github.com/QuantumNous/new-api/setting/ratio_setting"
	"github.com/QuantumNous/new-api/types"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

const (
	responsesWSEventResponseCreate = "response.create"
	responsesWSFirstFrameTimeout   = 15 * time.Second
	responsesWSHandshakeTimeout    = 10 * time.Second
	responsesWSWriteTimeout        = 10 * time.Second
	responsesWSDrainTimeout        = 2 * time.Minute
	responsesWSPingInterval        = 20 * time.Second
	responsesWSPongTimeout         = 60 * time.Second
	responsesWSMaxFrameBytes       = 64 << 20
	responsesWSClientQueueSize     = 8
	responsesWSUpstreamQueueSize   = 8
)

type responsesWSCreate struct {
	request     dto.OpenAIResponsesRequest
	root        map[string]json.RawMessage
	nestedRoot  map[string]json.RawMessage
	nested      bool
	generate    bool
	hasGenerate bool
	eventID     string
	raw         []byte
}

type responsesWSErrorEvent struct {
	Type    string             `json:"type"`
	Status  int                `json:"status"`
	EventID string             `json:"event_id,omitempty"`
	Error   *types.OpenAIError `json:"error"`
}

type responsesWSTurn struct {
	ctx            *gin.Context
	info           *relaycommon.RelayInfo
	usage          *dto.Usage
	outputText     strings.Builder
	generate       bool
	writeAttempted bool
	settleOnce     sync.Once
	rateOnce       sync.Once
	commitRate     middleware.ModelRequestRateLimitCommit
}

type responsesWSFrame struct {
	messageType int
	payload     []byte
	err         error
}

type responsesWSWrite struct {
	messageType int
	payload     []byte
	control     bool
	result      chan error
}

// responsesWSPeer owns exactly one reader and one writer for a websocket.
// Application goroutines communicate with those pumps through bounded queues.
type responsesWSPeer struct {
	ctx      context.Context
	cancel   context.CancelFunc
	conn     *websocket.Conn
	inbound  chan responsesWSFrame
	outbound chan responsesWSWrite
	done     chan struct{}
	wg       sync.WaitGroup
	close    sync.Once
	lastSeen atomic.Int64
}

func newResponsesWSPeer(parent context.Context, conn *websocket.Conn, queueSize int, firstFrameTimeout time.Duration) *responsesWSPeer {
	ctx, cancel := context.WithCancel(parent)
	p := &responsesWSPeer{
		ctx:      ctx,
		cancel:   cancel,
		conn:     conn,
		inbound:  make(chan responsesWSFrame, queueSize),
		outbound: make(chan responsesWSWrite, queueSize),
		done:     make(chan struct{}),
	}
	p.lastSeen.Store(time.Now().UnixNano())
	p.wg.Add(2)
	go p.readLoop(firstFrameTimeout)
	go p.writeLoop()
	go func() {
		p.wg.Wait()
		close(p.done)
	}()
	return p
}

func (p *responsesWSPeer) readLoop(firstFrameTimeout time.Duration) {
	defer p.wg.Done()
	p.conn.SetReadLimit(responsesWSMaxFrameBytes)
	if firstFrameTimeout > 0 {
		_ = p.conn.SetReadDeadline(time.Now().Add(firstFrameTimeout))
	}
	p.conn.SetPingHandler(func(data string) error {
		p.lastSeen.Store(time.Now().UnixNano())
		return p.sendControl(websocket.PongMessage, []byte(data))
	})
	p.conn.SetPongHandler(func(string) error {
		p.lastSeen.Store(time.Now().UnixNano())
		return nil
	})

	firstFrame := true
	for {
		messageType, payload, err := p.conn.ReadMessage()
		if err != nil {
			p.report(responsesWSFrame{err: err})
			return
		}
		p.lastSeen.Store(time.Now().UnixNano())
		if firstFrame {
			firstFrame = false
			_ = p.conn.SetReadDeadline(time.Time{})
		}
		frame := responsesWSFrame{messageType: messageType, payload: payload}
		select {
		case p.inbound <- frame:
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *responsesWSPeer) writeLoop() {
	defer p.wg.Done()
	ticker := time.NewTicker(responsesWSPingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case write := <-p.outbound:
			_ = p.conn.SetWriteDeadline(time.Now().Add(responsesWSWriteTimeout))
			var err error
			if write.control {
				err = p.conn.WriteControl(write.messageType, write.payload, time.Now().Add(responsesWSWriteTimeout))
			} else {
				err = p.conn.WriteMessage(write.messageType, write.payload)
			}
			write.result <- err
			if err != nil {
				p.report(responsesWSFrame{err: err})
				return
			}
		case now := <-ticker.C:
			if p.transportStale(now) {
				err := errors.New("websocket pong watchdog timeout")
				p.report(responsesWSFrame{err: err})
				return
			}
			deadline := time.Now().Add(responsesWSWriteTimeout)
			if err := p.conn.WriteControl(websocket.PingMessage, nil, deadline); err != nil {
				p.report(responsesWSFrame{err: err})
				return
			}
		}
	}
}

func (p *responsesWSPeer) transportStale(now time.Time) bool {
	return now.Sub(time.Unix(0, p.lastSeen.Load())) > responsesWSPongTimeout
}

func (p *responsesWSPeer) report(frame responsesWSFrame) {
	select {
	case p.inbound <- frame:
	case <-p.ctx.Done():
	}
}

func (p *responsesWSPeer) send(messageType int, payload []byte) error {
	return p.enqueue(responsesWSWrite{messageType: messageType, payload: payload, result: make(chan error, 1)})
}

func (p *responsesWSPeer) sendControl(messageType int, payload []byte) error {
	return p.enqueue(responsesWSWrite{messageType: messageType, payload: payload, control: true, result: make(chan error, 1)})
}

func (p *responsesWSPeer) enqueue(write responsesWSWrite) error {
	queueTimer := time.NewTimer(responsesWSWriteTimeout)
	defer queueTimer.Stop()
	select {
	case p.outbound <- write:
	case <-p.ctx.Done():
		return p.ctx.Err()
	case <-queueTimer.C:
		return errors.New("websocket write queue timeout")
	}

	resultTimer := time.NewTimer(responsesWSWriteTimeout)
	defer resultTimer.Stop()
	select {
	case err := <-write.result:
		return err
	case <-p.ctx.Done():
		return p.ctx.Err()
	case <-resultTimer.C:
		return errors.New("websocket write deadline exceeded")
	}
}

func (p *responsesWSPeer) shutdown() {
	if p == nil {
		return
	}
	p.close.Do(func() {
		p.cancel()
		_ = p.conn.Close()
	})
}

type responsesWSSession struct {
	baseCtx       *gin.Context
	ctx           context.Context
	cancel        context.CancelFunc
	client        *responsesWSPeer
	target        *responsesWSPeer
	current       *responsesWSTurn
	lockedModel   string
	lockedChannel *appmodel.Channel
	lockedKey     string
	lockedMulti   bool
	lockedKeyIdx  int
	nextTurn      int
	clientGone    bool
	// upstreamTerminalForwarded suppresses a second, locally generated error
	// when the upstream closes immediately after a terminal event.
	upstreamTerminalForwarded bool
	clientCloseCode           int
	clientCloseReason         string
	drainTimer                *time.Timer
	drainC                    <-chan time.Time
}

func ResponsesWebSocketHelper(c *gin.Context, clientConn *websocket.Conn) *types.NewAPIError {
	if c == nil || clientConn == nil {
		return types.NewError(errors.New("invalid responses websocket connection"), types.ErrorCodeInvalidRequest, types.ErrOptionWithSkipRetry())
	}

	// This context deliberately does not derive from c.Request.Context(). The
	// HTTP request is canceled as soon as the downstream socket disappears, but
	// an already-sent turn still needs a bounded window to read terminal usage.
	sessionCtx, cancel := context.WithCancel(context.Background())
	s := &responsesWSSession{
		baseCtx: c,
		ctx:     sessionCtx,
		cancel:  cancel,
		client:  newResponsesWSPeer(sessionCtx, clientConn, responsesWSClientQueueSize, responsesWSFirstFrameTimeout),
	}
	defer s.shutdown()
	return s.run()
}

func (s *responsesWSSession) run() *types.NewAPIError {
	for {
		var targetInbound <-chan responsesWSFrame
		if s.target != nil {
			targetInbound = s.target.inbound
		}

		select {
		case <-s.ctx.Done():
			s.abandonCurrent(false)
			return nil
		case <-s.drainC:
			logger.LogError(s.baseCtx, "responses websocket drain deadline reached before terminal usage")
			s.abandonCurrent(false)
			return nil
		case frame := <-s.client.inbound:
			if frame.err != nil {
				s.markClientGone(frame.err)
				if s.current == nil {
					return nil
				}
				continue
			}
			if s.clientGone {
				continue
			}
			if closeSession := s.handleClientFrame(frame); closeSession {
				return nil
			}
		case frame := <-targetInbound:
			if frame.err != nil {
				reportClose := s.shouldReportUpstreamClose()
				if reportClose {
					logger.LogError(s.baseCtx, "responses websocket upstream closed: "+frame.err.Error())
				}
				s.abandonCurrent(false)
				if !s.clientGone && reportClose {
					s.sendLocalError("", types.NewError(frame.err, types.ErrorCodeBadResponse, types.ErrOptionWithSkipRetry()))
				}
				if !s.clientGone {
					s.sendClientCloseForUpstreamError(frame.err, reportClose)
				}
				return nil
			}
			terminal, closeSession := s.observeUpstreamFrame(frame.payload)
			if !s.clientGone {
				if err := s.client.send(frame.messageType, frame.payload); err != nil {
					s.markClientGone(err)
				}
			}
			if terminal && s.clientGone {
				return nil
			}
			if closeSession {
				s.sendClientClose()
				return nil
			}
		}
	}
}

func (s *responsesWSSession) shouldReportUpstreamClose() bool {
	return s.current != nil || !s.upstreamTerminalForwarded
}

func (s *responsesWSSession) sendClientCloseForUpstreamError(err error, forceFatal bool) {
	if s.clientCloseCode == 0 {
		s.clientCloseCode = websocket.CloseInternalServerErr
		s.clientCloseReason = "upstream websocket closed"
		var closeErr *websocket.CloseError
		if !forceFatal && errors.As(err, &closeErr) && isResponsesWSForwardableCloseCode(closeErr.Code) {
			s.clientCloseCode = closeErr.Code
			s.clientCloseReason = closeErr.Text
		}
	}
	s.sendClientClose()
}

func (s *responsesWSSession) sendClientClose() {
	if s.clientGone || s.client == nil || s.clientCloseCode == 0 {
		return
	}
	payload := websocket.FormatCloseMessage(s.clientCloseCode, s.clientCloseReason)
	if err := s.client.sendControl(websocket.CloseMessage, payload); err != nil {
		s.markClientGone(err)
	}
}

func isResponsesWSForwardableCloseCode(code int) bool {
	switch code {
	case websocket.CloseNormalClosure,
		websocket.CloseGoingAway,
		websocket.CloseProtocolError,
		websocket.CloseUnsupportedData,
		websocket.CloseInvalidFramePayloadData,
		websocket.ClosePolicyViolation,
		websocket.CloseMessageTooBig,
		websocket.CloseInternalServerErr,
		websocket.CloseServiceRestart,
		websocket.CloseTryAgainLater:
		return true
	default:
		return code >= 3000 && code <= 4999
	}
}

func (s *responsesWSSession) handleClientFrame(frame responsesWSFrame) bool {
	eventType, err := responsesWSEventType(frame.payload)
	if err != nil {
		s.sendLocalError("", newResponsesWSInvalidRequestError(err))
		return false
	}

	if eventType != responsesWSEventResponseCreate {
		if s.target == nil {
			s.sendLocalError("", newResponsesWSInvalidRequestError(errors.New("first responses websocket event must be response.create")))
			return false
		}
		if err := s.target.send(frame.messageType, frame.payload); err != nil {
			s.abandonCurrent(false)
			s.sendLocalError("", types.NewError(err, types.ErrorCodeBadResponse, types.ErrOptionWithSkipRetry()))
			return true
		}
		return false
	}
	if s.current != nil {
		s.sendLocalError(responsesWSEventID(frame.payload), types.NewErrorWithStatusCode(
			errors.New("another response.create is already in progress on this websocket connection"),
			types.ErrorCodeInvalidRequest,
			http.StatusConflict,
			types.ErrOptionWithSkipRetry(),
		))
		s.closeClientAndDrain(
			websocket.ClosePolicyViolation,
			"concurrent response.create is not supported",
		)
		return false
	}

	create, err := parseResponsesWSCreate(frame.payload)
	if err != nil {
		s.sendLocalError(create.eventID, newResponsesWSInvalidRequestError(err))
		return false
	}
	if err := s.normalizeCreateRequest(&create); err != nil {
		s.sendLocalError(create.eventID, newResponsesWSInvalidRequestError(err))
		return false
	}
	turnCtx := s.newTurnContext()
	var commitRate middleware.ModelRequestRateLimitCommit
	reservationOwned := false
	if create.generate {
		var apiErr *types.NewAPIError
		commitRate, apiErr = middleware.CheckModelRequestRateLimit(turnCtx)
		if apiErr != nil {
			s.sendLocalError(create.eventID, apiErr)
			return false
		}
		reservationOwned = true
	}
	defer func() {
		if reservationOwned && commitRate != nil {
			commitRate(false)
		}
	}()

	info, payload, apiErr := s.prepareTurn(turnCtx, create)
	if apiErr != nil {
		if commitRate != nil {
			commitRate(false)
			reservationOwned = false
		}
		s.sendLocalError(create.eventID, apiErr)
		return false
	}
	turn := &responsesWSTurn{
		ctx:        turnCtx,
		info:       info,
		usage:      &dto.Usage{},
		generate:   create.generate,
		commitRate: commitRate,
	}
	s.current = turn
	reservationOwned = false
	s.upstreamTerminalForwarded = false

	// Once the writer is asked to emit response.create, a write error is
	// ambiguous: the peer may have received a complete frame. Never replay it
	// on another socket and never refund it merely because the write failed.
	if err := sendResponsesWSCreate(s.target, turn, payload); err != nil {
		turn.commitRateLimit(false)
		s.sendLocalError(create.eventID, types.NewError(err, types.ErrorCodeBadResponse, types.ErrOptionWithSkipRetry()))
		return true
	}
	return false
}

func (s *responsesWSSession) normalizeCreateRequest(create *responsesWSCreate) error {
	if create == nil {
		return errors.New("response.create is required")
	}
	if create.request.Model == "" {
		if s.lockedModel == "" {
			return errors.New("model is required on the first response.create")
		}
		create.request.Model = s.lockedModel
	}
	if s.lockedModel != "" && create.request.Model != s.lockedModel {
		return fmt.Errorf("responses websocket is locked to model %q; got %q", s.lockedModel, create.request.Model)
	}
	return helper.ValidateResponsesWebSocketRequest(&create.request, create.generate)
}

type responsesWSCreateSender interface {
	send(messageType int, payload []byte) error
}

func sendResponsesWSCreate(target responsesWSCreateSender, turn *responsesWSTurn, payload []byte) error {
	if target == nil || turn == nil {
		return errors.New("responses websocket upstream is not connected")
	}
	// Any failure after handing the frame to the writer is ambiguous: the
	// upstream may have accepted it. The caller must close instead of replaying.
	turn.writeAttempted = true
	return target.send(websocket.TextMessage, payload)
}

func (s *responsesWSSession) prepareTurn(turnCtx *gin.Context, create responsesWSCreate) (*relaycommon.RelayInfo, []byte, *types.NewAPIError) {
	if apiErr := checkResponsesWSModelAccess(turnCtx, create.request.Model); apiErr != nil {
		return nil, nil, apiErr
	}

	var info *relaycommon.RelayInfo
	if s.target == nil {
		var apiErr *types.NewAPIError
		info, apiErr = s.connectFirstTarget(turnCtx, &create.request)
		if apiErr != nil {
			return nil, nil, apiErr
		}
	} else {
		if apiErr := middleware.SetupContextForSelectedChannel(turnCtx, s.lockedChannel, create.request.Model); apiErr != nil {
			return nil, nil, apiErr
		}
		// A multi-key channel may rotate in SetupContextForSelectedChannel. The
		// existing upstream websocket is authenticated to the first key/account.
		common.SetContextKey(turnCtx, appconstant.ContextKeyChannelKey, s.lockedKey)
		common.SetContextKey(turnCtx, appconstant.ContextKeyChannelIsMultiKey, s.lockedMulti)
		common.SetContextKey(turnCtx, appconstant.ContextKeyChannelMultiKeyIndex, s.lockedKeyIdx)
		info = newResponsesWSTurnRelayInfo(turnCtx, &create.request)
	}

	payload, apiErr := buildResponsesWSPayload(turnCtx, info, create)
	if apiErr != nil {
		return nil, nil, apiErr
	}
	if !create.generate {
		return info, payload, nil
	}

	meta := create.request.GetTokenCountMeta()
	if setting.ShouldCheckPromptSensitive() && meta != nil {
		contains, words := service.CheckSensitiveText(meta.CombineText)
		if contains {
			return nil, nil, types.NewError(fmt.Errorf("user sensitive words detected: %s", strings.Join(words, ", ")), types.ErrorCodeSensitiveWordsDetected, types.ErrOptionWithSkipRetry())
		}
	}
	promptTokens, err := service.EstimateRequestToken(turnCtx, meta, info)
	if err != nil {
		return nil, nil, types.NewError(err, types.ErrorCodeCountTokenFailed)
	}
	info.SetEstimatePromptTokens(promptTokens)
	priceData, err := helper.ModelPriceHelper(turnCtx, info, promptTokens, meta)
	if err != nil {
		return nil, nil, types.NewError(err, types.ErrorCodeModelPriceError, types.ErrOptionWithStatusCode(http.StatusBadRequest))
	}
	if !priceData.FreeModel {
		if apiErr := service.PreConsumeBilling(turnCtx, priceData.QuotaToPreConsume, info); apiErr != nil {
			return nil, nil, apiErr
		}
	}
	return info, payload, nil
}

func (s *responsesWSSession) connectFirstTarget(turnCtx *gin.Context, request *dto.OpenAIResponsesRequest) (*relaycommon.RelayInfo, *types.NewAPIError) {
	retryParam := &service.RetryParam{
		Ctx:         turnCtx,
		TokenGroup:  common.GetContextKeyString(turnCtx, appconstant.ContextKeyTokenGroup),
		ModelName:   request.Model,
		RequestPath: "/v1/responses",
		Retry:       common.GetPointer(0),
	}
	if retryParam.TokenGroup == "" {
		retryParam.TokenGroup = common.GetContextKeyString(turnCtx, appconstant.ContextKeyUserGroup)
	}

	var lastErr *types.NewAPIError
	for ; retryParam.GetRetry() <= common.RetryTimes; retryParam.IncreaseRetry() {
		channel, apiErr := selectResponsesWSChannel(turnCtx, request.Model, retryParam)
		if apiErr != nil {
			lastErr = apiErr
			continue
		}
		if !isResponsesWSChannelSupported(channel.Type) {
			lastErr = newResponsesWSInvalidRequestError(fmt.Errorf(
				"responses websocket only supports OpenAI-compatible channels, got channel type %d", channel.Type,
			))
			continue
		}

		info := newResponsesWSTurnRelayInfo(turnCtx, request)
		adaptor := GetAdaptor(info.ApiType)
		if adaptor == nil {
			lastErr = types.NewError(fmt.Errorf("invalid api type: %d", info.ApiType), types.ErrorCodeInvalidApiType, types.ErrOptionWithSkipRetry())
			continue
		}
		adaptor.Init(info)
		targetConn, apiErr := dialResponsesWSUpstream(turnCtx, adaptor, info)
		if apiErr != nil {
			lastErr = apiErr
			logger.LogError(turnCtx, fmt.Sprintf("responses websocket dial failed for channel #%d: %s", channel.Id, apiErr.Error()))
			continue
		}

		// Application-level idle is intentionally unbounded here. Ping/Pong owns
		// transport liveness, while the upstream announces its soft connection
		// limit at a turn boundary.
		s.target = newResponsesWSPeer(s.ctx, targetConn, responsesWSUpstreamQueueSize, 0)
		s.lockedModel = request.Model
		s.lockedChannel = channel
		s.lockedKey = common.GetContextKeyString(turnCtx, appconstant.ContextKeyChannelKey)
		s.lockedMulti = common.GetContextKeyBool(turnCtx, appconstant.ContextKeyChannelIsMultiKey)
		s.lockedKeyIdx = common.GetContextKeyInt(turnCtx, appconstant.ContextKeyChannelMultiKeyIndex)
		service.RecordChannelAffinity(turnCtx, channel.Id)
		return info, nil
	}
	if lastErr == nil {
		lastErr = types.NewError(errors.New("failed to connect responses websocket upstream"), types.ErrorCodeDoRequestFailed, types.ErrOptionWithSkipRetry())
	}
	return nil, lastErr
}

func isResponsesWSChannelSupported(channelType int) bool {
	return channelType == appconstant.ChannelTypeOpenAI
}

func newResponsesWSTurnRelayInfo(turnCtx *gin.Context, request *dto.OpenAIResponsesRequest) *relaycommon.RelayInfo {
	info := relaycommon.GenRelayInfoResponses(turnCtx, request)
	info.InitRequestConversionChain()
	info.InitChannelMeta(turnCtx)
	return info
}

func (s *responsesWSSession) newTurnContext() *gin.Context {
	turnCtx := s.baseCtx.Copy()
	request := s.baseCtx.Request.Clone(s.ctx)
	request.Body = http.NoBody
	turnCtx.Request = request
	requestID := fmt.Sprintf("%s-ws-%d", s.baseCtx.GetString(common.RequestIdKey), s.nextTurn)
	s.nextTurn++
	turnCtx.Set(common.RequestIdKey, requestID)
	common.SetContextKey(turnCtx, appconstant.ContextKeyRequestStartTime, time.Now())
	turnCtx.Set("use_channel", []string{})
	return turnCtx
}

func (s *responsesWSSession) observeUpstreamFrame(payload []byte) (terminal bool, closeSession bool) {
	eventType, err := responsesWSEventType(payload)
	if err != nil {
		return false, false
	}
	var event struct {
		Type     string                       `json:"type"`
		Response *dto.OpenAIResponsesResponse `json:"response,omitempty"`
		Delta    string                       `json:"delta,omitempty"`
		Item     *dto.ResponsesOutput         `json:"item,omitempty"`
		Error    *types.OpenAIError           `json:"error,omitempty"`
	}
	if err := common.Unmarshal(payload, &event); err != nil {
		return false, false
	}

	turn := s.current
	if eventType == "error" {
		if turn != nil {
			turn.info.SetFirstResponseTime()
			s.finishCurrent(turn, false)
		}
		s.upstreamTerminalForwarded = true
		closeSession := isResponsesWSSessionTerminatingError(event.Error)
		if closeSession {
			s.clientCloseCode = websocket.CloseInternalServerErr
			s.clientCloseReason = fmt.Sprint(event.Error.Code)
			if fmt.Sprint(event.Error.Code) == "websocket_connection_limit_reached" {
				s.clientCloseCode = websocket.CloseTryAgainLater
			}
		}
		return true, closeSession
	}
	if turn == nil {
		return false, false
	}
	turn.info.SetFirstResponseTime()

	switch eventType {
	case "response.output_text.delta":
		turn.outputText.WriteString(event.Delta)
	case dto.ResponsesOutputTypeItemDone:
		if event.Item != nil && event.Item.Type == dto.BuildInCallWebSearchCall && turn.info.ResponsesUsageInfo != nil {
			if tool := turn.info.ResponsesUsageInfo.BuiltInTools[dto.BuildInToolWebSearchPreview]; tool != nil {
				tool.CallCount++
			}
		}
	case "response.completed", "response.done", "response.failed", "response.incomplete", "response.cancelled", "response.canceled":
		applyResponsesWSTerminalUsage(turn, event.Response)
		success := eventType == "response.completed" || eventType == "response.done"
		s.finishCurrent(turn, success)
		s.upstreamTerminalForwarded = true
		return true, false
	}
	return false, false
}

func isResponsesWSSessionTerminatingError(apiErr *types.OpenAIError) bool {
	if apiErr == nil {
		return false
	}
	switch fmt.Sprint(apiErr.Code) {
	case "websocket_connection_limit_reached", "previous_response_not_found":
		return true
	default:
		return false
	}
}

func applyResponsesWSTerminalUsage(turn *responsesWSTurn, response *dto.OpenAIResponsesResponse) {
	if turn == nil || response == nil {
		return
	}
	if response.Usage != nil {
		service.ApplyResponsesUsage(turn.usage, response.Usage)
	}
	if response.HasImageGenerationCall() {
		turn.ctx.Set("image_generation_call", true)
		turn.ctx.Set("image_generation_call_quality", response.GetQuality())
		turn.ctx.Set("image_generation_call_size", response.GetSize())
	}
}

func (s *responsesWSSession) finishCurrent(turn *responsesWSTurn, success bool) {
	if turn == nil || s.current != turn {
		return
	}
	turn.settleOnce.Do(func() {
		if !turn.generate {
			return
		}
		finalizeResponsesWSUsage(turn)
		service.PostTextConsumeQuota(turn.ctx, turn.info, turn.usage, nil)
	})
	turn.commitRateLimit(success)
	s.current = nil
}

func finalizeResponsesWSUsage(turn *responsesWSTurn) {
	if turn == nil || turn.usage == nil || turn.info == nil {
		return
	}
	if turn.usage.CompletionTokens == 0 {
		if output := turn.outputText.String(); output != "" {
			turn.usage.CompletionTokens = service.CountTextToken(output, turn.info.UpstreamModelName)
			turn.usage.OutputTokens = turn.usage.CompletionTokens
		}
	}
	if turn.usage.PromptTokens == 0 && turn.usage.CompletionTokens != 0 {
		turn.usage.PromptTokens = turn.info.GetEstimatePromptTokens()
		turn.usage.InputTokens = turn.usage.PromptTokens
	}
	if turn.usage.TotalTokens == 0 {
		turn.usage.TotalTokens = turn.usage.PromptTokens + turn.usage.CompletionTokens
	}
}

func (turn *responsesWSTurn) commitRateLimit(success bool) {
	if turn == nil || turn.commitRate == nil {
		return
	}
	turn.rateOnce.Do(func() { turn.commitRate(success) })
}

func (s *responsesWSSession) abandonCurrent(definitelyUnsent bool) {
	turn := s.current
	if turn == nil {
		return
	}
	if definitelyUnsent && !turn.writeAttempted && turn.info != nil && turn.info.Billing != nil {
		turn.info.Billing.Refund(turn.ctx)
	}
	turn.commitRateLimit(false)
	s.current = nil
}

func (s *responsesWSSession) markClientGone(err error) {
	if s.clientGone {
		return
	}
	s.clientGone = true
	if err != nil && !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
		logger.LogError(s.baseCtx, "responses websocket downstream closed: "+err.Error())
	}
	if s.current != nil && s.drainTimer == nil {
		s.drainTimer = time.NewTimer(responsesWSDrainTimeout)
		s.drainC = s.drainTimer.C
	}
}

func (s *responsesWSSession) closeClientAndDrain(code int, reason string) {
	if s == nil || s.clientGone {
		return
	}
	s.clientCloseCode = code
	s.clientCloseReason = reason
	s.sendClientClose()
	// The upstream may already be generating. Keep the session loop alive for
	// bounded terminal usage drain, while ignoring further downstream frames.
	s.markClientGone(nil)
}

func (s *responsesWSSession) sendLocalError(eventID string, apiErr *types.NewAPIError) {
	if apiErr == nil || s.clientGone {
		return
	}
	payload, err := buildResponsesWSErrorPayload(eventID, apiErr)
	if err != nil {
		return
	}
	if err := s.client.send(websocket.TextMessage, payload); err != nil {
		s.markClientGone(err)
	}
}

func (s *responsesWSSession) shutdown() {
	s.abandonCurrent(false)
	if s.drainTimer != nil {
		s.drainTimer.Stop()
	}
	s.cancel()
	if s.target != nil {
		s.target.shutdown()
	}
	if s.client != nil {
		s.client.shutdown()
	}
}

func responsesWSEventType(payload []byte) (string, error) {
	var event struct {
		Type string `json:"type"`
	}
	if err := common.Unmarshal(payload, &event); err != nil {
		return "", fmt.Errorf("invalid websocket event json: %w", err)
	}
	if strings.TrimSpace(event.Type) == "" {
		return "", errors.New("websocket event type is required")
	}
	return event.Type, nil
}

func responsesWSEventID(payload []byte) string {
	var event struct {
		EventID string `json:"event_id"`
	}
	if err := common.Unmarshal(payload, &event); err != nil {
		return ""
	}
	return strings.TrimSpace(event.EventID)
}

func parseResponsesWSCreate(payload []byte) (responsesWSCreate, error) {
	create := responsesWSCreate{generate: true, raw: append([]byte(nil), payload...)}
	if err := common.Unmarshal(payload, &create.root); err != nil {
		return create, err
	}
	eventType, err := responsesWSEventType(payload)
	if err != nil {
		return create, err
	}
	if eventType != responsesWSEventResponseCreate {
		return create, fmt.Errorf("unsupported event type %q", eventType)
	}
	if raw, ok := create.root["event_id"]; ok {
		_ = common.Unmarshal(raw, &create.eventID)
	}

	requestPayload := payload
	if raw, ok := create.root["response"]; ok && len(raw) > 0 {
		create.nested = true
		requestPayload = raw
		if err := common.Unmarshal(raw, &create.nestedRoot); err != nil {
			return create, errors.New("response must be an object")
		}
		if create.nestedRoot == nil {
			return create, errors.New("response must be an object")
		}
	}
	if err := common.Unmarshal(requestPayload, &create.request); err != nil {
		return create, err
	}

	rootGenerate, rootHasGenerate := create.root["generate"]
	nestedGenerate, nestedHasGenerate := create.nestedRoot["generate"]
	if rootHasGenerate {
		generate, err := parseResponsesWSGenerate(rootGenerate)
		if err != nil {
			return create, err
		}
		create.generate = generate
		create.hasGenerate = true
	}
	if nestedHasGenerate {
		generate, err := parseResponsesWSGenerate(nestedGenerate)
		if err != nil {
			return create, err
		}
		if !rootHasGenerate {
			create.generate = generate
			create.hasGenerate = true
		}
	}
	return create, nil
}

func parseResponsesWSGenerate(raw json.RawMessage) (bool, error) {
	var value any
	if err := common.Unmarshal(raw, &value); err != nil {
		return false, errors.New("generate must be a boolean")
	}
	generate, ok := value.(bool)
	if !ok {
		return false, errors.New("generate must be a boolean")
	}
	return generate, nil
}

func buildResponsesWSPayload(c *gin.Context, info *relaycommon.RelayInfo, create responsesWSCreate) ([]byte, *types.NewAPIError) {
	request := create.request
	if request.Reasoning != nil {
		reasoningCopy := *request.Reasoning
		request.Reasoning = &reasoningCopy
	}
	if err := helper.ModelMappedHelper(c, info, &request); err != nil {
		return nil, types.NewError(err, types.ErrorCodeChannelModelMappedError, types.ErrOptionWithSkipRetry())
	}
	adaptor := GetAdaptor(info.ApiType)
	if adaptor == nil {
		return nil, types.NewError(fmt.Errorf("invalid api type: %d", info.ApiType), types.ErrorCodeInvalidApiType, types.ErrOptionWithSkipRetry())
	}
	adaptor.Init(info)
	converted, err := adaptor.ConvertOpenAIResponsesRequest(c, info, request)
	if err != nil {
		return nil, types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
	}
	convertedRequest, ok := converted.(dto.OpenAIResponsesRequest)
	if !ok {
		return nil, types.NewError(fmt.Errorf("responses websocket requires an OpenAI-compatible request, got %T", converted), types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
	}
	request = convertedRequest

	target := flattenResponsesWSCreate(create)
	changed := create.nested
	typeRaw, err := common.Marshal(responsesWSEventResponseCreate)
	if err != nil {
		return nil, types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
	}
	target["type"] = typeRaw
	for _, field := range responsesWSUnsupportedCreateFields {
		if _, ok := target[field]; ok {
			delete(target, field)
			changed = true
		}
	}
	if create.hasGenerate {
		generateRaw, marshalErr := common.Marshal(create.generate)
		if marshalErr != nil {
			return nil, types.NewError(marshalErr, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
		}
		target["generate"] = generateRaw
	}
	convertedChanged, mergeErr := mergeConvertedResponsesWSRequest(target, create.request, request)
	if mergeErr != nil {
		return nil, types.NewError(mergeErr, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
	}
	changed = convertedChanged || changed
	changed = filterResponsesWSFields(target, info) || changed

	result := create.raw
	if changed {
		result, err = common.Marshal(target)
		if err != nil {
			return nil, types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
		}
	}
	if len(info.ParamOverride) > 0 {
		result, err = relaycommon.ApplyParamOverrideWithRelayInfo(result, info)
		if err != nil {
			return nil, newAPIErrorFromParamOverride(err)
		}
		result, err = normalizeResponsesWSCreatePayload(result)
		if err != nil {
			return nil, types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
		}
	}
	if err := validateResponsesWSUpstreamPayload(result); err != nil {
		return nil, types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
	}
	return result, nil
}

var responsesWSUnsupportedCreateFields = []string{"response", "event_id", "background", "stream", "stream_options"}

func flattenResponsesWSCreate(create responsesWSCreate) map[string]json.RawMessage {
	target := cloneRawMessageMap(create.root)
	delete(target, "response")
	for key, value := range create.nestedRoot {
		target[key] = append(json.RawMessage(nil), value...)
	}
	return target
}

// mergeConvertedResponsesWSRequest applies only fields changed by the normal
// OpenAI HTTP adaptor. Unknown client fields remain in target byte-for-byte.
func mergeConvertedResponsesWSRequest(target map[string]json.RawMessage, original, converted dto.OpenAIResponsesRequest) (bool, error) {
	originalJSON, err := common.Marshal(original)
	if err != nil {
		return false, err
	}
	convertedJSON, err := common.Marshal(converted)
	if err != nil {
		return false, err
	}
	var originalFields map[string]json.RawMessage
	var convertedFields map[string]json.RawMessage
	if err := common.Unmarshal(originalJSON, &originalFields); err != nil {
		return false, err
	}
	if err := common.Unmarshal(convertedJSON, &convertedFields); err != nil {
		return false, err
	}

	changed := false
	for key, originalValue := range originalFields {
		convertedValue, ok := convertedFields[key]
		if !ok {
			if _, exists := target[key]; exists {
				delete(target, key)
				changed = true
			}
			continue
		}
		if !bytes.Equal(originalValue, convertedValue) {
			target[key] = append(json.RawMessage(nil), convertedValue...)
			changed = true
		}
	}
	for key, convertedValue := range convertedFields {
		if _, existed := originalFields[key]; existed {
			continue
		}
		target[key] = append(json.RawMessage(nil), convertedValue...)
		changed = true
	}
	if _, hasModel := target["model"]; !hasModel {
		if model, ok := convertedFields["model"]; ok {
			target["model"] = append(json.RawMessage(nil), model...)
			changed = true
		}
	}
	return changed, nil
}

func normalizeResponsesWSCreatePayload(payload []byte) ([]byte, error) {
	var target map[string]json.RawMessage
	if err := common.Unmarshal(payload, &target); err != nil {
		return nil, err
	}
	typeRaw, err := common.Marshal(responsesWSEventResponseCreate)
	if err != nil {
		return nil, err
	}
	target["type"] = typeRaw
	for _, field := range responsesWSUnsupportedCreateFields {
		delete(target, field)
	}
	if raw, ok := target["generate"]; ok {
		if _, err := parseResponsesWSGenerate(raw); err != nil {
			return nil, err
		}
	}
	return common.Marshal(target)
}

func validateResponsesWSUpstreamPayload(payload []byte) error {
	var request dto.OpenAIResponsesRequest
	if err := common.Unmarshal(payload, &request); err != nil {
		return err
	}
	generate := true
	var fields map[string]json.RawMessage
	if err := common.Unmarshal(payload, &fields); err != nil {
		return err
	}
	if raw, ok := fields["generate"]; ok {
		var err error
		generate, err = parseResponsesWSGenerate(raw)
		if err != nil {
			return err
		}
	}
	return helper.ValidateResponsesWebSocketRequest(&request, generate)
}

func cloneRawMessageMap(source map[string]json.RawMessage) map[string]json.RawMessage {
	result := make(map[string]json.RawMessage, len(source))
	for key, value := range source {
		result[key] = append(json.RawMessage(nil), value...)
	}
	return result
}

func filterResponsesWSFields(payload map[string]json.RawMessage, info *relaycommon.RelayInfo) bool {
	if info == nil {
		return false
	}
	settings := info.ChannelOtherSettings
	changed := false
	remove := func(key string, allowed bool) {
		if allowed {
			return
		}
		if _, ok := payload[key]; ok {
			delete(payload, key)
			changed = true
		}
	}
	remove("service_tier", settings.AllowServiceTier)
	remove("inference_geo", settings.AllowInferenceGeo)
	remove("speed", settings.AllowSpeed)
	remove("store", !settings.DisableStore)
	remove("safety_identifier", settings.AllowSafetyIdentifier)

	if !settings.AllowIncludeObfuscation {
		if raw, ok := payload["stream_options"]; ok {
			var options map[string]json.RawMessage
			if common.Unmarshal(raw, &options) == nil {
				if _, exists := options["include_obfuscation"]; exists {
					delete(options, "include_obfuscation")
					changed = true
					if len(options) == 0 {
						delete(payload, "stream_options")
					} else if updated, err := common.Marshal(options); err == nil {
						payload["stream_options"] = updated
					}
				}
			}
		}
	}
	return changed
}

func newResponsesWSInvalidRequestError(err error) *types.NewAPIError {
	return types.NewErrorWithStatusCode(err, types.ErrorCodeInvalidRequest, http.StatusBadRequest, types.ErrOptionWithSkipRetry())
}

func buildResponsesWSErrorPayload(eventID string, apiErr *types.NewAPIError) ([]byte, error) {
	if apiErr == nil {
		return nil, errors.New("api error is nil")
	}
	status := apiErr.StatusCode
	if status == 0 {
		status = http.StatusInternalServerError
	}
	openaiErr := apiErr.ToOpenAIError()
	return common.Marshal(&responsesWSErrorEvent{
		Type:    "error",
		Status:  status,
		EventID: eventID,
		Error:   &openaiErr,
	})
}

func checkResponsesWSModelAccess(c *gin.Context, modelName string) *types.NewAPIError {
	if !common.GetContextKeyBool(c, appconstant.ContextKeyTokenModelLimitEnabled) {
		return nil
	}
	raw, ok := common.GetContextKey(c, appconstant.ContextKeyTokenModelLimit)
	if !ok {
		return types.NewErrorWithStatusCode(errors.New("token has no model access"), types.ErrorCodeAccessDenied, http.StatusForbidden, types.ErrOptionWithSkipRetry())
	}
	limits, ok := raw.(map[string]bool)
	if !ok {
		limits = map[string]bool{}
	}
	matchName := ratio_setting.FormatMatchingModelName(modelName)
	if _, ok := limits[matchName]; !ok {
		return types.NewErrorWithStatusCode(fmt.Errorf("token is not allowed to use model %s", modelName), types.ErrorCodeAccessDenied, http.StatusForbidden, types.ErrOptionWithSkipRetry())
	}
	return nil
}

func selectResponsesWSChannel(c *gin.Context, modelName string, retryParam *service.RetryParam) (*appmodel.Channel, *types.NewAPIError) {
	if raw, ok := common.GetContextKey(c, appconstant.ContextKeyTokenSpecificChannelId); ok {
		channelID, ok := raw.(string)
		if !ok {
			return nil, types.NewErrorWithStatusCode(errors.New("invalid specified channel id"), types.ErrorCodeGetChannelFailed, http.StatusBadRequest, types.ErrOptionWithSkipRetry())
		}
		id, err := strconv.Atoi(channelID)
		if err != nil {
			return nil, types.NewErrorWithStatusCode(err, types.ErrorCodeGetChannelFailed, http.StatusBadRequest, types.ErrOptionWithSkipRetry())
		}
		channel, err := appmodel.GetChannelById(id, true)
		if err != nil {
			return nil, types.NewErrorWithStatusCode(err, types.ErrorCodeGetChannelFailed, http.StatusBadRequest, types.ErrOptionWithSkipRetry())
		}
		if channel.Status != common.ChannelStatusEnabled {
			return nil, types.NewErrorWithStatusCode(errors.New("specified channel is disabled"), types.ErrorCodeGetChannelFailed, http.StatusForbidden, types.ErrOptionWithSkipRetry())
		}
		if apiErr := middleware.SetupContextForSelectedChannel(c, channel, modelName); apiErr != nil {
			return nil, apiErr
		}
		return channel, nil
	}

	channel, selectedGroup, err := service.CacheGetRandomSatisfiedChannel(retryParam)
	if err != nil {
		return nil, types.NewError(fmt.Errorf("failed to select channel for group %s and model %s: %w", selectedGroup, modelName, err), types.ErrorCodeGetChannelFailed, types.ErrOptionWithSkipRetry())
	}
	if channel == nil {
		return nil, types.NewError(fmt.Errorf("no available channel for group %s and model %s", selectedGroup, modelName), types.ErrorCodeGetChannelFailed, types.ErrOptionWithSkipRetry())
	}
	if apiErr := middleware.SetupContextForSelectedChannel(c, channel, modelName); apiErr != nil {
		return nil, apiErr
	}
	return channel, nil
}

func dialResponsesWSUpstream(c *gin.Context, adaptor relaychannel.Adaptor, info *relaycommon.RelayInfo) (*websocket.Conn, *types.NewAPIError) {
	requestURL, err := adaptor.GetRequestURL(info)
	if err != nil {
		return nil, types.NewError(fmt.Errorf("get request url failed: %w", err), types.ErrorCodeDoRequestFailed)
	}
	requestURL = responsesWebSocketURL(requestURL)
	header := http.Header{}
	if err := adaptor.SetupRequestHeader(c, &header, info); err != nil {
		return nil, types.NewError(fmt.Errorf("setup request header failed: %w", err), types.ErrorCodeDoRequestFailed)
	}
	overrides, err := relaychannel.ResolveHeaderOverride(info, c)
	if err != nil {
		return nil, types.NewError(err, types.ErrorCodeChannelHeaderOverrideInvalid)
	}
	for key, value := range overrides {
		header.Set(key, value)
	}
	copyResponsesWSClientHeaders(c.Request.Header, header)

	dialer := websocket.Dialer{
		Proxy:             http.ProxyFromEnvironment,
		HandshakeTimeout:  responsesWSHandshakeTimeout,
		EnableCompression: true,
	}
	if common.TLSInsecureSkipVerify {
		dialer.TLSClientConfig = common.InsecureTLSConfig
	}
	target, response, err := dialer.DialContext(safeContext(c), requestURL, header)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	if err != nil {
		status := http.StatusBadGateway
		if response != nil {
			status = response.StatusCode
		}
		return nil, types.NewErrorWithStatusCode(
			fmt.Errorf("dial failed to %s: %w", relaycommon.SanitizeURLForLog(requestURL), err),
			types.ErrorCodeDoRequestFailed,
			status,
			types.ErrOptionWithSkipRetry(),
		)
	}
	return target, nil
}

func safeContext(c *gin.Context) context.Context {
	if c != nil && c.Request != nil {
		return c.Request.Context()
	}
	return context.Background()
}

func responsesWebSocketURL(raw string) string {
	switch {
	case strings.HasPrefix(raw, "https://"):
		return "wss://" + strings.TrimPrefix(raw, "https://")
	case strings.HasPrefix(raw, "http://"):
		return "ws://" + strings.TrimPrefix(raw, "http://")
	default:
		return raw
	}
}

func copyResponsesWSClientHeaders(source, target http.Header) {
	for name := range source {
		lower := strings.ToLower(name)
		allowed := lower == "openai-beta" ||
			lower == "session-id" ||
			lower == "session_id" ||
			lower == "thread-id" ||
			lower == "thread_id" ||
			lower == "conversation-id" ||
			lower == "conversation_id" ||
			lower == "x-client-request-id" ||
			lower == "openai-model" ||
			lower == "originator" ||
			lower == "traceparent" ||
			lower == "tracestate" ||
			strings.HasPrefix(lower, "x-codex-") ||
			strings.HasPrefix(lower, "x-oai-") ||
			strings.HasPrefix(lower, "x-responsesapi-")
		if allowed {
			target.Set(name, source.Get(name))
		}
	}
	copyResponsesWSHeaderAlias(source, target, "session_id", "session_id", "session-id")
	copyResponsesWSHeaderAlias(source, target, "conversation_id", "conversation_id", "conversation-id")
}

func copyResponsesWSHeaderAlias(source, target http.Header, targetName string, sourceNames ...string) {
	for _, sourceName := range sourceNames {
		if value := source.Get(sourceName); value != "" {
			target.Set(targetName, value)
			return
		}
	}
}
