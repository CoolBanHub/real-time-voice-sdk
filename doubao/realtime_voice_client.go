package doubao

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

var (
	wsURL = url.URL{Scheme: "wss", Host: "openspeech.bytedance.com", Path: "/api/v3/realtime/dialogue"}
)

type RealtimeVoiceClientConfig struct {
	Appid       string
	AccessToken string
	SessionId   string
	Log         *log.Helper

	// 快捷配置：常用的简化配置选项
	Speaker        string // 快捷配置：默认音色，会用于 TTS 配置（如果未提供 TTS 配置）
	EnableEventLog bool   // 是否启用事件处理详细日志（默认 false，设置为 true 可查看详细事件日志）

	// 完整配置：如果需要更详细的控制，使用以下配置（优先级高于快捷配置）
	ASR    *ASRPayload    // ASR 配置，nil 则使用默认配置
	TTS    *TTSPayload    // TTS 配置，nil 则使用默认配置
	Dialog *DialogPayload // Dialog 配置，nil 则使用默认配置

	// 重连配置：控制连接失败后的自动重连行为
	Reconnect *ReconnectConfig // 重连配置，nil 则使用默认配置
}

type MsgMetadata struct {
	TTSType    string `json:"tts_type"`
	TTSTaskID  string `json:"tts_task_id"` // 新增：TTS任务ID
	QuestionID string `json:"question_id"`
	ReplyID    string `json:"reply_id"`
	Timestamp  int64  `json:"timestamp"` // 新增：时间戳
}

type RealtimeVoiceClient struct {
	Appid       string
	AccessToken string
	wsURL       url.URL
	wsWriteLock sync.Mutex
	SessionId   string

	localSequence        atomic.Int64
	isUserQuerying       atomic.Bool
	isSendingChatTTSText atomic.Bool
	queryChan            chan struct{}
	protocol             *BinaryProtocol
	doubaoWSConn         *websocket.Conn //跟豆包的实时链接
	ClientWsConn         *websocket.Conn //客户端链接
	ctx                  context.Context
	dialogID             string

	// 用户自定义配置
	asrConfig    *ASRPayload
	ttsConfig    *TTSPayload
	dialogConfig *DialogPayload

	// 运行时缓存字段（从配置中派生，用于性能优化）
	pcmFormat     string // 从 ttsConfig.AudioConfig.Format 派生，用于快速访问
	speakerCached string // 从配置中缓存的 Speaker，用于默认 TTS 配置

	// Callbacks - 基础回调
	OnAudio func([]byte) // 接收音频数据
	OnError func(error)  // 错误处理

	// Connection callbacks - 连接事件回调
	OnConnectionStarted  func(*Message) // 连接成功建立
	OnConnectionFailed   func(*Message) // 连接失败
	OnConnectionFinished func(*Message) // 连接结束

	// Session callbacks - 会话事件回调
	OnSessionStarted  func(*Message) // 会话开始（返回dialog_id）
	OnSessionFinished func(*Message) // 会话结束
	OnSessionFailed   func(*Message) // 会话失败

	// ASR callbacks - 语音识别回调
	OnInterrupt              func()             // ASR开始 - 用户开始说话，可用于打断播放
	OnInputTranscript        func(string, bool) // ASR识别结果 - text, isFinal
	OnInputTranscriptPartial func(string, bool) // ASR中间结果 - text, isFinal (interim)
	OnASREnded               func()             // ASR结束 - 用户停止说话

	// TTS callbacks - 语音合成回调
	OnTTSStart         func(*MsgMetadata)         // TTS开始 - 带元数据
	OnOutputTranscript func(string, *MsgMetadata) // TTS分句结束 - text, metadata
	OnTTSEnded         func(*Message)             // TTS合成结束

	// Chat callbacks - 聊天对话回调
	OnChatResponse           func(string)   // 聊天文本回复 - content
	OnChatEnded              func(*Message) // 聊天回复结束
	OnChatTextQueryConfirmed func(*Message) // 文本查询确认 - 返回question_id

	// Conversation callbacks - 上下文管理回调
	OnConversationCreated   func(*Message) // 上下文创建确认 - 返回创建的item数组
	OnConversationUpdated   func(*Message) // 上下文更新确认 - 返回更新结果
	OnConversationRetrieved func(*Message) // 上下文查询结果 - 返回查询的item数组
	OnConversationDeleted   func(*Message) // 上下文删除确认 - 返回删除的item数组

	// Usage callbacks - 用量信息回调
	OnUsageInfo func(*Message) // 用量统计信息

	// Connection state callbacks - 连接状态管理
	OnConnectionStateChange func(oldState, newState ConnectionState) // 连接状态变化
	OnReconnecting          func(attempt int, delay time.Duration)   // 重连尝试前
	OnReconnectFailed       func(attempts int, err error)            // 所有重连尝试失败

	// 自定义消息处理器，如果设置则替代默认的 handleMessage()
	// 用户可以通过设置此字段来完全控制消息处理流程
	MessageHandler func()

	enableEventLog bool // 是否启用事件处理详细日志
	log            *log.Helper

	// Connection state management
	stateManager      *connectionStateManager
	reconnectConfig   *ReconnectConfig
	stopReconnect     chan struct{} // Signal to stop reconnection attempts
	isManuallyClosing bool          // Flag to distinguish manual close from connection errors
}

func NewRealtimeVoiceClient(cfg *RealtimeVoiceClientConfig) *RealtimeVoiceClient {
	if cfg == nil {
		return nil
	}
	// 设置默认值
	if cfg.Speaker == "" {
		cfg.Speaker = "zh_female_vv_jupiter_bigtts"
	}
	if cfg.SessionId == "" {
		cfg.SessionId = uuid.New().String()
	}

	// 初始化重连配置
	reconnectConfig := cfg.Reconnect
	if reconnectConfig == nil {
		reconnectConfig = DefaultReconnectConfig()
	}
	this := &RealtimeVoiceClient{
		Appid:          cfg.Appid,
		AccessToken:    cfg.AccessToken,
		wsURL:          wsURL,
		queryChan:      make(chan struct{}, 1),
		protocol:       NewBinaryProtocol(),
		SessionId:      cfg.SessionId,
		isUserQuerying: atomic.Bool{},
		localSequence:  atomic.Int64{},
		wsWriteLock:    sync.Mutex{},

		enableEventLog:  cfg.EnableEventLog,
		ctx:             context.Background(),
		log:             cfg.Log,
		asrConfig:       cfg.ASR,
		ttsConfig:       cfg.TTS,
		dialogConfig:    cfg.Dialog,
		speakerCached:   cfg.Speaker, // 缓存 Speaker 用于默认 TTS 配置
		reconnectConfig: reconnectConfig,
		stateManager:    newConnectionStateManager(reconnectConfig),
		stopReconnect:   make(chan struct{}),
	}
	this.protocol.SetVersion(Version1)
	this.protocol.SetHeaderSize(HeaderSize4)
	this.protocol.SetSerialization(SerializationJSON)  // 默认JSON用于握手
	this.protocol.SetCompression(CompressionNone, nil) // 默认无压缩
	// 注意：音频发送时会单独设置 Raw + GZIP
	this.protocol.containsSequence = ContainsSequence
	rand.New(rand.NewSource(time.Now().UnixNano()))

	// 设置状态管理器的回调
	this.stateManager.SetOnStateChange(func(oldState, newState ConnectionState) {
		this.log.Infof("Connection state changed: %s -> %s", oldState, newState)
		if this.OnConnectionStateChange != nil {
			this.OnConnectionStateChange(oldState, newState)
		}
	})

	this.stateManager.SetOnReconnectFailed(func(attempts int, err error) {
		this.log.Errorf("All reconnection attempts failed after %d tries: %v", attempts, err)
		if this.OnReconnectFailed != nil {
			this.OnReconnectFailed(attempts, err)
		}
	})

	return this
}
func (this *RealtimeVoiceClient) Start() error {

	if this.AccessToken == "" {
		return ErrMissingAccessToken
	}
	if this.Appid == "" {
		return ErrMissingAppID
	}

	conn, resp, err := websocket.DefaultDialer.DialContext(this.ctx, wsURL.String(), http.Header{
		"X-Api-Resource-Id": []string{"volc.speech.dialog"},
		"X-Api-Access-Key":  []string{this.AccessToken},
		"X-Api-App-Key":     []string{"PlgvMymc7f3tQnJ6"},
		"X-Api-App-ID":      []string{this.Appid},
		"X-Api-Connect-Id":  []string{uuid.New().String()},
	})
	if err != nil {
		this.log.Errorf("Websocket dial error: %v", err)
		return err
	}
	if resp != nil {
		this.log.Infof("Websocket dial response logid: %s", resp.Header.Get("X-Tt-Logid"))
	}
	this.doubaoWSConn = conn
	return nil
}

func (this *RealtimeVoiceClient) RealTimeDialog() error {
	err := this.startConnection()
	if err != nil {
		this.log.Errorf("realTimeDialog startConnection error: %v", err)
		return err
	}

	// 构建会话配置，优先使用用户自定义配置，否则使用默认配置
	payload := &StartSessionPayload{
		ASR:    this.getASRConfig(),
		TTS:    this.getTTSConfig(),
		Dialog: this.getDialogConfig(),
	}

	err = this.startSession(payload)
	if err != nil {
		this.log.Errorf("realTimeDialog startSession error: %v", err)
		return err
	}

	// 缓存 pcmFormat 以便运行时快速访问
	this.pcmFormat = payload.TTS.AudioConfig.Format

	//todo 支持多mod

	// 开启服务端接收消息处理
	// 如果用户设置了自定义的 MessageHandler，使用自定义的；否则使用默认的 handleMessage()
	this.log.Infof("Start realTimeDialog")
	if this.MessageHandler != nil {
		go this.MessageHandler()
	} else {
		go this.handleMessage()
	}
	return nil
}

// getASRConfig 返回 ASR 配置，优先使用用户自定义配置，否则返回默认配置
func (this *RealtimeVoiceClient) getASRConfig() ASRPayload {
	if this.asrConfig != nil {
		return *this.asrConfig
	}
	// 默认 ASR 配置
	return ASRPayload{
		Format:  "pcm",
		Rate:    16000, // 客户端录音采样率为16kHz
		Bits:    16,
		Channel: 1,
		Extra: map[string]interface{}{
			"enable_itn_convert": true,
		},
	}
}

// getTTSConfig 返回 TTS 配置，优先使用用户自定义配置，否则返回默认配置
func (this *RealtimeVoiceClient) getTTSConfig() TTSPayload {
	if this.ttsConfig != nil {
		return *this.ttsConfig
	}
	// 默认 TTS 配置
	return TTSPayload{
		Speaker: this.speakerCached,
		AudioConfig: AudioConfig{
			Channel:    1,
			Format:     "pcm_s16le",
			SampleRate: 24000,
		},
	}
}

// getDialogConfig 返回 Dialog 配置，优先使用用户自定义配置，否则返回默认配置
func (this *RealtimeVoiceClient) getDialogConfig() DialogPayload {
	if this.dialogConfig != nil {
		return *this.dialogConfig
	}
	// 默认 Dialog 配置 - 豆包助手示例
	return DialogPayload{
		BotName:       "豆包",
		SystemRole:    "你是豆包，一个由字节跳动开发的智能助手",
		SpeakingStyle: "友好、专业、有帮助。回答简洁明了，善于解答各类问题。",
		Extra: map[string]interface{}{
			"strict_audit":   false,
			"input_mod":      "audio",
			"model":          "O",
			"audit_response": "抱歉，我暂时无法回答这个问题。让我们聊点别的吧。",
			// 网络搜索功能默认禁用，用户可以根据需要启用
			"enable_volc_websearch": false,
		},
	}
}

func (this *RealtimeVoiceClient) startConnection() error {
	if this.doubaoWSConn == nil {
		return ErrNotConnected
	}
	msg, err := NewMessage(MsgTypeFullClient, MsgTypeFlagWithEvent)
	if err != nil {
		return fmt.Errorf("create StartSession request message: %w", err)
	}
	msg.Event = EVENT_START_CONNECTION
	msg.Payload = []byte("{}")

	frame, err := this.protocol.Marshal(msg)
	this.log.Infof("StartConnection frame: %v", frame)
	if err != nil {
		return fmt.Errorf("marshal StartConnection request message: %w", err)
	}

	if err := this.sendRequest(this.doubaoWSConn, frame); err != nil {
		return fmt.Errorf("send StartConnection request: %w", err)
	}

	// Read ConnectionStarted message.
	mt, frame, err := this.doubaoWSConn.ReadMessage()
	if err != nil {
		return fmt.Errorf("read ConnectionStarted response: %w", err)
	}
	if mt != websocket.BinaryMessage && mt != websocket.TextMessage {
		return fmt.Errorf("unexpected Websocket message type: %d", mt)
	}

	msg, _, err = Unmarshal(frame, this.protocol.containsSequence)
	if err != nil {
		this.log.Infof("StartConnection response: %s", frame)
		return fmt.Errorf("unmarshal ConnectionStarted response message: %w", err)
	}
	if msg.Type != MsgTypeFullServer {
		return fmt.Errorf("unexpected ConnectionStarted message type: %s", msg.Type)
	}
	if msg.Event != EVENT_CONNECTION_STARTED {
		return fmt.Errorf("unexpected response event (%d) for StartConnection request", msg.Event)
	}
	this.log.Infof("Connection started (event=%d) connectID: %s, payload: %s", msg.Event, msg.ConnectID, msg.Payload)

	return nil
}

func (this *RealtimeVoiceClient) startSession(req *StartSessionPayload) error {
	if this.doubaoWSConn == nil {
		return ErrNotConnected
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal StartSession request payload: %w", err)
	}

	msg, err := NewMessage(MsgTypeFullClient, MsgTypeFlagWithEvent)
	if err != nil {
		return fmt.Errorf("create StartSession request message: %w", err)
	}
	msg.Event = EVENT_START_SESSION
	msg.SessionID = this.SessionId
	msg.Payload = payload

	frame, err := this.protocol.Marshal(msg)
	//this.log.Infof("StartSession request frame: %v", frame)
	if err != nil {
		return fmt.Errorf("marshal StartSession request message: %w", err)
	}
	if err := this.sendRequest(this.doubaoWSConn, frame); err != nil {

		return fmt.Errorf("send StartSession request: %w", err)
	}

	// Read SessionStarted message.
	mt, frame, err := this.doubaoWSConn.ReadMessage()
	if err != nil {
		return fmt.Errorf("read SessionStarted response: %w", err)
	}
	if mt != websocket.BinaryMessage && mt != websocket.TextMessage {
		return fmt.Errorf("unexpected Websocket message type: %d", mt)
	}

	// Validate SessionStarted message.
	msg, _, err = Unmarshal(frame, this.protocol.containsSequence)
	if err != nil {
		this.log.Infof("StartSession response: %s", frame)
		return fmt.Errorf("unmarshal SessionStarted response message: %w", err)
	}
	if msg.Type != MsgTypeFullServer {
		return fmt.Errorf("unexpected SessionStarted message type: %s", msg.Type)
	}
	if msg.Event != 150 {
		return fmt.Errorf("unexpected response event (%d) for StartSession request", msg.Event)
	}
	this.log.Infof("SessionStarted response payload: %v", string(msg.Payload))
	var jsonData map[string]interface{}
	if err := json.Unmarshal(msg.Payload, &jsonData); err != nil {
		return fmt.Errorf("unmarshal SessionStarted response payload: %w", err)
	}
	this.dialogID = jsonData["dialog_id"].(string)
	return nil
}

func (this *RealtimeVoiceClient) SayHello(req *SayHelloPayload) error {
	if this.doubaoWSConn == nil {
		return ErrNotConnected
	}
	payload, err := json.Marshal(req)
	this.log.Infof("SayHello request payload: %s", string(payload))
	if err != nil {
		return fmt.Errorf("marshal SayHello request payload: %w", err)
	}

	msg, err := NewMessage(MsgTypeFullClient, MsgTypeFlagWithEvent)
	if err != nil {
		return fmt.Errorf("create SayHello request message: %w", err)
	}
	msg.Event = EVENT_SAY_HELLO
	msg.SessionID = this.SessionId
	msg.Payload = payload

	frame, err := this.protocol.Marshal(msg)
	this.log.Infof("SayHello frame: %v", frame)
	if err != nil {
		return fmt.Errorf("marshal SayHello request message: %w", err)
	}

	if err := this.sendRequest(this.doubaoWSConn, frame); err != nil {
		return fmt.Errorf("send SayHello request: %w", err)
	}
	return nil
}
func (this *RealtimeVoiceClient) ChatTextQuery(req *ChatTextQueryPayload) error {
	if this.doubaoWSConn == nil {
		return ErrNotConnected
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal ChatTextQuery request payload: %w", err)
	}
	msg, err := NewMessage(MsgTypeFullClient, MsgTypeFlagWithEvent)
	if err != nil {
		return fmt.Errorf("create ChatTextQuery request message: %w", err)
	}
	msg.Event = EVENT_CHAT_TEXT_QUERY
	msg.SessionID = this.SessionId
	msg.Payload = payload
	frame, err := this.protocol.Marshal(msg)
	//this.log.Infof("ChatTextQuery request frame: %v", frame)
	if err != nil {
		return fmt.Errorf("marshal ChatTextQuery request message: %w", err)
	}
	if err := this.doubaoWSConn.WriteMessage(websocket.BinaryMessage, frame); err != nil {
		return fmt.Errorf("send ChatTextQuery request: %w", err)
	}
	return nil
}

func (this *RealtimeVoiceClient) ChatTTSText(req *ChatTTSTextPayload) error {
	if this.doubaoWSConn == nil {
		return ErrNotConnected
	}
	if this.isUserQuerying.Load() {
		this.log.Warnf("chatTTSText cant be called while user is querying.")
		return ErrUserQuerying
	}
	payload, err := json.Marshal(req)
	this.log.Infof("ChatTTSText request payload: %s", string(payload))
	if err != nil {
		return fmt.Errorf("marshal ChatTTSText request payload: %w", err)
	}

	this.protocol.SetSerialization(SerializationJSON)
	msg, err := NewMessage(MsgTypeFullClient, MsgTypeFlagWithEvent)
	if err != nil {
		return fmt.Errorf("create ChatTTSText request message: %w", err)
	}
	msg.Event = EVENT_CHAT_TTS_TEXT
	msg.SessionID = this.SessionId
	msg.Payload = payload

	frame, err := this.protocol.Marshal(msg)
	//this.log.Infof("ChatTTSText frame: %v", frame)
	if err != nil {
		return fmt.Errorf("marshal ChatTTSText request message: %w", err)
	}
	if err := this.sendRequest(this.doubaoWSConn, frame); err != nil {
		return fmt.Errorf("send ChatTTSText request: %w", err)
	}
	return nil
}

// SpeakText mimics the TS speakText method: sends text in two packets (start and end).
func (this *RealtimeVoiceClient) SpeakText(text string) error {
	if !this.isUserQuerying.Load() {
		this.isSendingChatTTSText.Store(true)
		// First packet: start=true, content=text, end=false
		err := this.ChatTTSText(&ChatTTSTextPayload{
			Start:   true,
			Content: text,
			End:     false,
		})
		if err != nil {
			return fmt.Errorf("send first packet failed: %w", err)
		}
		this.log.Infof("Sent ChatTTSText first packet: %s", text)
		time.Sleep(time.Millisecond * 20)
		// Last packet: start=false, content="", end=true
		err = this.ChatTTSText(&ChatTTSTextPayload{
			Start:   false,
			Content: "",
			End:     true,
		})
		if err != nil {
			return fmt.Errorf("send last packet failed: %w", err)
		}
		this.log.Infof("Sent ChatTTSText last packet")
	}
	return nil
}

func (this *RealtimeVoiceClient) ChatRAGText(req *ChatRAGTextPayload) error {
	if this.doubaoWSConn == nil {
		return ErrNotConnected
	}
	payload, err := json.Marshal(req)
	this.log.Infof("ChatTTSText request payload: %s", string(payload))
	if err != nil {
		return fmt.Errorf("marshal ChatTTSText request payload: %w", err)
	}

	this.protocol.SetSerialization(SerializationJSON)
	msg, err := NewMessage(MsgTypeFullClient, MsgTypeFlagWithEvent)
	if err != nil {
		return fmt.Errorf("create ChatTTSText request message: %w", err)
	}
	msg.Event = EVENT_CHAT_RAG_TEXT
	msg.SessionID = this.SessionId
	msg.Payload = payload

	frame, err := this.protocol.Marshal(msg)
	//this.log.Infof("ChatRAGText frame: %v", frame)
	if err != nil {
		return fmt.Errorf("marshal ChatRAGText request message: %w", err)
	}
	if err := this.sendRequest(this.doubaoWSConn, frame); err != nil {
		return fmt.Errorf("send ChatRAGText request: %w", err)
	}
	return nil
}

func (this *RealtimeVoiceClient) SendAudio(content []byte) error {
	//if this.isSendingChatTTSText.Load() {
	//	return nil
	//}
	if this.doubaoWSConn == nil {
		return ErrNotConnected
	}
	// 16000Hz, 16bit, 1 channel (PCM format matching client recording rate)
	//this.log.Debugf("[SendAudio] 准备发送音频: %d bytes (%.2f samples)", len(content), float64(len(content))/2.0)

	// 设置为 Raw 序列化，不压缩（protocol.Marshal不支持自动压缩payload）
	this.protocol.SetSerialization(SerializationRaw)
	// 保持无压缩 - protocol.Marshal() 不会真正压缩数据

	msg, err := NewMessage(MsgTypeAudioOnlyClient, MsgTypeFlagWithEvent)
	if err != nil {
		this.log.Errorf("create Task request message: %v", err)
		// 恢复默认设置
		this.protocol.SetSerialization(SerializationJSON)
		return err
	}
	msg.Event = EVENT_TASK_REQUEST
	msg.SessionID = this.SessionId
	msg.Payload = content
	frame, err := this.protocol.Marshal(msg)
	if err != nil {
		this.log.Errorf("marshal TaskRequest message: %v", err)
		// 恢复默认设置
		this.protocol.SetSerialization(SerializationJSON)
		return err
	}

	// 恢复默认设置（JSON + 无压缩）
	this.protocol.SetSerialization(SerializationJSON)

	if err = this.sendRequest(this.doubaoWSConn, frame); err != nil {
		this.log.Errorf("send TaskRequest request: %v", err)
		return err
	}
	//this.log.Debugf("[SendAudio] 成功发送音频帧: %d bytes", len(frame))
	return nil
}

func (this *RealtimeVoiceClient) Interrupt() error {
	this.log.Info("Sending Interrupt signal (simulated by client logic if needed, or protocol specific)")
	if this.OnInterrupt != nil {
		this.OnInterrupt()
	}
	return nil
}

// GetConnectionState returns the current connection state (thread-safe)
func (c *RealtimeVoiceClient) GetConnectionState() ConnectionState {
	if c.stateManager == nil {
		return StateDisconnected
	}
	return c.stateManager.GetState()
}

func (c *RealtimeVoiceClient) Close() error {
	// Set flag to indicate manual closing (not a connection error)
	c.isManuallyClosing = true

	// Stop any ongoing reconnection attempts
	select {
	case c.stopReconnect <- struct{}{}:
	default:
	}

	if c.doubaoWSConn == nil {
		return nil
	}

	// Update connection state
	if c.stateManager != nil {
		c.stateManager.SetState(StateDisconnected)
	}

	_ = c.finishSession()
	_ = c.finishConnection()
	return c.doubaoWSConn.Close()
}

func (c *RealtimeVoiceClient) Stop() error {
	return c.Close()
}

func (c *RealtimeVoiceClient) SendText(text string) error {
	return c.ChatTextQuery(&ChatTextQueryPayload{Content: text})
}

func (this *RealtimeVoiceClient) finishSession() error {
	if this.doubaoWSConn == nil {
		return ErrNotConnected
	}
	msg, err := NewMessage(MsgTypeFullClient, MsgTypeFlagWithEvent)
	if err != nil {
		return fmt.Errorf("create FinishSession request message: %w", err)
	}
	msg.Event = EVENT_FINISH_SESSION
	msg.SessionID = this.SessionId
	msg.Payload = []byte("{}")

	frame, err := this.protocol.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal FinishSession request message: %w", err)
	}

	if err := this.sendRequest(this.doubaoWSConn, frame); err != nil {
		return fmt.Errorf("send FinishSession request: %w", err)
	}

	this.log.Info("FinishSession request is sent.")
	return nil
}

func (this *RealtimeVoiceClient) finishConnection() error {
	if this.doubaoWSConn == nil {
		return ErrNotConnected
	}
	msg, err := NewMessage(MsgTypeFullClient, MsgTypeFlagWithEvent)
	if err != nil {
		return fmt.Errorf("create FinishConnection request message: %w", err)
	}
	msg.Event = EVENT_FINISH_CONNECTION
	msg.Payload = []byte("{}")

	frame, err := this.protocol.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal FinishConnection request message: %w", err)
	}

	if err := this.sendRequest(this.doubaoWSConn, frame); err != nil {
		return fmt.Errorf("send FinishConnection request: %w", err)
	}

	// Read ConnectionStarted message.
	mt, frame, err := this.doubaoWSConn.ReadMessage()
	if err != nil {
		return fmt.Errorf("read ConnectionFinished response: %w", err)
	}
	if mt != websocket.BinaryMessage && mt != websocket.TextMessage {
		return fmt.Errorf("unexpected Websocket message type: %d", mt)
	}

	msg, _, err = Unmarshal(frame, this.protocol.containsSequence)
	if err != nil {
		this.log.Infof("FinishConnection response: %s", frame)
		return fmt.Errorf("unmarshal ConnectionFinished response message: %w", err)
	}
	if msg.Type != MsgTypeFullServer {
		return fmt.Errorf("unexpected ConnectionFinished message type: %s", msg.Type)
	}
	if msg.Event != 52 {
		return fmt.Errorf("unexpected response event (%d) for FinishConnection request", msg.Event)
	}

	this.log.Infof("Connection finished (event=%d).", msg.Event)
	return nil
}

func (this *RealtimeVoiceClient) sendRequest(conn *websocket.Conn, frame []byte) error {
	if conn == nil {
		return fmt.Errorf("websocket connection is nil")
	}
	this.wsWriteLock.Lock()
	defer this.wsWriteLock.Unlock()

	if conn == nil {
		return fmt.Errorf("websocket connection is nil")
	}
	if err := conn.WriteMessage(websocket.BinaryMessage, frame); err != nil {
		return fmt.Errorf("send SayHello request: %w", err)
	}
	this.localSequence.Add(1)
	//this.log.Infof("Send request, sequence: %d", localSequence.Load())
	return nil
}

type StartSessionPayload struct {
	ASR    ASRPayload    `json:"asr"`
	TTS    TTSPayload    `json:"tts"`
	Dialog DialogPayload `json:"dialog"`
}

type ASRPayload struct {
	Format  string                 `json:"format"`
	Rate    int                    `json:"rate"`
	Bits    int                    `json:"bits"`
	Channel int                    `json:"channel"`
	Extra   map[string]interface{} `json:"extra"`
}

type TTSPayload struct {
	Speaker     string      `json:"speaker"`
	AudioConfig AudioConfig `json:"audio_config"`
}
type AudioConfig struct {
	Channel    int    `json:"channel"`
	Format     string `json:"format"`
	SampleRate int    `json:"sample_rate"`
}

type DialogPayload struct {
	DialogID          string                 `json:"dialog_id"`
	BotName           string                 `json:"bot_name"`
	SystemRole        string                 `json:"system_role"`
	SpeakingStyle     string                 `json:"speaking_style"`
	CharacterManifest string                 `json:"character_manifest,omitempty"`
	Location          *LocationInfo          `json:"location,omitempty"`
	Extra             map[string]interface{} `json:"extra"`
}
type LocationInfo struct {
	Longitude   float64 `json:"longitude"`
	Latitude    float64 `json:"latitude"`
	City        string  `json:"city"`
	Country     string  `json:"country"`
	Province    string  `json:"province"`
	District    string  `json:"district"`
	Town        string  `json:"town"`
	CountryCode string  `json:"country_code"`
	Address     string  `json:"address"`
}

type SayHelloPayload struct {
	Content string `json:"content"`
}
type ChatTextQueryPayload struct {
	Content string `json:"content"`
}
type ChatTTSTextPayload struct {
	Start   bool   `json:"start"`
	End     bool   `json:"end"`
	Content string `json:"content"`
}
type ChatRAGTextPayload struct {
	ExternalRAG string `json:"external_rag"`
}
type RAGObject struct {
	Title   string `json:"title"`
	Content string `json:"content"`
}
