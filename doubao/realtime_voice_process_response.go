package doubao

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/gorilla/websocket"
)

// EventHandler 定义事件处理函数类型
type EventHandler func(*RealtimeVoiceClient, *Message) error

// initEventHandlers 初始化事件处理器映射表
func (this *RealtimeVoiceClient) initEventHandlers() map[int32]EventHandler {
	return map[int32]EventHandler{
		EVENT_CONNECTION_STARTED:        (*RealtimeVoiceClient).handleConnectionStarted,
		EVENT_CONNECTION_FAILED:         (*RealtimeVoiceClient).handleConnectionFailed,
		EVENT_CONNECTION_FINISHED:       (*RealtimeVoiceClient).handleConnectionFinished,
		EVENT_SESSION_STARTED:           (*RealtimeVoiceClient).handleSessionStarted,
		EVENT_SESSION_FINISHED:          (*RealtimeVoiceClient).handleSessionFinished,
		EVENT_SESSION_FAILED:            (*RealtimeVoiceClient).handleSessionFailed,
		EVENT_ASR_STARTED:               (*RealtimeVoiceClient).handleASRStarted,
		EVENT_ASR_RESPONSE:              (*RealtimeVoiceClient).handleASRResponse,
		EVENT_ASR_ENDED:                 (*RealtimeVoiceClient).handleASREnded,
		EVENT_TTS_SENTENCE_START:        (*RealtimeVoiceClient).handleTTSSentenceStart,
		EVENT_TTS_SENTENCE_END:          (*RealtimeVoiceClient).handleTTSSentenceEnd,
		EVENT_TTS_ENDED:                 (*RealtimeVoiceClient).handleTTSEnded,
		EVENT_CHAT_RESPONSE:             (*RealtimeVoiceClient).handleChatResponse,
		EVENT_CHAT_ENDED:                (*RealtimeVoiceClient).handleChatEnded,
		EVENT_CHAT_TEXT_QUERY_CONFIRMED: (*RealtimeVoiceClient).handleChatTextQueryConfirmed,
		EVENT_CONVERSATION_CREATED:      (*RealtimeVoiceClient).handleConversationCreated,
		EVENT_CONVERSATION_UPDATED:      (*RealtimeVoiceClient).handleConversationUpdated,
		EVENT_CONVERSATION_RETRIEVED:    (*RealtimeVoiceClient).handleConversationRetrieved,
		EVENT_CONVERSATION_DELETED:      (*RealtimeVoiceClient).handleConversationDeleted,
		EVENT_DIALOG_COMMON_ERROR:       (*RealtimeVoiceClient).handleDialogCommonError,
		EVENT_USAGE_INFO:                (*RealtimeVoiceClient).handleUsageInfo,
	}
}

func (this *RealtimeVoiceClient) handleMessage() {
	// 启动音频捕获和播放（如果启用了 PortAudio）
	this.startAudioCapture()
	this.startAudioPlayback()

	// 初始化事件处理器映射表
	eventHandlers := this.initEventHandlers()

	this.log.Info("Waiting for message...")
	for {
		msg, err := this.receiveMessage()
		if err != nil {
			this.log.Errorf("Receive message error: %v", err)
			if this.OnError != nil {
				go this.OnError(err) // 异步执行
			}
			return
		}

		// Handle Audio (ServerACK)
		if msg.Type == MsgTypeAudioOnlyServer && len(msg.Payload) > 0 {
			if this.enableEventLog {
				this.log.Infof("Receive audio message (len=%d)", len(msg.Payload))
			}
			if this.OnAudio != nil {
				go this.OnAudio(msg.Payload) // 异步执行
			}
			if this.isUsePortAudio {
				this.handleIncomingAudio(msg.Payload)
			}
			continue
		}

		// Handle FullServer events
		if msg.Type == MsgTypeFullServer {
			eventName := GetEventName(msg.Event)
			if this.enableEventLog {
				this.log.Infof("Receive event %d (%s): %s", msg.Event, eventName, msg.Payload)
			}

			// 使用 map 查找并执行对应的事件处理器
			if handler, ok := eventHandlers[msg.Event]; ok {
				if err := handler(this, msg); err != nil {
					this.log.Errorf("Handle event %d error: %v", msg.Event, err)
					// 检查是否需要终止连接
					if msg.Event == EVENT_SESSION_FINISHED || msg.Event == EVENT_SESSION_FAILED {
						return
					}
				}
			} else {
				this.log.Warnf("No handler for event %d (%s)", msg.Event, eventName)
			}
			continue
		}

		// Handle Error messages
		if msg.Type == MsgTypeError {
			this.log.Errorf("Receive Error message (code=%d): %s", msg.ErrorCode, string(msg.Payload))
			if this.OnError != nil {
				go this.OnError(fmt.Errorf("server error code %d: %s", msg.ErrorCode, string(msg.Payload))) // 异步执行
			}
			return
		}
	}
}

// ==================== 事件处理器实现 ====================

func (this *RealtimeVoiceClient) handleConnectionStarted(msg *Message) error {
	if this.enableEventLog {
		this.log.Info("Connection started")
	}
	if this.OnConnectionStarted != nil {
		go this.OnConnectionStarted(msg) // 异步执行
	}
	return nil
}

func (this *RealtimeVoiceClient) handleConnectionFailed(msg *Message) error {
	this.log.Errorf("[豆包] 连接失败: %s", msg.Payload)
	if this.OnConnectionFailed != nil {
		go this.OnConnectionFailed(msg) // 异步执行
	}
	return nil
}

func (this *RealtimeVoiceClient) handleConnectionFinished(msg *Message) error {
	if this.enableEventLog {
		this.log.Info("Connection finished")
	}
	if this.OnConnectionFinished != nil {
		go this.OnConnectionFinished(msg) // 异步执行
	}
	return nil
}

func (this *RealtimeVoiceClient) handleSessionStarted(msg *Message) error {
	if this.enableEventLog {
		this.log.Info("Session started")
	}
	if this.OnSessionStarted != nil {
		go this.OnSessionStarted(msg) // 异步执行
	}
	return nil
}

func (this *RealtimeVoiceClient) handleSessionFinished(msg *Message) error {
	if this.enableEventLog {
		this.log.Info("Session finished")
	}
	if this.OnSessionFinished != nil {
		go this.OnSessionFinished(msg) // 异步执行
	}
	return fmt.Errorf("session finished") // 返回错误以终止连接
}

func (this *RealtimeVoiceClient) handleSessionFailed(msg *Message) error {
	if this.enableEventLog {
		this.log.Info("Session failed")
	}
	if this.OnSessionFailed != nil {
		go this.OnSessionFailed(msg) // 异步执行
	}
	return fmt.Errorf("session failed") // 返回错误以终止连接
}

func (this *RealtimeVoiceClient) handleASRStarted(msg *Message) error {
	if this.enableEventLog {
		this.log.Info("ASR Started - User started speaking")
	}
	if this.OnInterrupt != nil {
		go this.OnInterrupt() // 异步执行
	}
	return nil
}

func (this *RealtimeVoiceClient) handleASRResponse(msg *Message) error {
	if this.enableEventLog {
		this.log.Infof("[ASR响应] 收到识别结果")
	}
	var payload ASRResponse
	if err := json.Unmarshal(msg.Payload, &payload); err == nil && len(payload.Results) > 0 {
		res := payload.Results[0]
		if this.OnInputTranscriptPartial != nil && res.IsInterim {
			go this.OnInputTranscriptPartial(res.Text, false) // 异步执行
		}
		if this.OnInputTranscript != nil && !res.IsInterim {
			go this.OnInputTranscript(res.Text, true) // 异步执行
		}
	} else {
		this.log.Warnf("[ASR响应] 解析失败或结果为空: %v", err)
	}
	return nil
}

func (this *RealtimeVoiceClient) handleASREnded(msg *Message) error {
	if this.enableEventLog {
		this.log.Info("ASR Ended - User stopped speaking")
	}
	// Critical timing: this is when ChatTTSText should be sent
	if this.OnASREnded != nil {
		go this.OnASREnded() // 🎯 异步执行 - 最关键的修复！
	}
	return nil
}

func (this *RealtimeVoiceClient) handleTTSSentenceStart(msg *Message) error {
	// 发送ChatTTSText请求事件之后，收到tts_type为chat_tts_text的事件，清空本地缓存的S2S模型闲聊音频数据
	if this.enableEventLog {
		this.log.Info("TTS Sentence Start")
	}
	this.bufferLock.Lock()
	this.buffer = this.buffer[:0]
	this.s16Buffer = this.s16Buffer[:0]
	this.bufferLock.Unlock()

	var payload TTSSentenceStartResponse
	if err := json.Unmarshal(msg.Payload, &payload); err == nil {
		if this.OnTTSStart != nil {
			metadata := &MsgMetadata{
				TTSType:    payload.TTSType,
				TTSTaskID:  "",
				QuestionID: payload.QuestionID,
				ReplyID:    payload.ReplyID,
				Timestamp:  time.Now().UnixMilli(),
			}
			go this.OnTTSStart(metadata) // 异步执行
		}
	}
	return nil
}

func (this *RealtimeVoiceClient) handleTTSSentenceEnd(msg *Message) error {
	var payload TTSSentenceEndResponse
	if err := json.Unmarshal(msg.Payload, &payload); err == nil {
		text := payload.SentenceText
		if text == "" {
			text = payload.Text
		}
		if this.enableEventLog {
			this.log.Infof("TTS Sentence End: %s", text)
		}
		if this.OnOutputTranscript != nil {
			metadata := &MsgMetadata{
				TTSType:    payload.TTSType,
				TTSTaskID:  payload.TTSTaskID,
				QuestionID: payload.QuestionID,
				ReplyID:    payload.ReplyID,
				Timestamp:  time.Now().UnixMilli(),
			}
			go this.OnOutputTranscript(text, metadata) // 异步执行
		}
	}
	return nil
}

func (this *RealtimeVoiceClient) handleTTSEnded(msg *Message) error {
	if this.enableEventLog {
		this.log.Info("TTS Ended - Audio synthesis completed")
	}
	var payload TTSEndedResponse
	if err := json.Unmarshal(msg.Payload, &payload); err == nil {
		if this.enableEventLog {
			this.log.Infof("TTS Ended - QuestionID: %s, ReplyID: %s", payload.QuestionID, payload.ReplyID)
		}
	}
	if this.OnTTSEnded != nil {
		go this.OnTTSEnded(msg) // 异步执行
	}
	return nil
}

func (this *RealtimeVoiceClient) handleChatResponse(msg *Message) error {
	var payload ChatResponse
	if err := json.Unmarshal(msg.Payload, &payload); err == nil {
		if this.OnChatResponse != nil {
			this.OnChatResponse(payload.Content) // 异步执行
		}
	}
	return nil
}

func (this *RealtimeVoiceClient) handleChatEnded(msg *Message) error {
	if this.enableEventLog {
		this.log.Info("Chat response ended")
	}
	if this.OnChatEnded != nil {
		go this.OnChatEnded(msg) // 异步执行
	}
	return nil
}

func (this *RealtimeVoiceClient) handleChatTextQueryConfirmed(msg *Message) error {
	if this.enableEventLog {
		this.log.Info("ChatTextQuery confirmed")
	}
	var payload ChatTextQueryConfirmedResponse
	if err := json.Unmarshal(msg.Payload, &payload); err == nil {
		if this.enableEventLog {
			this.log.Infof("ChatTextQuery confirmed - QuestionID: %s", payload.QuestionID)
		}
	}
	if this.OnChatTextQueryConfirmed != nil {
		go this.OnChatTextQueryConfirmed(msg) // 异步执行
	}
	return nil
}

func (this *RealtimeVoiceClient) handleConversationCreated(msg *Message) error {
	if this.enableEventLog {
		this.log.Info("Conversation context created")
	}
	var payload ConversationCreatedResponse
	if err := json.Unmarshal(msg.Payload, &payload); err == nil {
		if this.enableEventLog {
			this.log.Infof("Conversation created - %d item(s)", len(payload.Items))
		}
	}
	if this.OnConversationCreated != nil {
		go this.OnConversationCreated(msg) // 异步执行
	}
	return nil
}

func (this *RealtimeVoiceClient) handleConversationUpdated(msg *Message) error {
	if this.enableEventLog {
		this.log.Info("Conversation context updated")
	}
	var payload ConversationUpdatedResponse
	if err := json.Unmarshal(msg.Payload, &payload); err == nil {
		if payload.Message != "" {
			this.log.Warnf("Conversation update failed: %s", payload.Message)
		} else if this.enableEventLog {
			this.log.Info("Conversation updated successfully")
		}
	}
	if this.OnConversationUpdated != nil {
		go this.OnConversationUpdated(msg) // 异步执行
	}
	return nil
}

func (this *RealtimeVoiceClient) handleConversationRetrieved(msg *Message) error {
	if this.enableEventLog {
		this.log.Info("Conversation context retrieved")
	}
	var payload ConversationRetrievedResponse
	if err := json.Unmarshal(msg.Payload, &payload); err == nil {
		if this.enableEventLog {
			this.log.Infof("Conversation retrieved - %d item(s)", len(payload.Items))
		}
	}
	if this.OnConversationRetrieved != nil {
		go this.OnConversationRetrieved(msg) // 异步执行
	}
	return nil
}

func (this *RealtimeVoiceClient) handleConversationDeleted(msg *Message) error {
	if this.enableEventLog {
		this.log.Info("Conversation context deleted")
	}
	var payload ConversationDeletedResponse
	if err := json.Unmarshal(msg.Payload, &payload); err == nil {
		if this.enableEventLog {
			if payload.StatusCode == 40000010 {
				this.log.Info("No conversation items were deleted (empty)")
			} else {
				this.log.Infof("Conversation deleted - %d item(s) removed", len(payload.Items))
			}
		}
	}
	if this.OnConversationDeleted != nil {
		go this.OnConversationDeleted(msg) // 异步执行
	}
	return nil
}

func (this *RealtimeVoiceClient) handleDialogCommonError(msg *Message) error {
	this.log.Error("Dialog common error occurred")
	this.log.Errorf("Dialog error: %s", string(msg.Payload))
	if this.OnError != nil {
		go this.OnError(fmt.Errorf("dialog error: %s", string(msg.Payload)))
	}
	return nil
}

func (this *RealtimeVoiceClient) handleUsageInfo(msg *Message) error {
	if this.enableEventLog {
		this.log.Info("Usage info received")
	}
	if this.OnUsageInfo != nil {
		go this.OnUsageInfo(msg) // 异步执行
	}
	return nil
}

// ==================== 辅助函数 ====================

func (this *RealtimeVoiceClient) receiveMessage() (*Message, error) {
	if this.doubaoWSConn == nil {
		return nil, fmt.Errorf("doubao websocket connection is nil")
	}
	mt, frame, err := this.doubaoWSConn.ReadMessage()
	if err != nil {
		return nil, err
	}
	if mt != websocket.BinaryMessage && mt != websocket.TextMessage {
		return nil, fmt.Errorf("unexpected Websocket message type: %d", mt)
	}

	framePrefix := frame
	if len(frame) > 100 {
		framePrefix = frame[:100]
	}
	_ = framePrefix
	//this.log.Infof("Receive frame prefix: %v", framePrefix)
	msg, _, err := Unmarshal(frame, ContainsSequence)
	if err != nil {
		if len(frame) > 500 {
			frame = frame[:500]
		}
		this.log.Infof("Data response: %s", frame)
		return nil, fmt.Errorf("unmarshal response message: %w", err)
	}
	return msg, nil
}

const (
	sampleRate      = 24000
	channels        = 1
	framesPerBuffer = 512
	bufferSeconds   = 100 // 最多缓冲100秒数据
	DefaultPCM      = "pcm"
	PcmS16LE        = "pcm_s16le"
)

func (this *RealtimeVoiceClient) handleIncomingAudio(data []byte) {
	switch this.pcmFormat {
	case PcmS16LE:
		if this.enableEventLog {
			this.log.Infof("Received audio byte len: %d, float16 len: %d", len(data), len(data)/2)
		}
		sampleCount := len(data) / 2
		samples := make([]int16, sampleCount)
		for i := 0; i < sampleCount; i++ {
			bits := binary.LittleEndian.Uint16(data[i*2 : (i+1)*2])
			samples[i] = int16(bits)
		}
		// 将音频加载到缓冲区
		this.bufferLock.Lock()
		defer this.bufferLock.Unlock()
		this.s16Buffer = append(this.s16Buffer, samples...)
		if len(this.s16Buffer) > sampleRate*bufferSeconds {
			this.s16Buffer = this.s16Buffer[len(this.s16Buffer)-(sampleRate*bufferSeconds):]
		}
		if this.enableEventLog {
			this.log.Infof("Audio buffer size: %d samples", len(this.s16Buffer))
		}
	case DefaultPCM:
		if this.enableEventLog {
			this.log.Infof("Received audio byte len: %d, float32 len: %d", len(data), len(data)/4)
		}
		sampleCount := len(data) / 4
		samples := make([]float32, sampleCount)
		for i := 0; i < sampleCount; i++ {
			bits := binary.LittleEndian.Uint32(data[i*4 : (i+1)*4])
			samples[i] = math.Float32frombits(bits)
		}
		// 将音频加载到缓冲区
		this.bufferLock.Lock()
		defer this.bufferLock.Unlock()
		this.buffer = append(this.buffer, samples...)
		if len(this.buffer) > sampleRate*bufferSeconds {
			this.buffer = this.buffer[len(this.buffer)-(sampleRate*bufferSeconds):]
		}
		if this.enableEventLog {
			this.log.Infof("Audio buffer size: %d samples", len(this.buffer))
		}
	}

}
