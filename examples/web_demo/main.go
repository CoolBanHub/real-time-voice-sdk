package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/CoolBanHub/real-time-voice-sdk/doubao"
	"github.com/CoolBanHub/real-time-voice-sdk/pkg/log"
	"github.com/gorilla/websocket"
)

// WebSocket 实时语音对话示例
//
// 此示例展示如何通过浏览器 WebSocket 进行实时语音对话：
// 1. 浏览器通过 WebSocket 发送音频数据到后端
// 2. 后端转发音频到豆包服务器
// 3. 接收豆包服务器的响应并返回给浏览器
//
// 运行方法：
//   export DOUBAO_APP_ID=your-app-id
//   export DOUBAO_ACCESS_TOKEN=your-token
//   go run main.go
//   然后在浏览器打开 http://localhost:8080

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true // 允许所有来源（生产环境需要限制）
		},
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}
)

// WebSocket 消息类型
type WSMessage struct {
	Type    string          `json:"type"`    // 消息类型：audio, text, control
	Payload json.RawMessage `json:"payload"` // 消息内容
}

// 客户端会话
type ClientSession struct {
	ws            *websocket.Conn
	doubaoClient  *doubao.RealtimeVoiceClient
	logger        *log.Helper
	writeToWs     sync.Mutex // WebSocket 写锁，避免并发写入
	closeOnce     sync.Once
	isActive      bool
	latestText    string
	latestTextMux sync.Mutex
}

func main() {
	// 从环境变量读取配置
	appID := os.Getenv("DOUBAO_APP_ID")
	accessToken := os.Getenv("DOUBAO_ACCESS_TOKEN")
	if appID == "" || accessToken == "" {
		fmt.Println("请设置环境变量：DOUBAO_APP_ID 和 DOUBAO_ACCESS_TOKEN")
		fmt.Println("示例：")
		fmt.Println("  export DOUBAO_APP_ID=your-app-id")
		fmt.Println("  export DOUBAO_ACCESS_TOKEN=your-access-token")
		os.Exit(1)
	}

	// 创建日志
	logger := log.NewStdLogger(os.Stdout)
	logger = log.With(logger, "caller", log.Caller(4), "ts", log.DefaultTimestamp)

	fmt.Println("==============================================")
	fmt.Println("  豆包实时语音对话 Web 服务")
	fmt.Println("==============================================")
	fmt.Println()

	// 检查 public 目录是否存在
	publicDir := "./public"
	if _, err := os.Stat(publicDir); os.IsNotExist(err) {
		fmt.Printf("❌ 错误: public 目录不存在 (%s)\n", publicDir)
		fmt.Println("请确保在 examples/web_demo 目录下运行此程序")
		os.Exit(1)
	}

	// 设置 HTTP 路由
	http.HandleFunc("/ws/voice-assistant", func(w http.ResponseWriter, r *http.Request) {
		handleVoiceAssistant(w, r, appID, accessToken, log.NewHelper(logger))
	})

	// 静态文件服务（必须在所有具体路由之后）
	fs := http.FileServer(http.Dir(publicDir))
	http.Handle("/", fs)

	fmt.Printf("📁 静态文件目录: %s\n", publicDir)

	// 启动 HTTP 服务器
	port := ":8080"
	fmt.Printf("🚀 服务已启动: http://localhost%s\n", port)
	fmt.Println("   请在浏览器中打开上述地址")
	fmt.Println()

	// 在单独的 goroutine 中启动服务器
	go func() {
		if err := http.ListenAndServe(port, nil); err != nil {
			fmt.Printf("❌ 服务器错误: %v\n", err)
			os.Exit(1)
		}
	}()

	// 等待中断信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\n正在关闭服务器...")
}

// handleVoiceAssistant 处理 WebSocket 连接
func handleVoiceAssistant(w http.ResponseWriter, r *http.Request, appID, accessToken string, logger *log.Helper) {
	// Extract voice parameter from URL query string
	voice := r.URL.Query().Get("voice")
	if voice == "" {
		voice = "zh_female_vv_jupiter_bigtts" // Default voice
	}

	// 升级 HTTP 连接为 WebSocket
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Errorf("WebSocket 升级失败: %v", err)
		return
	}

	logger.Info("新的 WebSocket 连接已建立")

	// 创建豆包客户端配置
	cfg := &doubao.RealtimeVoiceClientConfig{
		Appid:       appID,
		AccessToken: accessToken,
		Speaker:     voice,
		Log:         logger,
		// 启用事件日志用于调试（可选）
		EnableEventLog: false,
		// 重连配置
		Reconnect: &doubao.ReconnectConfig{
			Enabled:           true,
			MaxAttempts:       5,
			InitialDelay:      2 * time.Second,
			MaxDelay:          30 * time.Second,
			BackoffMultiplier: 2.0,
		},
	}

	// 创建豆包客户端
	doubaoClient := doubao.NewRealtimeVoiceClient(cfg)

	// 创建会话
	session := &ClientSession{
		ws:            ws,
		doubaoClient:  doubaoClient,
		logger:        logger,
		isActive:      true,
		latestText:    "",
		latestTextMux: sync.Mutex{},
		writeToWs:     sync.Mutex{},
	}

	// 设置豆包客户端回调
	setupDoubaoCallbacks(session)

	// 启动豆包连接
	if err := doubaoClient.Start(); err != nil {
		logger.Errorf("启动豆包连接失败: %v", err)
		session.sendToClient(map[string]interface{}{
			"type":    "error",
			"payload": map[string]interface{}{"error": fmt.Sprintf("连接失败: %v", err)},
		})
		ws.Close()
		return
	}
	defer doubaoClient.Close()

	logger.Info("豆包连接已建立")

	// 启动实时对话会话
	if err := doubaoClient.RealTimeDialog(); err != nil {
		logger.Errorf("启动对话失败: %v", err)
		session.sendToClient(map[string]interface{}{
			"type":    "error",
			"payload": map[string]interface{}{"error": fmt.Sprintf("启动对话失败: %v", err)},
		})
		ws.Close()
		return
	}

	logger.Info("实时对话会话已启动")

	// 发送连接成功消息
	session.sendToClient(map[string]interface{}{
		"type":      "ready",
		"text":      "WebSocket connection established",
		"timestamp": time.Now().UnixMilli(),
	})

	// 处理客户端消息
	for session.isActive {
		messageType, data, err := ws.ReadMessage()
		if err != nil {
			// 区分正常关闭和异常错误
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				logger.Info("[WebSocket] 客户端正常断开连接")
			} else if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				logger.Errorf("[WebSocket] 客户端异常断开: %v", err)
			} else {
				logger.Errorf("[WebSocket] 客户端错误: %v", err)
			}
			session.Cleanup()
			return
		}

		// 处理二进制消息（音频）
		if messageType == websocket.BinaryMessage {
			if len(data) > 0 {
				streamType := data[0]
				if streamType == 0 {
					// 音频数据
					audioData := data[1:]
					if err := doubaoClient.SendAudio(audioData); err != nil {
						logger.Errorf("[WebSocket] 发送音频到豆包失败: %v", err)
						session.sendToClient(map[string]interface{}{
							"type":    "error",
							"payload": map[string]interface{}{"error": fmt.Sprintf("发送音频失败: %v", err)},
						})
					}
				}
			}
		} else if messageType == websocket.TextMessage {
			//todo 前端没有支持不会有
		}
	}
}

type BufferedEvent struct {
	Type     string
	Data     interface{}
	Metadata map[string]interface{}
}

// setupDoubaoCallbacks 设置豆包客户端回调
func setupDoubaoCallbacks(session *ClientSession) {
	client := session.doubaoClient

	// 接收音频数据 - 直接发送二进制数据
	client.OnAudio = func(audioData []byte) {
		session.sendToClient(audioData)
	}

	// ASR 最终识别结果
	client.OnInputTranscript = func(text string, isFinal bool) {
		// Create metadata map from available info
		metadata := map[string]interface{}{
			"isFinal":   isFinal,
			"isInterim": !isFinal,
			"resultType": func() string {
				if isFinal {
					return "final"
				}
				return "partial"
			}(),
		}
		// Extract result type for logging
		resultType := "final"
		var isInterim bool
		var questionID string
		timestamp := time.Now().UnixMilli()

		if metadata != nil {
			if rt, ok := metadata["resultType"].(string); ok {
				resultType = rt
			}
			if interim, ok := metadata["isInterim"].(bool); ok {
				isInterim = interim
			}
			if qid, ok := metadata["questionId"].(string); ok {
				questionID = qid
			}
			if ts, ok := metadata["timestamp"].(int64); ok {
				timestamp = ts
			}
		}
		session.sendEvent("asr", "transcript", map[string]interface{}{
			"text": text,
		}, map[string]interface{}{
			"ts":   timestamp,
			"done": !isInterim,
			"ref": map[string]interface{}{
				"questionId": questionID,
			},
			"meta": map[string]interface{}{
				"resultType": resultType,
				"isInterim":  isInterim,
			},
		})
	}

	// ASR 临时识别结果
	client.OnInputTranscriptPartial = func(text string, isFinal bool) {
		if isFinal {
			metadata := map[string]interface{}{
				"isFinal": isFinal,
			}
			// Extract result type for logging
			resultType := "unknown"
			if metadata != nil {
				if rt, ok := metadata["resultType"].(string); ok {
					resultType = rt
				}
			}
			// Extract metadata fields
			var questionID string
			var isInterim bool
			timestamp := time.Now().UnixMilli()

			if metadata != nil {
				if qid, ok := metadata["questionId"].(string); ok {
					questionID = qid
				}
				if ts, ok := metadata["timestamp"].(int64); ok {
					timestamp = ts
				}
				if interim, ok := metadata["isInterim"].(bool); ok {
					isInterim = interim
				}
			}

			session.sendEvent("asr", "transcript", map[string]interface{}{
				"text": text,
			}, map[string]interface{}{
				"ts":   timestamp,
				"done": false,
				"ref": map[string]interface{}{
					"questionId": questionID,
				},
				"meta": map[string]interface{}{
					"resultType": resultType,
					"isInterim":  isInterim,
				},
			})
		}
	}

	// ASR 结束
	client.OnASREnded = func() { // todo
		session.sendToClient(map[string]interface{}{
			"type": "control",
			"payload": map[string]interface{}{
				"event": "asr_ended",
				"data":  nil,
			},
		})
	}

	// TTS 开始
	client.OnTTSStart = func(metadata *doubao.MsgMetadata) {
		session.sendToClient(map[string]interface{}{ // todo
			"type": "control",
			"payload": map[string]interface{}{
				"event": "tts_start",
				"data": map[string]interface{}{
					"tts_type": metadata.TTSType,
				},
			},
		})
	}

	// TTS 输出文本
	client.OnOutputTranscript = func(text string, metadata *doubao.MsgMetadata) {
		session.sendToClient(map[string]interface{}{ // todo
			"type": "transcript",
			"payload": map[string]interface{}{
				"direction": "output",
				"text":      text,
				"is_final":  true,
			},
		})
	}

	// TTS 结束
	client.OnTTSEnded = func(msg *doubao.Message) { // todo
		session.sendToClient(map[string]interface{}{
			"type": "control",
			"payload": map[string]interface{}{
				"event": "tts_ended",
				"data":  nil,
			},
		})
	}

	// LLM 流式响应
	client.OnChatResponse = func(content string) {
		metadata := map[string]interface{}{}
		questionID := ""
		if metadata != nil {
			if qid, ok := metadata["questionId"].(string); ok {
				questionID = qid
			}
		}

		session.latestTextMux.Lock()
		session.latestText += content
		session.latestTextMux.Unlock()

		event := BufferedEvent{
			Type:     "chat_response",
			Data:     content,
			Metadata: metadata,
		}
		session.processBufferedEvent(event)
		_ = questionID
	}

	// LLM 响应结束
	client.OnChatEnded = func(msg *doubao.Message) {
		questionID := ""
		metadata := map[string]interface{}{}
		if metadata != nil {
			if qid, ok := metadata["questionId"].(string); ok {
				questionID = qid
			}
		}
		session.latestTextMux.Lock()
		text := session.latestText
		session.latestText = ""
		session.latestTextMux.Unlock()
		event := BufferedEvent{
			Type:     "chat_ended",
			Data:     text,
			Metadata: metadata,
		}
		session.processBufferedEvent(event)
		_ = questionID
	}

	// 用户打断
	client.OnInterrupt = func() {
		session.sendToClient(map[string]interface{}{
			"type": "interrupt",
		})
	}

	// 错误处理
	client.OnError = func(err error) {
		session.sendToClient(map[string]interface{}{
			"type": "error",
			"payload": map[string]interface{}{
				"error": err.Error(),
			},
		})
	}

	// 连接状态变化
	client.OnConnectionStateChange = func(oldState, newState doubao.ConnectionState) {
		session.sendToClient(map[string]interface{}{
			"type": "control",
			"payload": map[string]interface{}{
				"event": "connection_state",
				"data": map[string]interface{}{
					"old_state": oldState.String(),
					"new_state": newState.String(),
				},
			},
		})
	}
}

// Cleanup 清理会话资源
func (s *ClientSession) Cleanup() {
	s.closeOnce.Do(func() {
		s.isActive = false
		s.logger.Info("正在清理会话资源...")

		// 关闭豆包客户端
		if s.doubaoClient != nil {
			if err := s.doubaoClient.Close(); err != nil {
				s.logger.Errorf("关闭豆包客户端失败: %v", err)
			}
		}

		// 关闭 WebSocket 连接
		if s.ws != nil {
			if err := s.ws.Close(); err != nil {
				s.logger.Errorf("关闭 WebSocket 失败: %v", err)
			}
		}

		s.logger.Info("会话资源已清理")
	})
}

// sendEvent sends an event to the client wrapped in v2 protocol envelope
// 🎯 与 JavaScript 对齐：使用 v2 协议 envelope 发送事件
func (s *ClientSession) sendEvent(eventType, event string, data map[string]interface{}, options map[string]interface{}) {
	envelope := s.buildEnvelope(eventType, event, data, options)
	s.sendToClient(envelope)
}

func (s *ClientSession) buildEnvelope(eventType, event string, data map[string]interface{}, options map[string]interface{}) map[string]interface{} {
	envelope := map[string]interface{}{
		"v":     2,
		"type":  eventType,
		"event": event,
		"ts":    time.Now().UnixMilli(),
		"data":  data,
	}

	// Override timestamp if provided
	if ts, ok := options["ts"].(int64); ok {
		envelope["ts"] = ts
	}

	// Optional fields
	if done, ok := options["done"].(bool); ok {
		envelope["done"] = done
	}
	if ref, ok := options["ref"].(map[string]interface{}); ok {
		envelope["ref"] = ref
	}
	if meta, ok := options["meta"].(map[string]interface{}); ok {
		envelope["meta"] = meta
	}

	return envelope
}

func (h *ClientSession) processBufferedEvent(event BufferedEvent) {
	switch event.Type {
	case "chat_response":
		// Extract text from event data
		text := ""
		if textVal, ok := event.Data.(string); ok {
			text = textVal
		}
		metadata := event.Metadata
		if metadata == nil {
			metadata = make(map[string]interface{})
		}

		h.sendEvent("assistant", "chat_text", map[string]interface{}{
			"text": text,
		}, map[string]interface{}{
			"ts":   getInt64OrDefault(metadata, "timestamp", time.Now().UnixMilli()),
			"done": false,
			"ref": map[string]interface{}{
				"questionId": metadata["questionId"],
				"replyId":    metadata["replyId"],
			},
			"meta": map[string]interface{}{
				"isPartial": true,
			},
		})

	case "chat_ended":
		metadata := event.Metadata
		if metadata == nil {
			metadata = make(map[string]interface{})
		}
		h.sendEvent("assistant", "chat_text", map[string]interface{}{
			"text": event.Data, //todo 这个要存一下
		}, map[string]interface{}{
			"ts":   getInt64OrDefault(metadata, "timestamp", time.Now().UnixMilli()),
			"done": true,
			"ref": map[string]interface{}{
				"questionId": metadata["questionId"],
				"replyId":    metadata["replyId"],
			},
			"meta": map[string]interface{}{
				"isPartial": false,
			},
		})

	case "output_transcript":
		// Extract transcript from event data
		transcript := ""
		if transcriptVal, ok := event.Data.(string); ok {
			transcript = transcriptVal
		}

		metadata := event.Metadata
		if metadata == nil {
			metadata = make(map[string]interface{})
		}

		isDone := false
		if isDoneVal, ok := metadata["isDone"].(bool); ok {
			isDone = isDoneVal
		}
		// 🎯 与 JavaScript 对齐：使用 sendEvent 发送 v2 协议 envelope
		h.sendEvent("assistant", "tts_text", map[string]interface{}{
			"text": transcript,
		}, map[string]interface{}{
			"ts":   getInt64OrDefault(metadata, "timestamp", time.Now().UnixMilli()),
			"done": isDone,
			"ref": map[string]interface{}{
				"questionId": metadata["questionId"],
				"replyId":    metadata["replyId"],
				"ttsTaskId":  metadata["ttsTaskId"],
			},
			"meta": map[string]interface{}{
				"ttsType": metadata["ttsType"],
			},
		})

	case "tts_start":
		// TTS 开始事件不需要发送到客户端
		break
	}
}

func getInt64OrDefault(metadata map[string]interface{}, key string, defaultVal int64) int64 {
	if val, ok := metadata[key].(int64); ok {
		return val
	}
	if val, ok := metadata[key].(int); ok {
		return int64(val)
	}
	if val, ok := metadata[key].(float64); ok {
		return int64(val)
	}
	return defaultVal
}

// sendToClient 发送消息到 WebSocket 客户端
func (s *ClientSession) sendToClient(message interface{}) {
	if s.ws == nil {
		s.logger.Warn("[sendToClient] WebSocket 连接为空，无法发送")
		return
	}

	// 检测是否为二进制数据（音频）
	if binaryData, ok := message.([]byte); ok {
		// 直接发送二进制数据
		s.writeToWs.Lock()
		if err := s.ws.WriteMessage(websocket.BinaryMessage, binaryData); err != nil {
			s.logger.Errorf("[sendToClient] 发送二进制数据失败: %v, size: %d bytes", err, len(binaryData))
		} else {
			s.logger.Debugf("[sendToClient] ✅ 成功发送二进制数据: %d bytes", len(binaryData))
		}
		s.writeToWs.Unlock()
		return
	}

	// JSON 序列化后发送文本消息
	data, err := json.Marshal(message)
	if err != nil {
		s.logger.Errorf("[sendToClient] 序列化消息失败: %v", err)
		return
	}
	s.writeToWs.Lock()
	if err := s.ws.WriteMessage(websocket.TextMessage, data); err != nil {
		s.logger.Errorf("[sendToClient] 发送消息到客户端失败: %v, message: %s", err, string(data))
	} else {
		s.logger.Debugf("[sendToClient] ✅ 成功发送: %s", string(data))
	}
	s.writeToWs.Unlock()
}
