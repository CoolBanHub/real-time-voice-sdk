package doubao

import (
	"errors"
	"fmt"
)

// 常见错误定义
var (
	// ErrNotConnected 表示 WebSocket 未连接
	ErrNotConnected = errors.New("websocket not connected")

	// ErrConnectionClosed 表示连接已关闭
	ErrConnectionClosed = errors.New("connection closed")

	// ErrInvalidConfig 表示配置无效
	ErrInvalidConfig = errors.New("invalid configuration")

	// ErrMissingAppID 表示缺少 AppID
	ErrMissingAppID = errors.New("appid is required")

	// ErrMissingAccessToken 表示缺少 AccessToken
	ErrMissingAccessToken = errors.New("access token is required")

	// ErrSessionNotStarted 表示会话未启动
	ErrSessionNotStarted = errors.New("session not started")

	// ErrInvalidAudioFormat 表示音频格式无效
	ErrInvalidAudioFormat = errors.New("invalid audio format")

	// ErrUserQuerying 表示用户正在查询中，不能执行某些操作
	ErrUserQuerying = errors.New("operation not allowed while user is querying")

	// ErrTimeout 表示操作超时
	ErrTimeout = errors.New("operation timeout")
)

// ClientError 表示客户端错误
type ClientError struct {
	Op  string // 操作名称
	Err error  // 底层错误
}

func (e *ClientError) Error() string {
	if e.Op == "" {
		return e.Err.Error()
	}
	return e.Op + ": " + e.Err.Error()
}

func (e *ClientError) Unwrap() error {
	return e.Err
}

// ServerError 表示服务器返回的错误
type ServerError struct {
	Code    uint32 // 错误码
	Message string // 错误消息
	Event   int32  // 事件 ID（可选）
}

func (e *ServerError) Error() string {
	if e.Event != 0 {
		return fmt.Sprintf("server error (code=%d, event=%d): %s", e.Code, e.Event, e.Message)
	}
	return fmt.Sprintf("server error (code=%d): %s", e.Code, e.Message)
}

// ProtocolError 表示协议错误
type ProtocolError struct {
	Message string
	Data    []byte // 原始数据（用于调试）
}

func (e *ProtocolError) Error() string {
	return "protocol error: " + e.Message
}
