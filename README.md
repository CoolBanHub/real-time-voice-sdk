# 豆包实时语音 SDK (Doubao Realtime Voice SDK)

<div align="center">

一个功能完整、易于使用的豆包（Doubao）实时语音对话 Go SDK

[![Go Version](https://img.shields.io/badge/Go-%3E%3D%201.18-blue.svg)](https://golang.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

</div>

---

## 📖 简介

豆包实时语音 SDK 是一个功能强大的 Go 语言客户端库，用于与豆包实时语音对话 API 进行交互。SDK 提供了完整的实时语音对话能力，集成了自动语音识别（ASR）、文本转语音（TTS）和大语言模型（LLM）对话功能，让您轻松构建智能语音应用。

### ✨ 核心特性

- 🎙️ **实时语音对话** - 基于 WebSocket 的双向实时通信
- 🗣️ **自动语音识别（ASR）** - 支持实时和最终识别结果
- 🔊 **文本转语音（TTS）** - 高质量语音合成，支持多种音色
- 🤖 **大语言模型对话** - 流式 LLM 响应，支持上下文对话
- 📡 **音频流式传输** - 高效的音频数据收发
- 🗜️ **GZIP 压缩** - 可选的音频数据压缩，节省带宽
- 🎯 **事件驱动架构** - 丰富的回调系统，灵活处理各种事件
- 🎨 **自定义配置** - 支持自定义音色、对话风格等参数

---

## 📦 安装

```bash
go get github.com/CoolBanHub/real-time-voice-sdk
```

### 前置要求

- **Go 版本**: 1.18 或更高
- **API 凭证**: 豆包 API 的 AppID 和 AccessToken（请访问[豆包开放平台](https://www.volcengine.com/)获取）

---

## 🚀 快速开始

### 基础实时对话示例

以下示例展示了如何创建一个基本的实时语音对话应用：

```go
package main

import (
    "fmt"
    "os"
    "os/signal"
    "syscall"

    "github.com/go-kratos/kratos/v2/log"
    "github.com/CoolBanHub/real-time-voice-sdk/doubao"
)

func main() {
    // 1. 初始化日志
    logger := log.NewStdLogger(os.Stdout)
    logger = log.With(logger, "caller", log.Caller(4), "ts", log.DefaultTimestamp)

    // 2. 配置客户端
    cfg := &doubao.RealtimeVoiceClientConfig{
        Appid:       "your-app-id",        // 替换为你的 App ID
        AccessToken: "your-access-token",  // 替换为你的 Access Token
        Speaker:     "zh_female_vv_jupiter_bigtts", // 音色选择（快捷配置）
        Log:         log.NewHelper(log.With(logger, "module", "doubao")),
		
		// 可选：自定义 ASR 配置（如需精确控制识别结果）
		//ASR: &doubao.ASRPayload{}
        // 可选：自定义 TTS 配置（如需精确控制音频格式）
        // TTS: &doubao.TTSPayload{
        //     Speaker: "zh_female_vv_jupiter_bigtts",
        //     AudioConfig: doubao.AudioConfig{
        //         Channel:    1,
        //         Format:     "pcm_s16le",  // 或 "pcm" (float32)
        //         SampleRate: 24000,
        //     },
        // },
		// 可选：自定义 Dialog 配置（如需精确控制对话）
		//Dialog &DialogPayload{} // Dialog 配置，nil 则使用默认配置
    }

    // 3. 创建客户端实例
    client := doubao.NewRealtimeVoiceClient(cfg)

    // 4. 设置事件回调
    setupCallbacks(client)

    // 5. 启动 WebSocket 连接
    if err := client.Start(); err != nil {
        log.Fatalf("Failed to start client: %v", err)
    }
    defer client.Close()

    // 6. 开始实时对话会话
    if err := client.RealTimeDialog(); err != nil {
        log.Fatalf("Failed to start dialog: %v", err)
    }

    // 7. 发送文本查询
    if err := client.SendText("你好，请介绍一下你自己"); err != nil {
        log.Errorf("Failed to send text: %v", err)
    }

    // 8. 等待系统信号（优雅退出）
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    <-sigChan

    fmt.Println("\nShutting down gracefully...")
}

func setupCallbacks(client *doubao.RealtimeVoiceClient) {
    // 接收 TTS 音频数据
    client.OnAudio = func(audioData []byte) {
        fmt.Printf("🔊 Received audio: %d bytes\n", len(audioData))
        // TODO: 播放音频或保存到文件
    }

    // ASR 最终识别结果
    client.OnInputTranscript = func(text string, isFinal bool) {
        if isFinal {
            fmt.Printf("🎙️ ASR [Final]: %s\n", text)
        }
    }

    // ASR 临时识别结果（实时反馈）
    client.OnInputTranscriptPartial = func(text string, isFinal bool) {
        fmt.Printf("🎙️ ASR [Partial]: %s\n", text)
    }

    // LLM 流式响应
    client.OnChatResponse = func(content string) {
        fmt.Printf("🤖 LLM: %s", content)
    }

    // LLM 响应结束
    client.OnChatEnded = func() {
        fmt.Println("\n✅ LLM Response Completed")
    }

    // TTS 输出文本
    client.OnOutputTranscript = func(text string, metadata *doubao.MsgMetadata) {
        fmt.Printf("📝 TTS Text: %s (type: %s)\n", text, metadata.TTSType)
    }

    // 用户打断事件
    client.OnInterrupt = func() {
        fmt.Println("⏸️ User interrupted")
    }

    // 错误处理
    client.OnError = func(err error) {
        fmt.Printf("❌ Error: %v\n", err)
    }
}
```

### 发送音频数据

```go
// 发送 PCM 音频数据
// 格式要求：16000Hz, 16bit, 单声道（Mono）
audioData := []byte{...} // 你的音频数据

if err := client.SendAudio(audioData); err != nil {
    log.Errorf("Failed to send audio: %v", err)
}
```

### 使用 GZIP 压缩传输音频

如果需要节省网络带宽，可以使用 GZIP 压缩：

```go
// 使用 GZIP 压缩发送音频（推荐）
audioData := []byte{...}

if err := client.SendAudioWithCompression(audioData); err != nil {
    log.Errorf("Failed to send compressed audio: %v", err)
}
```

### 使用纯 TTS 语音合成

直接朗读文本，不经过 LLM 处理：

```go
// 让 AI 直接朗读指定文本
if err := client.SpeakText("你好，欢迎使用豆包语音助手"); err != nil {
    log.Errorf("Failed to speak text: %v", err)
}
```

---

## 📚 API 文档

### 配置参数

#### `RealtimeVoiceClientConfig`

客户端配置结构体，用于初始化客户端：

| 字段 | 类型 | 必填 | 说明 | 默认值 |
|------|------|------|------|--------|
| `Appid` | `string` | ✅ | 豆包应用 ID | - |
| `AccessToken` | `string` | ✅ | API 访问令牌 | - |
| `SessionId` | `string` | ❌ | 自定义会话 ID | 自动生成 UUID |
| `Log` | `*log.Helper` | ❌ | Kratos 日志实例 | `nil` |
| `Speaker` | `string` | ❌ | 音色名称（快捷配置） | `""` |
| `IsUsePortAudio` | `bool` | ❌ | 是否启用 PortAudio 播放 | `false` |
| `EnableEventLog` | `bool` | ❌ | 是否启用事件处理详细日志 | `false` |
| `ASR` | `*ASRPayload` | ❌ | ASR 语音识别配置 | `nil`（使用默认） |
| `TTS` | `*TTSPayload` | ❌ | TTS 语音合成配置 | `nil`（使用默认） |
| `Dialog` | `*DialogPayload` | ❌ | 对话配置（LLM 相关） | `nil`（使用默认） |
| `Reconnect` | `*ReconnectConfig` | ❌ | 自动重连配置 | `nil`（使用默认） |

### 核心方法

#### 客户端生命周期

##### `NewRealtimeVoiceClient(cfg *RealtimeVoiceClientConfig) *RealtimeVoiceClient`

创建新的实时语音客户端实例。

**参数:**
- `cfg`: 客户端配置对象

**返回:**
- `*RealtimeVoiceClient`: 客户端实例

---

##### `Start() error`

启动 WebSocket 连接并建立与服务器的通信。

**返回:**
- `error`: 启动失败时返回错误

---

##### `RealTimeDialog() error`

启动实时对话会话，发送初始化配置。

**返回:**
- `error`: 启动对话失败时返回错误

**注意**: 必须在 `Start()` 之后调用。

---

##### `Close() error`

关闭连接并清理资源。

**返回:**
- `error`: 关闭失败时返回错误

**最佳实践**: 使用 `defer client.Close()` 确保资源释放。

---

#### 音频传输

##### `SendAudio(content []byte) error`

发送未压缩的 PCM 音频数据。

**参数:**
- `content`: PCM 音频字节数组（16000Hz, 16bit, Mono）

**返回:**
- `error`: 发送失败时返回错误

---

##### `SendAudioWithCompression(content []byte) error`

发送 GZIP 压缩的音频数据（推荐用于节省带宽）。

**参数:**
- `content`: PCM 音频字节数组

**返回:**
- `error`: 发送失败时返回错误

**优势**: 可节省约 50-70% 的网络带宽。

---

#### 文本交互

##### `SendText(text string) error`

发送文本查询，触发 LLM 推理和 TTS 合成。

**参数:**
- `text`: 要查询的文本内容

**返回:**
- `error`: 发送失败时返回错误

**流程**: 文本 → LLM 处理 → TTS 合成 → 音频输出

---

##### `SpeakText(text string) error`

直接朗读文本，不经过 LLM 处理（快速 TTS）。

**参数:**
- `text`: 要朗读的文本

**返回:**
- `error`: 发送失败时返回错误

**用途**: 适用于系统提示、通知等场景。

---

#### 高级 API

##### `ChatTTSText(req *ChatTTSTextPayload) error`

发送自定义 TTS 文本请求（低级 API）。

**参数:**
- `req`: TTS 文本请求对象

**返回:**
- `error`: 发送失败时返回错误

---

##### `ChatTextQuery(req *ChatTextQueryPayload) error`

发送自定义文本查询请求（低级 API）。

**参数:**
- `req`: 文本查询请求对象

**返回:**
- `error`: 发送失败时返回错误

---

### 事件回调系统

SDK 提供了丰富的事件回调，用于处理各种异步事件：

#### 基础回调

| 回调函数 | 函数签名 | 触发时机 |
|---------|---------|---------|
| `OnAudio` | `func([]byte)` | 接收到 TTS 音频数据 |
| `OnError` | `func(error)` | 发生错误 |

#### 连接事件回调

| 回调函数 | 函数签名 | 触发时机 |
|---------|---------|---------|
| `OnConnectionStarted` | `func(*Message)` | 成功建立连接 |
| `OnConnectionFailed` | `func(*Message)` | 建立连接失败，返回错误信息 |
| `OnConnectionFinished` | `func(*Message)` | 连接结束 |

#### 会话事件回调

| 回调函数 | 函数签名 | 触发时机 |
|---------|---------|---------|
| `OnSessionStarted` | `func(*Message)` | 成功启动会话，返回的 dialog_id 用于接续最近的对话内容，增加模型智能度 |
| `OnSessionFinished` | `func(*Message)` | 会话已结束 |
| `OnSessionFailed` | `func(*Message)` | 会话失败，返回错误信息 |

#### ASR 语音识别回调

| 回调函数 | 函数签名 | 触发时机 |
|---------|---------|---------|
| `OnInterrupt` | `func()` | 模型识别出音频流中的首字返回的事件，用于打断客户端的播报 |
| `OnInputTranscript` | `func(text string, isFinal bool)` | 模型识别出用户说话的文本内容（最终结果） |
| `OnInputTranscriptPartial` | `func(text string, isFinal bool)` | 模型识别出用户说话的文本内容（中间结果） |
| `OnASREnded` | `func()` | 模型认为用户说话结束的事件 |

#### TTS 语音合成回调

| 回调函数 | 函数签名 | 触发时机 |
|---------|---------|---------|
| `OnTTSStart` | `func(*MsgMetadata)` | 合成音频的起始事件 |
| `OnOutputTranscript` | `func(text string, *MsgMetadata)` | 合成音频的分句结束事件 |
| `OnTTSEnded` | `func(*Message)` | 模型一轮音频合成结束事件 |

#### Chat 对话回调

| 回调函数 | 函数签名 | 触发时机 |
|---------|---------|---------|
| `OnChatResponse` | `func(content string)` | 模型回复的文本内容，包含 question_id 和 reply_id |
| `OnChatEnded` | `func(*Message)` | 模型回复文本结束事件，包含 question_id 和 reply_id |
| `OnChatTextQueryConfirmed` | `func(*Message)` | ChatTextQuery 请求对应的 ack，返回 question_id |

#### Conversation 上下文管理回调

| 回调函数 | 函数签名 | 触发时机 |
|---------|---------|---------|
| `OnConversationCreated` | `func(*Message)` | 增加上下文请求对应的 ack，返回创建成功的上下文 item 数组 |
| `OnConversationUpdated` | `func(*Message)` | 更新上下文请求对应的 ack，更新成功返回空，失败返回错误信息 |
| `OnConversationRetrieved` | `func(*Message)` | 查询上下文请求对应的 ack，返回上下文 item 数组 |
| `OnConversationDeleted` | `func(*Message)` | 删除上下文请求对应的 ack，返回被删除的上下文 item 数组 |

#### 用量统计回调

| 回调函数 | 函数签名 | 触发时机 |
|---------|---------|---------|
| `OnUsageInfo` | `func(*Message)` | 每一轮交互对应的用量信息，包含输入输出的文本和音频 tokens |

#### 连接状态管理回调

| 回调函数 | 函数签名 | 触发时机 |
|---------|---------|---------|
| `OnConnectionStateChange` | `func(oldState, newState ConnectionState)` | 连接状态变化 |
| `OnReconnecting` | `func(attempt int, delay time.Duration)` | 准备进行重连尝试 |
| `OnReconnectFailed` | `func(attempts int, err error)` | 所有重连尝试均失败 |

#### 回调使用示例

```go
client.OnInputTranscript = func(text string, isFinal bool) {
    if isFinal {
        // 用户说话结束，最终识别结果
        fmt.Printf("User said: %s\n", text)
        // 可以在这里保存对话记录
    }
}

client.OnChatResponse = func(content string) {
    // 实时接收 LLM 响应
    fmt.Print(content)
}

client.OnAudio = func(audioData []byte) {
    // 播放接收到的音频
    playAudio(audioData)
}

// 连接状态管理回调
client.OnConnectionStateChange = func(oldState, newState doubao.ConnectionState) {
    fmt.Printf("连接状态变化: %s -> %s\n", oldState, newState)
}

client.OnReconnecting = func(attempt int, delay time.Duration) {
    fmt.Printf("正在尝试第 %d 次重连，延迟 %v\n", attempt, delay)
}

client.OnReconnectFailed = func(attempts int, err error) {
    fmt.Printf("重连失败：所有 %d 次尝试均失败: %v\n", attempts, err)
}
```

---

## 🎵 音频格式规范

### 输入音频（发送到服务器）

| 参数 | 值 |
|------|-----|
| 采样率 | **16000 Hz** |
| 位深度 | **16 bit** |
| 声道 | **单声道（Mono）** |
| 编码格式 | **PCM S16LE**（有符号 16 位小端序） |

### 输出音频（从服务器接收）

| 参数 | 值 |
|------|-----|
| 采样率 | **24000 Hz** |
| 位深度 | **16 bit** 或 **32 bit**（取决于 `PcmFormat`） |
| 声道 | **单声道（Mono）** |
| 编码格式 | **PCM S16LE** 或 **PCM F32LE** |

> **注意**: 输入和输出的采样率不同（16kHz vs 24kHz），请在实际应用中注意音频格式转换。

---

## 📂 示例代码

项目提供了多个示例，帮助您快速上手：

| 目录 | 说明 |
|------|------|
| [examples/basic/](examples/basic/) | 基本文本对话示例 |
| [examples/audio_streaming/](examples/audio_streaming/) | 音频流式传输示例 |
| [examples/tts_only/](examples/tts_only/) | 纯 TTS 语音合成示例 |

运行示例：

```bash
# 基本对话示例
cd examples/basic
go run main.go

# 音频流式传输
cd examples/audio_streaming
go run main.go
```

---

## 🎨 支持的音色

SDK 支持多种高质量音色，满足不同场景需求：

### 常用预设音色

| 音色 ID | 描述 | 适用场景 |
|--------|------|---------|
| `zh_female_vv_jupiter_bigtts` | 温柔女声（默认） | 客服、助手 |
| `zh_male_chunhoubaobeishu_moon_bigtts` | 沉稳男声 | 新闻播报、专业解说 |
| `ICL_zh_female_aojiaonvyou_tob` | 官方克隆音色 | 品牌定制 |

### 自定义克隆音色

支持使用自定义克隆音色（需要 `character_manifest`）：

```go
cfg := &doubao.RealtimeVoiceClientConfig{
    Speaker: "S_XXXXXX", // 你的自定义音色 ID
    // ... 其他配置
}
```

> 更多音色列表请参考 [豆包官方文档 - 音色列表](https://www.volcengine.com/docs/6561/1221103)

---

## ⚙️ 高级配置

### 自定义 TTS 配置

如需精确控制 TTS 音频格式：

```go
cfg := &doubao.RealtimeVoiceClientConfig{
    Appid:       appID,
    AccessToken: accessToken,
    TTS: &doubao.TTSPayload{
        Speaker: "zh_female_vv_jupiter_bigtts",
        AudioConfig: doubao.AudioConfig{
            Channel:    1,           // 单声道
            Format:     "pcm_s16le", // PCM 16位小端序，或 "pcm" (float32)
            SampleRate: 24000,       // 24kHz 采样率
        },
    },
}
```

### 自定义重连配置

配置自动重连策略：

```go
cfg := &doubao.RealtimeVoiceClientConfig{
    Appid:       appID,
    AccessToken: accessToken,
    Reconnect: &doubao.ReconnectConfig{
        Enabled:           true,              // 启用自动重连
        MaxAttempts:       10,                // 最多重连 10 次
        InitialDelay:      1 * time.Second,   // 初始延迟 1 秒
        MaxDelay:          60 * time.Second,  // 最大延迟 60 秒
        BackoffMultiplier: 2.0,               // 指数退避倍数
    },
}
```

### 启用网络搜索

SDK 默认启用网络搜索功能（`enable_volc_websearch: true`），可以在对话中获取实时信息：

```go
// 示例：询问实时信息
client.SendText("今天北京的天气怎么样？")
```

### 自定义 Dialog 对话配置

```go
cfg := &doubao.RealtimeVoiceClientConfig{
    Appid:       appID,
    AccessToken: accessToken,
    Dialog: &doubao.DialogPayload{
        Model: "your-model-id",
        // 其他对话配置...
    },
}
```

### 事件日志控制

SDK 支持通过 `EnableEventLog` 配置项控制事件处理的详细日志输出。默认情况下，为了保持简洁，事件日志处于关闭状态，只显示错误和警告信息。

```go
cfg := &doubao.RealtimeVoiceClientConfig{
    Appid:          appID,
    AccessToken:    accessToken,
    EnableEventLog: true,  // 启用详细事件日志，用于调试
    Log:            log.NewHelper(logger),
}
```

**使用场景：**
- **开发调试**: 设置为 `true` 可查看所有事件的详细处理流程
- **生产环境**: 保持默认 `false`，减少日志输出，提升性能
- **问题排查**: 遇到问题时临时启用，快速定位事件处理中的问题

**日志输出示例（启用后）：**
```
Receive event 1 (connection.started): {...}
Connection started
Receive event 10 (session.started): {...}
Session started
Receive audio message (len=4096)
TTS Ended - Audio synthesis completed
```

> **注意**: 错误和警告日志始终会输出，不受此配置影响。

---

## 🛡️ 错误处理

### 错误回调处理

所有错误都会通过 `OnError` 回调传递，建议在生产环境中妥善处理：

```go
client.OnError = func(err error) {
    log.Errorf("Client error: %v", err)
    
    // 根据错误类型决定处理策略
    if isNetworkError(err) {
        // 网络错误：尝试重连
        reconnect()
    } else if isAuthError(err) {
        // 认证错误：刷新 Token
        refreshToken()
    }
}
```

### 常见错误类型

| 错误类型 | 原因 | 解决方案 |
|---------|------|---------|
| 连接失败 | 网络问题或服务不可用 | 检查网络连接，稍后重试 |
| 认证失败 | AccessToken 无效或过期 | 刷新或重新获取 Token |
| 音频格式错误 | 音频参数不符合要求 | 检查采样率、位深度等参数 |
| 超时错误 | 长时间未收到响应 | 检查 `RecvTimeout` 配置 |

---

## ⚠️ 重要注意事项

### 1️⃣ 连接管理
- **必须调用 `Close()`**: 确保在程序退出时释放资源
- **优雅关闭**: 使用 `defer client.Close()` 或捕获系统信号

### 2️⃣ 音频格式
- **严格遵守规范**: 输入音频必须是 16000Hz/16bit/Mono PCM S16LE
- **格式转换**: 如果源音频格式不匹配，需要预先转换

### 3️⃣ 并发安全
- **异步回调**: 所有回调都在独立的 goroutine 中执行
- **线程安全**: 在回调中访问共享资源时需要加锁

### 4️⃣ 安全性
- **保护凭证**: 切勿将 `AccessToken` 硬编码或提交到版本控制
- **使用环境变量**: 推荐使用环境变量或配置文件管理凭证

```go
cfg := &doubao.RealtimeVoiceClientConfig{
    Appid:       os.Getenv("DOUBAO_APP_ID"),
    AccessToken: os.Getenv("DOUBAO_ACCESS_TOKEN"),
    // ... 其他配置
}
```

### 5️⃣ 性能优化
- **使用压缩**: 在带宽受限的环境下，使用 `SendAudioWithCompression()`
- **合理设置超时**: 根据实际场景调整 `RecvTimeout`

---

## 📦 依赖包

| 依赖 | 版本 | 用途 |
|------|------|------|
| [`github.com/gorilla/websocket`](https://github.com/gorilla/websocket) | latest | WebSocket 客户端 |
| [`github.com/google/uuid`](https://github.com/google/uuid) | latest | UUID 生成 |
| [`github.com/go-kratos/kratos/v2`](https://github.com/go-kratos/kratos) | v2.x | 日志框架 |
| [`github.com/golang/glog`](https://github.com/golang/glog) | latest | 辅助日志 |

---

## 📄 许可证

本项目采用 **MIT License** 开源协议。详见 [LICENSE](LICENSE) 文件。

---

## 🤝 贡献指南

我们欢迎并感谢所有形式的贡献！

### 如何贡献

1. **Fork** 本仓库
2. 创建你的特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交你的更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 提交 **Pull Request**

### 报告问题

如果您发现 Bug 或有功能建议，请通过 [GitHub Issues](../../issues) 提交。

---

## 📞 联系方式

- **问题反馈**: [GitHub Issues](../../issues)
- **官方文档**: [豆包开放平台](https://www.volcengine.com/docs/6561/1221103)

---

## 📝 更新日志

查看 [CHANGELOG.md](CHANGELOG.md) 获取详细的版本更新历史。

---

<div align="center">

**Made with ❤️ by the Doubao Community**

</div>