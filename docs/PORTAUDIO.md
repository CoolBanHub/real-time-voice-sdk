# PortAudio 支持

豆包实时语音 SDK 支持通过 PortAudio 进行音频捕获和播放，但由于 PortAudio 依赖 CGO，因此采用可选编译的方式实现。

## 为什么使用条件编译？

- **服务器端**：不需要 PortAudio，禁用 CGO 编译可以减少依赖、简化部署
- **桌面应用/示例**：需要音频输入输出，启用 PortAudio 获得完整功能

## 使用方法

### 1. 服务器端编译（不使用 PortAudio）

默认情况下，SDK 不包含 PortAudio 功能：

```bash
# 正常编译，不需要 CGO
go build

# 或者显式禁用 CGO
CGO_ENABLED=0 go build
```

此时即使配置了 `isUsePortAudio: true`，SDK 也会输出警告并跳过音频捕获/播放。

### 2. 启用 PortAudio（桌面应用/示例）

编译时添加 `-tags portaudio` 标签：

```bash
# 编译时启用 PortAudio
go build -tags portaudio

# 或在 examples 中运行
cd examples/basic
go run -tags portaudio main.go
```

### 3. 安装 PortAudio 依赖

使用 PortAudio 前需要安装系统依赖：

**macOS:**
```bash
brew install portaudio
```

**Ubuntu/Debian:**
```bash
sudo apt-get install portaudio19-dev
```

**Windows:**
```bash
# 使用 MSYS2
pacman -S mingw-w64-x86_64-portaudio
```

然后安装 Go 包：
```bash
go get github.com/gorilla/portaudio
```

### 4. 代码示例

```go
package main

import (
    "github.com/real-time-voice-sdk/doubao"
)

func main() {
    cfg := &doubao.RealtimeVoiceClientConfig{
        Appid:       "your-app-id",
        AccessToken: "your-token",
        // 启用 PortAudio
        isUsePortAudio: true,
    }

    client := doubao.NewRealtimeVoiceClient(cfg)

    // PortAudio 会自动：
    // 1. 从麦克风捕获音频并发送到服务器
    // 2. 接收服务器音频并播放到扬声器

    if err := client.Start(); err != nil {
        panic(err)
    }

    if err := client.RealTimeDialog(); err != nil {
        panic(err)
    }

    // 保持运行...
    select {}
}
```

## 实现原理

SDK 使用 Go 的 build tags 实现条件编译：

- `realtime_voice_audio_portaudio.go` - 包含 PortAudio 实现（`//go:build portaudio`）
- `realtime_voice_audio_stub.go` - 空实现（`//go:build !portaudio`）

编译时：
- 不加 `-tags portaudio`：使用 stub 实现，不依赖 CGO
- 加 `-tags portaudio`：使用完整实现，需要 CGO 和 PortAudio 库

## 功能对比

| 功能 | 不使用 PortAudio | 使用 PortAudio |
|------|-----------------|---------------|
| 文本对话 | ✅ | ✅ |
| TTS 语音合成 | ✅ | ✅ |
| 发送音频文件 | ✅ | ✅ |
| 麦克风实时录音 | ❌ | ✅ |
| 扬声器播放 | ❌ | ✅ |
| CGO 依赖 | ❌ | ✅ |
| 交叉编译 | 简单 | 复杂 |

## 常见问题

### Q: 编译时提示 "portaudio.h: No such file or directory"

A: 需要先安装 PortAudio 系统库（见上文安装说明）。

### Q: 服务器部署时是否需要 PortAudio？

A: 不需要。服务器端编译时不要加 `-tags portaudio` 标签即可。

### Q: 如何在不启用 PortAudio 的情况下发送音频？

A: 使用 `client.SendAudio(audioBytes)` 方法手动发送音频数据，参考 `examples/audio_streaming` 示例。

### Q: 启用 PortAudio 后性能如何？

A: PortAudio 是成熟的跨平台音频库，性能优秀。音频捕获/播放在独立 goroutine 中运行，不会阻塞主流程。

## 更多示例

查看示例代码：
- `examples/basic` - 基础文本对话
- `examples/audio_streaming` - 不使用 PortAudio 的音频流式传输
- `examples/tts_only` - 纯 TTS 合成

## 技术细节

### 音频参数

**输入（麦克风）:**
- 采样率：16000 Hz
- 位深度：16 bit
- 声道：单声道
- 格式：PCM S16LE

**输出（扬声器）:**
- 采样率：24000 Hz
- 位深度：16 bit 或 32 bit（根据配置）
- 声道：单声道
- 格式：PCM S16LE 或 PCM F32LE

### Build Tags 说明

SDK 文件结构：
```
doubao/
├── realtime_voice_audio_portaudio.go  (//go:build portaudio)
├── realtime_voice_audio_stub.go       (//go:build !portaudio)
└── ...
```

Go 编译器根据 build tag 自动选择要编译的文件。