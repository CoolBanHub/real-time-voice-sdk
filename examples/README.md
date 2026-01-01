# 示例代码

这个目录包含了豆包实时语音 SDK 的使用示例。

## 准备工作

在运行示例之前，需要设置环境变量：

```bash
export DOUBAO_APP_ID=your-app-id
export DOUBAO_ACCESS_TOKEN=your-access-token
```

## 示例列表

### 1. 基础示例 (basic)

演示基本的文本对话功能。

```bash
cd examples/basic
go run main.go
```

功能：
- 建立 WebSocket 连接
- 发送文本查询
- 接收 LLM 响应
- 接收 TTS 音频
- 处理各种事件回调

### 2. 音频流式传输 (audio_streaming)

演示如何流式发送音频文件并接收响应。

```bash
cd examples/audio_streaming
go run main.go <your-audio.pcm>
```

要求：
- 音频格式：16000Hz, 16bit, 单声道 PCM
- 输出：将接收到的音频保存为 `output.pcm`

如何生成测试音频：

```bash
# 使用 ffmpeg 从任何音频文件转换
ffmpeg -i input.wav -f s16le -ar 16000 -ac 1 test.pcm
```

功能：
- 模拟实时音频流
- 100ms 间隔发送音频块
- 接收 ASR 识别结果
- 保存 TTS 输出音频

### 3. 纯 TTS 合成 (tts_only)

演示如何使用 TTS 功能合成语音。

```bash
cd examples/tts_only
go run main.go
```

功能：
- 直接发送文本进行语音合成
- 不经过 LLM 处理
- 保存合成的音频到文件

输出音频播放：

```bash
# 使用 ffplay 播放
ffplay -f s16le -ar 24000 -ac 1 tts_output.pcm

# 或转换为 WAV
ffmpeg -f s16le -ar 24000 -ac 1 -i tts_output.pcm tts_output.wav
```

## 音频格式说明

### 输入音频（发送到服务器）
- 采样率：16000Hz
- 位深度：16bit
- 声道：单声道
- 格式：PCM S16LE
- 字节顺序：小端序

### 输出音频（从服务器接收）
- 采样率：24000Hz
- 位深度：16bit（pcm_s16le）或 32bit（pcm）
- 声道：单声道
- 格式：PCM S16LE 或 PCM F32LE

## 常见问题

### 1. 连接失败

检查：
- AppID 和 AccessToken 是否正确
- 网络连接是否正常
- 防火墙是否允许 WebSocket 连接

### 2. 没有收到音频

检查：
- `OnAudio` 回调是否正确设置
- 是否发送了触发 TTS 的请求（文本查询或 SpeakText）
- 查看日志中的错误信息

### 3. ASR 无法识别

检查：
- 输入音频格式是否正确（16000Hz/16bit/Mono）
- 音频内容是否包含清晰的语音
- 音频文件是否损坏

## 更多示例

查看主 README 获取更多 API 使用说明。