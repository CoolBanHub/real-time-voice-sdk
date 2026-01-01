# 豆包实时语音对话 Web 示例

这是一个基于 WebSocket 的实时语音对话 Web 应用示例，展示了如何通过浏览器与豆包实时语音 API 进行交互。

## 功能特性

- 🎙️ **浏览器麦克风录音** - 实时捕获用户语音
- 🔊 **实时语音播放** - 自动播放 AI 语音回复
- 💬 **文本对话** - 支持文本输入与 AI 对话
- 📡 **WebSocket 通信** - 低延迟的实时双向通信
- 🎯 **事件驱动** - 完整的事件回调系统

## 架构说明

```
浏览器 (index.html + js)
    ↕ WebSocket
后端服务器 (main.go)
    ↕ WebSocket
豆包实时语音 API
```

## 快速开始

### 1. 设置环境变量

```bash
export DOUBAO_APP_ID=your-app-id
export DOUBAO_ACCESS_TOKEN=your-access-token
```

### 2. 启动服务器

```bash
cd examples/web_demo
go run main.go
```

### 3. 打开浏览器

访问 http://localhost:8080

## WebSocket 协议说明

### 客户端 → 服务器

所有消息格式：
```json
{
  "type": "消息类型",
  "payload": "消息内容"
}
```

#### 1. 发送音频数据

```json
{
  "type": "audio",
  "payload": [字节数组]
}
```

音频格式要求：
- 采样率：16000 Hz
- 位深度：16 bit
- 声道：单声道 (Mono)
- 编码：PCM S16LE

#### 2. 发送文本消息

```json
{
  "type": "text",
  "payload": "你好，请介绍一下你自己"
}
```

#### 3. 控制消息

```json
{
  "type": "control",
  "payload": {
    "action": "ping"
  }
}
```

支持的控制动作：
- `ping` - 心跳检测

### 服务器 → 客户端

#### 1. 音频数据

```json
{
  "type": "audio",
  "payload": [字节数组]
}
```

音频格式：
- 采样率：24000 Hz
- 位深度：16 bit 或 32 bit
- 声道：单声道 (Mono)
- 编码：PCM S16LE 或 PCM F32LE

#### 2. 转录文本

```json
{
  "type": "transcript",
  "payload": {
    "direction": "input",  // 或 "output"
    "text": "识别的文本内容",
    "is_final": true
  }
}
```

- `direction: "input"` - ASR 识别的用户语音
- `direction: "output"` - TTS 输出的文本
- `is_final: true` - 最终结果
- `is_final: false` - 中间结果

#### 3. 聊天响应

```json
{
  "type": "chat",
  "payload": {
    "content": "LLM 回复的文本内容"
  }
}
```

#### 4. 控制消息

```json
{
  "type": "control",
  "payload": {
    "event": "事件名称",
    "data": {}
  }
}
```

支持的事件：
- `connected` - 连接成功
- `asr_ended` - ASR 识别结束
- `tts_start` - TTS 开始
- `tts_ended` - TTS 结束
- `chat_ended` - LLM 响应结束
- `interrupt` - 用户打断
- `connection_state` - 连接状态变化
- `pong` - ping 响应

#### 5. 错误消息

```json
{
  "type": "error",
  "payload": {
    "error": "错误描述"
  }
}
```

## 前端集成示例

### JavaScript 连接 WebSocket

```javascript
// 创建 WebSocket 连接
const ws = new WebSocket('ws://localhost:8080/ws/voice-assistant');

// 连接成功
ws.onopen = () => {
  console.log('已连接到服务器');
};

// 接收消息
ws.onmessage = (event) => {
  const message = JSON.parse(event.data);

  switch (message.type) {
    case 'audio':
      // 播放音频
      playAudio(message.payload);
      break;
    case 'transcript':
      // 显示转录文本
      displayTranscript(message.payload);
      break;
    case 'chat':
      // 显示聊天响应
      displayChat(message.payload);
      break;
    case 'control':
      // 处理控制消息
      handleControl(message.payload);
      break;
    case 'error':
      // 显示错误
      console.error(message.payload.error);
      break;
  }
};

// 发送音频数据
function sendAudio(audioData) {
  const message = {
    type: 'audio',
    payload: Array.from(new Uint8Array(audioData))
  };
  ws.send(JSON.stringify(message));
}

// 发送文本消息
function sendText(text) {
  const message = {
    type: 'text',
    payload: text
  };
  ws.send(JSON.stringify(message));
}
```

### 录制和发送音频

```javascript
// 获取麦克风权限
navigator.mediaDevices.getUserMedia({ audio: true })
  .then(stream => {
    const mediaRecorder = new MediaRecorder(stream);
    const audioContext = new AudioContext({ sampleRate: 16000 });
    const source = audioContext.createMediaStreamSource(stream);

    // 处理音频数据并发送
    const processor = audioContext.createScriptProcessor(4096, 1, 1);
    processor.onaudioprocess = (e) => {
      const inputData = e.inputBuffer.getChannelData(0);
      const pcmData = convertToPCM16(inputData);
      sendAudio(pcmData);
    };

    source.connect(processor);
    processor.connect(audioContext.destination);
  });

// 转换为 PCM 16 位
function convertToPCM16(float32Array) {
  const buffer = new ArrayBuffer(float32Array.length * 2);
  const view = new DataView(buffer);
  for (let i = 0; i < float32Array.length; i++) {
    const s = Math.max(-1, Math.min(1, float32Array[i]));
    view.setInt16(i * 2, s < 0 ? s * 0x8000 : s * 0x7FFF, true);
  }
  return buffer;
}
```

## 项目结构

```
web_demo/
├── main.go           # WebSocket 服务器
├── README.md         # 本文档
└── public/           # 前端静态文件
    ├── index.html    # 主页面
    └── js/
        └── app.js    # 前端 JavaScript
```

## 注意事项

1. **音频格式**: 确保发送的音频符合格式要求（16kHz, 16bit, Mono）
2. **CORS**: 生产环境需要正确配置 CORS 策略
3. **错误处理**: 实现完善的错误处理和重连机制
4. **安全性**: 不要在前端暴露 AccessToken
5. **性能**: 合理控制音频发送频率，避免过载

## 故障排查

### 连接失败
- 检查 `DOUBAO_APP_ID` 和 `DOUBAO_ACCESS_TOKEN` 环境变量是否正确
- 确认服务器正在运行 (http://localhost:8080)
- 检查防火墙设置

### 音频无法播放
- 确认浏览器支持 Web Audio API
- 检查麦克风权限是否已授予
- 查看浏览器控制台错误信息

### 延迟过高
- 检查网络连接质量
- 减小音频缓冲区大小
- 优化音频编码和解码性能

## 扩展功能

可以基于此示例实现更多功能：

- 🎨 自定义 UI 界面
- 📊 显示音频波形
- 💾 保存对话历史
- 🔊 音量控制
- 🎯 自定义音色选择
- 📝 导出对话记录

## 许可证

本示例代码采用 MIT License 开源协议。