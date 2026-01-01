class VoiceAssistant {
  constructor() {
    this.ws = null;
    this.isConnected = false;
    this.audioContext = null;
    this.playbackAudioContext = null;  // 录制用 AudioContext（播放由 PCMPlayer 负责）
    this.mediaStream = null;
    this.processor = null;
    this.audioQueue = [];
    this.isPlaying = false;
    this.isMicEnabled = true;
    
    // DOM元素
    this.voiceOrb = document.getElementById('voice-orb');
    this.micBtn = document.getElementById('mic-btn');
    this.stopBtn = document.getElementById('stop-btn');
    this.statusIndicator = document.getElementById('status-indicator');
    this.historyContainer = document.getElementById('history-container');
    this.historyMessages = document.getElementById('history-messages');
    this.commandContainer = document.getElementById('command-container');
    this.commandList = document.getElementById('command-list');
    
    // 流式输出的当前消息引用
    this.currentAssistantMessage = null;
    this.pendingAssistantText = '';  // 缓存待更新的文本
    this.updateTimer = null;         // 节流定时器
    
    // 流式识别的当前用户消息引用
    this.currentUserMessage = null;
    this.pendingUserText = '';
    this.pcmPlayer = null;          // PCM 播放器实例
    this.playbackVolumeLevel = this.loadVolumeLevel();

    this.lastCommandReplyText = '';
    this.lastCommandReplyAt = 0;

    // 🎯 录音发送分帧：推荐 20ms 一包（16000Hz => 320 samples => 640 bytes）
    // 说明：ScriptProcessor 回调粒度通常大于 20ms，这里做客户端侧切片。
    this.audioFrameSamples = 320;
    this.audioSendRemainder = new Int16Array(0);

    // 心跳：客户端 ping / 服务端 pong（应用层 v2 Envelope）
    this.heartbeatIntervalMs = 30000;
    this.heartbeatTimer = null;
    this.lastPongAt = 0;
    
    this.setupEventHandlers();
  }

  setupEventHandlers() {
    this.micBtn.onclick = () => this.toggleMicrophone();
    this.stopBtn.onclick = () => this.disconnect();
  }

  async connect() {
    try {
      this.updateStatus('正在连接...', 'active');
      
      // 连接 WebSocket
      const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
      this.ws = new WebSocket(`${protocol}//${location.host}/ws/voice-assistant`);
      //this.ws = new WebSocket(`ws://localhost:8081/ws/voice-assistant`);
      // 通过 WebSocket 协议标准，无法直接设置 header，只能通过 URL 参数传递 token
      this.ws.binaryType = 'arraybuffer';
      
      this.ws.onopen = async () => {
        console.log('WebSocket 已连接');
        this.isConnected = true;
        this.updateStatus('已连接', 'active');

        // 启动客户端心跳：定时发送 control.ping，服务端返回 control.pong
        this.startHeartbeat();
        
        // 初始化音频录制
        await this.initAudioRecording();
      };
      
      this.ws.onmessage = (event) => {
        this.handleMessage(event);
      };
      
      this.ws.onclose = () => {
        console.log('WebSocket 已断开');
        this.isConnected = false;
        this.stopHeartbeat();
        this.updateStatus('连接已断开', 'error');
      };
      
      this.ws.onerror = (error) => {
        console.error('WebSocket 错误:', error);
        this.stopHeartbeat();
        this.updateStatus('连接错误', 'error');
      };
      
    } catch (error) {
      console.error('连接失败:', error);
      this.updateStatus('连接失败', 'error');
    }
  }

  startHeartbeat() {
    this.stopHeartbeat();
    this.sendPing('onopen');
    this.heartbeatTimer = setInterval(() => {
      this.sendPing('interval');
    }, this.heartbeatIntervalMs);
  }

  stopHeartbeat() {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  sendPing(reason = 'ping') {
    try {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
      const envelope = { v: 2, type: 'control', event: 'ping', ts: Date.now(), done: true, data: {} };
      this.ws.send(JSON.stringify(envelope));
      // console.debug('[前端] 发送心跳 ping:', reason);
    } catch (e) {
      // 心跳失败不影响主流程
    }
  }

  handleMessage(event) {
    // 处理二进制数据（音频）
    if (event.data instanceof ArrayBuffer) {
      this.enqueueAudio(event.data);
      return;
    }
    
    // 处理文本消息
    if (typeof event.data !== 'string') return;

    try {
      const message = JSON.parse(event.data);

      // v2 协议：统一 Envelope
      if (message && typeof message === 'object' && message.v === 2 && message.type && message.event) {
        this.handleV2Envelope(message);
        return;
      }

      console.warn('[前端] 收到非 v2 Envelope 文本消息，已忽略:', message);
    } catch (error) {
      console.error('解析消息失败:', error);
    }
  }

  rememberCommandReply(text) {
    const normalized = String(text || '').trim();
    if (!normalized) return;
    this.lastCommandReplyText = normalized;
    this.lastCommandReplyAt = Date.now();
  }

  handleV2Envelope(envelope) {
    const { type, event, data, done, ref, meta, ts } = envelope || {};
    const payload = (data && typeof data === 'object') ? data : {};
    const refObj = (ref && typeof ref === 'object') ? ref : {};
    const metaObj = (meta && typeof meta === 'object') ? meta : {};
    const metadata = { ...refObj, ...metaObj, timestamp: ts };

    if (type === 'control') {
      if (event === 'pong') {
        this.lastPongAt = ts || Date.now();
        return;
      }
      if (event === 'interrupt') {
        this.handleInterrupt();
      }
      return;
    }

    if (type === 'asr' && event === 'transcript') {
      const text = payload.text || '';
      if (done) {
        this.handleUserMessageFinal(text, metadata);
      } else {
        this.handleUserMessagePartial(text, metadata);
      }
      return;
    }

    if (type === 'assistant' && event === 'chat_text') {
      const text = payload.text || '';
      if (done) {
        this.handleChatEnded(text, metadata);
      } else {
        this.handleChatResponse(text, metadata);
      }
      return;
    }

    if (type === 'assistant' && event === 'tts_text') {
      const text = payload.text || '';
      this.handleAssistantMessage(text, !!done, metaObj);
      return;
    }

    if (type === 'command') {
      if (event === 'clarify') {
        const replyText = payload.replyText || payload.question || '';
        this.rememberCommandReply(replyText);
        if (replyText) this.handleCommandResponse(replyText);
        return;
      }

      const replyText = payload.replyText || '';
      if (replyText) {
        this.rememberCommandReply(replyText);
        this.handleCommandResponse(replyText);
      }

      const moduleName = payload.module || event;
      if (moduleName !== 'music') {
        console.warn('[前端] 收到未知 command 模块，已忽略:', moduleName, payload);
        return;
      }

      this.handleMusicCommand({
        intent: payload.intent,
        parameters: payload.parameters || {},
        originalText: payload.originalText || '',
        timestamp: ts || Date.now(),
        processingTime: payload.processingTime
      });
      return;
    }
  }

  handleCommandResponse(text) {
    console.log('[前端] 收到指令响应文本:', text);
    // 显示指令响应文本（与语音一致）
    this.showLargeText(text);
    // 添加到消息历史
    this.addMessage('assistant', text);
  }

  /**
   * 处理流式用户输入（ASRResponse 事件）
   * @param {string} text - 流式识别文本
   * @param {object} metadata - 元数据
   */
  handleUserMessagePartial(text, metadata) {
    console.log('[流式识别]', text, metadata);
    
    this.historyContainer.style.display = 'flex';
    
    if (!this.currentUserMessage) {
      // 创建新的用户消息气泡
      const messageElement = document.createElement('div');
      messageElement.classList.add('message', 'user');
      
      const roleElement = document.createElement('div');
      roleElement.classList.add('message-role');
      roleElement.textContent = '你';
      
      const contentElement = document.createElement('div');
      contentElement.classList.add('message-content');
      contentElement.textContent = text;
      contentElement.style.opacity = '0.7'; // 流式识别时半透明
      
      messageElement.appendChild(roleElement);
      messageElement.appendChild(contentElement);
      this.historyMessages.appendChild(messageElement);
      
      this.currentUserMessage = contentElement;
    } else {
      // 更新现有气泡
      this.currentUserMessage.textContent = text;
    }
    
    this.historyMessages.scrollTop = this.historyMessages.scrollHeight;
  }

  /**
   * 处理最终用户输入（ASREnded 事件）
   * @param {string} text - 最终识别文本
   * @param {object} metadata - 元数据
   */
  handleUserMessageFinal(text, metadata) {
    console.log('[最终识别]', text, metadata);
    
    if (this.currentUserMessage) {
      // 更新现有气泡为最终结果
      this.currentUserMessage.textContent = text;
      this.currentUserMessage.style.opacity = '1'; // 恢复不透明
      this.currentUserMessage = null;
    } else {
      // 如果没有流式消息，直接添加
      this.addMessage('user', text);
    }
    
    this.historyMessages.scrollTop = this.historyMessages.scrollHeight;
  }

  /**
   * 处理 LLM 流式回复（ChatResponse 事件）
   * @param {string} text - LLM 文本增量
   * @param {object} metadata - 元数据
   */
  handleChatResponse(text, metadata) {
    console.log('[LLM流式]', text);
    
    this.historyContainer.style.display = 'flex';
    
    if (!this.currentAssistantMessage) {
      // 创建新的 AI 消息气泡
      const messageElement = document.createElement('div');
      messageElement.classList.add('message', 'assistant');
      
      const roleElement = document.createElement('div');
      roleElement.classList.add('message-role');
      roleElement.textContent = 'AI 助手 (LLM)';
      
      const contentElement = document.createElement('div');
      contentElement.classList.add('message-content');
      contentElement.textContent = text;
      contentElement.style.opacity = '0.8'; // 流式时半透明
      
      messageElement.appendChild(roleElement);
      messageElement.appendChild(contentElement);
      this.historyMessages.appendChild(messageElement);
      
      this.currentAssistantMessage = contentElement;
      this.pendingAssistantText = text;
    } else {
      // 更新现有气泡（累加文本）
      this.pendingAssistantText += text;
      this.currentAssistantMessage.textContent = this.pendingAssistantText;
    }
    
    this.historyMessages.scrollTop = this.historyMessages.scrollHeight;
  }

  /**
   * 处理 LLM 回复结束（ChatEnded 事件）
   * @param {string} text - LLM 完整文本（通常为空，因为 ChatEnded 只是标记结束）
   * @param {object} metadata - 元数据
   */
  handleChatEnded(text, metadata) {
    console.log('[LLM完成]', '回复结束');
    
    if (this.currentAssistantMessage) {
      if (text && typeof text === 'string') {
        this.pendingAssistantText = text;
        this.currentAssistantMessage.textContent = text;
      }

      // 固化流式文本为最终结果
      this.currentAssistantMessage.style.opacity = '1'; // 恢复不透明
      
      // 更新角色标签
      const messageElement = this.currentAssistantMessage.parentElement;
      const roleElement = messageElement.querySelector('.message-role');
      if (roleElement) {
        roleElement.textContent = 'AI 助手';
      }
      
      // 保存最终文本到历史（如果需要）
      const finalText = this.pendingAssistantText;
      console.log('[LLM完成] 最终文本:', finalText);
      
      this.currentAssistantMessage = null;
      this.pendingAssistantText = '';
    } else if (text && typeof text === 'string') {
      this.addMessage('assistant', text);
    }
    
    this.historyMessages.scrollTop = this.historyMessages.scrollHeight;
  }

  /**
   * 处理 TTS 文本输出（TTSSentenceEnd 事件）
   * @param {string} text - TTS 文本
   * @param {boolean} isDone - 是否已完成
   */
  handleAssistantMessage(text, isDone, meta = null) {
    if (!isDone) {
      return;
    }
    
    console.log('[TTS文本]', text);

    const normalized = String(text || '').trim();
    const sinceLastCommand = Date.now() - (this.lastCommandReplyAt || 0);
    if (normalized && normalized === this.lastCommandReplyText && sinceLastCommand >= 0 && sinceLastCommand < 3000) {
      return;
    }
    
    // TTS 文本通常是最终确认，直接显示
    // 注意：如果已经有 LLM 文本，这里可能重复，需要去重
    if (!this.currentAssistantMessage) {
      this.addMessage('assistant', text);
    }
  }

  handleMusicCommand(command) {
    console.log('[前端] 收到音乐指令:', command);
    console.log('[前端] 指令详情:', JSON.stringify(command, null, 2));
    
    // 显示在自定义指令区域
    this.addCommandToDisplay(command, 'music');
    
    // 这里可以调用实际的音乐播放API
    // 例如: window.location.href = `musicapp://play?song=${command.parameters.song}`;
  }

  addCommandToDisplay(command, type = 'music') {
    if (type !== 'music') {
      console.warn('[前端] 收到未支持的指令类型，已忽略:', type, command);
      return;
    }

    console.log('[前端] 添加指令到显示区域, 类型:', type);
    console.log('[前端] commandContainer:', this.commandContainer);
    console.log('[前端] commandList:', this.commandList);
    
    // 显示命令容器
    this.commandContainer.style.display = 'flex';
    
    // 创建命令项
    const commandItem = document.createElement('div');
    commandItem.className = 'command-item';
    
    let intentText, paramsHTML = '';

    // 🎵 音乐指令
    const intentMap = {
      'play': '▶️ 播放',
      'pause': '⏸️ 暂停',
      'next': '⏭️ 下一首',
      'previous': '⏮️ 上一首',
      'resume': '▶️ 继续',
      'stop': '⏹️ 停止',
      'search': '🔍 搜索'
    };

    intentText = intentMap[command.intent] || command.intent;

    if (command.parameters) {
      const params = [];
      if (command.parameters.artist) params.push(`歌手: ${command.parameters.artist}`);
      if (command.parameters.song) params.push(`歌曲: ${command.parameters.song}`);
      if (command.parameters.query) params.push(`搜索: ${command.parameters.query}`);

      if (params.length > 0) {
        paramsHTML = `<div class="command-params">${params.map(p => `<span class="command-param">${p}</span>`).join('')}</div>`;
      }
    }
    
    const time = new Date(command.timestamp || Date.now()).toLocaleTimeString();
    
    commandItem.innerHTML = `
      <div class="command-intent">${intentText}</div>
      <div class="command-text">"${command.originalText || ''}"</div>
      ${paramsHTML}
      <div class="command-time">⏱️ ${time} | 耗时: ${command.processingTime || 'N/A'}ms</div>
    `;
    
    this.commandList.insertBefore(commandItem, this.commandList.firstChild);
    
    // 限制显示数量
    while (this.commandList.children.length > 10) {
      this.commandList.removeChild(this.commandList.lastChild);
    }
  }

  handleInterrupt() {
    console.log('检测到打断');
    if (this.pcmPlayer) {
      this.pcmPlayer.destroy();
      this.pcmPlayer = null;
    }
    this.audioQueue = [];
    this.isPlaying = false;
    this.voiceOrb.classList.remove('speaking');
    this.voiceOrb.classList.add('listening');
    
    // 清空流式输出引用和定时器
    if (this.updateTimer) {
      clearTimeout(this.updateTimer);
      this.updateTimer = null;
    }
    this.currentAssistantMessage = null;
    this.pendingAssistantText = '';
  }

  async initAudioRecording() {
    try {
      this.mediaStream = await navigator.mediaDevices.getUserMedia({ audio: true });
      
      // 🔥 重要：豆包要求输入音频为 PCM 16000Hz
      this.audioContext = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: 16000 });
      
      // 播放由 PCMPlayer 负责，不在此创建播放用 AudioContext
      this.playbackAudioContext = null;
      
      const source = this.audioContext.createMediaStreamSource(this.mediaStream);
      // 🎯 降低回调粒度，减少端到端延迟（仍会在回调内切片到 20ms）
      this.processor = this.audioContext.createScriptProcessor(1024, 1, 1);
      
      source.connect(this.processor);
      this.processor.connect(this.audioContext.destination);
      
      this.processor.onaudioprocess = (e) => {
        if (!this.isMicEnabled || !this.isConnected) return;
        
        const input = e.inputBuffer.getChannelData(0);
        const int16 = new Int16Array(input.length);
        for (let i = 0; i < input.length; i++) {
          const s = Math.max(-1, Math.min(1, input[i]));
          int16[i] = s < 0 ? s * 0x8000 : s * 0x7FFF;
        }

        // 🎯 20ms 分包发送（强烈推荐），客户端无需额外发送静音片段
        this.sendAudioFrames(int16);
      };
      
      this.voiceOrb.classList.add('listening');
      this.updateStatus('正在聆听...', 'active');
      
      return true;
    } catch (error) {
      console.error('初始化音频录制失败:', error);
      this.updateStatus('无法访问麦克风', 'error');
      return false;
    }
  }

  /**
   * 🎯 将 Int16 PCM 切分为 20ms 帧发送
   * 16000Hz * 0.02s = 320 samples；每个 sample 2 字节 => 640 bytes
   */
  sendAudioFrames(int16) {
    if (!this.isConnected || this.ws.readyState !== WebSocket.OPEN) return;

    // 合并上一次残留
    const prev = this.audioSendRemainder || new Int16Array(0);
    const combined = new Int16Array(prev.length + int16.length);
    combined.set(prev, 0);
    combined.set(int16, prev.length);

    const frameSamples = this.audioFrameSamples || 320;
    let offset = 0;
    while (offset + frameSamples <= combined.length) {
      const frame = combined.subarray(offset, offset + frameSamples);
      const frameBuf = frame.buffer.slice(frame.byteOffset, frame.byteOffset + frame.byteLength);
      this.sendAudio(frameBuf);
      offset += frameSamples;
    }

    // 保存残留
    this.audioSendRemainder = combined.slice(offset);
  }

  sendAudio(audioData) {
    if (!this.isConnected || this.ws.readyState !== WebSocket.OPEN) return;
    
    const bufferToSend = new Uint8Array(1 + audioData.byteLength);
    bufferToSend[0] = 0; // 0 表示音频
    bufferToSend.set(new Uint8Array(audioData), 1);
    
    this.ws.send(bufferToSend.buffer);
  }

  enqueueAudio(buffer) {
    this.ensurePCMPlayer();
    if (!this.pcmPlayer) return;

    if (!this.isPlaying) {
      this.isPlaying = true;
      this.voiceOrb.classList.remove('listening');
      this.voiceOrb.classList.add('speaking');
      this.updateStatus('AI 正在回复...', 'active');
    }

    // PCMPlayer 支持直接喂入 16bit 小端 PCM 的 ArrayBuffer
    this.pcmPlayer.feed(buffer);
  }

  ensurePCMPlayer() {
    if (this.pcmPlayer) return;
    if (typeof PCMPlayer === 'undefined') {
      console.error('PCMPlayer 库未加载');
      return;
    }
    this.pcmPlayer = new PCMPlayer({
      encoding: '16bitInt',
      channels: 1,
      sampleRate: 24000,
      flushTime: 10,
      volume: this.getPlaybackVolume()
    });
  }

  loadVolumeLevel() {
    const raw = localStorage.getItem('playbackVolumeLevel');
    const parsed = Number(raw);
    if (Number.isFinite(parsed)) {
      return this.clamp(Math.round(parsed), 0, 10);
    }
    return 9;
  }

  getPlaybackVolume() {
    const level = this.clamp(Number(this.playbackVolumeLevel), 0, 10);
    return level / 10;
  }

  setPlaybackVolumeLevel(level) {
    const nextLevel = this.clamp(Math.round(Number(level)), 0, 10);
    this.playbackVolumeLevel = nextLevel;
    localStorage.setItem('playbackVolumeLevel', String(nextLevel));

    const volume = this.getPlaybackVolume();
    if (this.pcmPlayer) {
      if (typeof this.pcmPlayer.volume === 'function') {
        this.pcmPlayer.volume(volume);
      } else if (typeof this.pcmPlayer.setVolume === 'function') {
        this.pcmPlayer.setVolume(volume);
      } else if (typeof this.pcmPlayer.setVolume === 'number') {
        this.pcmPlayer.setVolume = volume;
      } else if (typeof this.pcmPlayer.volume === 'number') {
        this.pcmPlayer.volume = volume;
      }
    }

    return { level: nextLevel, volume };
  }

  adjustPlaybackVolume(deltaLevels) {
    const current = this.clamp(this.playbackVolumeLevel, 0, 10);
    const next = this.clamp(current + Number(deltaLevels || 0), 0, 10);
    return this.setPlaybackVolumeLevel(next);
  }

  clamp(value, min, max) {
    const num = Number(value);
    if (!Number.isFinite(num)) return min;
    return Math.min(Math.max(num, min), max);
  }

  toggleMicrophone() {
    this.isMicEnabled = !this.isMicEnabled;
    
    if (this.isMicEnabled) {
      this.micBtn.classList.add('active');
      this.voiceOrb.classList.remove('muted');
      this.voiceOrb.classList.add('listening');
      this.updateStatus('正在聆听...', 'active');
    } else {
      this.micBtn.classList.remove('active');
      this.voiceOrb.classList.remove('listening', 'speaking');
      this.voiceOrb.classList.add('muted');
      this.updateStatus('麦克风已关闭');
    }
  }

  disconnect() {
    this.stopHeartbeat();
    if (this.processor) {
      this.processor.disconnect();
    }
    if (this.mediaStream) {
      this.mediaStream.getTracks().forEach(t => t.stop());
    }
    if (this.audioContext) {
      this.audioContext.close();
    }
    if (this.playbackAudioContext) {
      this.pcmPlayer = null;
      this.playbackAudioContext.close();
    }
    if (this.ws) {
      this.ws.close();
    }
    
    this.updateStatus('已断开连接');
    
    setTimeout(() => {
      location.reload();
    }, 1000);
  }

  addMessage(role, text, options = {}) {
    this.historyContainer.style.display = 'flex';
    
    const messageElement = document.createElement('div');
    messageElement.classList.add('message', role);
    
    // 如果是中性词，添加特殊样式
    if (options.isNeutral) {
      messageElement.classList.add('neutral');
      messageElement.style.opacity = '0.7';
      messageElement.style.fontStyle = 'italic';
    }
    
    const roleElement = document.createElement('div');
    roleElement.classList.add('message-role');
    roleElement.textContent = role === 'user' ? '你' : 'AI 助手';
    
    const contentElement = document.createElement('div');
    contentElement.classList.add('message-content');
    contentElement.textContent = text;
    
    messageElement.appendChild(roleElement);
    messageElement.appendChild(contentElement);
    this.historyMessages.appendChild(messageElement);
    
    // 滚动到底部
    this.historyMessages.scrollTop = this.historyMessages.scrollHeight;
  }

  showLargeText(text) {
    const overlay = document.createElement('div');
    overlay.style.cssText = `
      position: fixed;
      top: 50%;
      left: 50%;
      transform: translate(-50%, -50%);
      font-size: 48px;
      font-weight: bold;
      color: #b794f4;
      text-shadow: 0 0 20px rgba(183, 148, 244, 0.5);
      z-index: 1000;
      animation: fadeInOut 1.5s ease-out;
    `;
    overlay.textContent = text;
    document.body.appendChild(overlay);
    
    setTimeout(() => overlay.remove(), 1500);
  }

  updateStatus(message, type = 'normal') {
    this.statusIndicator.textContent = message;
    this.statusIndicator.className = 'status-indicator';
    if (type === 'active') {
      this.statusIndicator.classList.add('active');
    } else if (type === 'error') {
      this.statusIndicator.classList.add('error');
    }
  }
}

// 添加 fadeInOut 动画
const style = document.createElement('style');
style.textContent = `
  @keyframes fadeInOut {
    0% { opacity: 0; transform: translate(-50%, -50%) scale(0.8); }
    50% { opacity: 1; transform: translate(-50%, -50%) scale(1); }
    100% { opacity: 0; transform: translate(-50%, -50%) scale(1.2); }
  }
`;
document.head.appendChild(style);

// 自动初始化
const assistant = new VoiceAssistant();
window.addEventListener('load', () => {
  assistant.connect();
});
