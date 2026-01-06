package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/CoolBanHub/real-time-voice-sdk/doubao"
	"github.com/CoolBanHub/real-time-voice-sdk/pkg/log"
	"github.com/gordonklaus/portaudio"
)

// 使用 PortAudio 前需要安装系统依赖：
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
	helper := log.NewHelper(log.With(logger, "module", "portaudio_realtime"))
	fmt.Println("==============================================")
	fmt.Println("  English Pronunciation Practice (PortAudio)")
	fmt.Println("==============================================")
	fmt.Println()

	// 配置客户端 - 英语陪练专用配置
	cfg := &doubao.RealtimeVoiceClientConfig{
		Appid:          appID,
		AccessToken:    accessToken,
		Speaker:        "zh_female_vv_jupiter_bigtts", // 使用英文音色
		Log:            helper,
		EnableEventLog: true,
		// ASR 配置
		ASR: &doubao.ASRPayload{
			Format:  "pcm",
			Rate:    16000, // 客户端录音采样率为16kHz
			Bits:    16,
			Channel: 1,
			Extra: map[string]interface{}{
				"enable_itn_convert": true,
				//"end_smooth_window_ms": 1500,
			},
		},
		// TTS 配置 - 使用英文音色
		TTS: &doubao.TTSPayload{
			Speaker: "zh_female_vv_jupiter_bigtts", // 英文女声，自然流畅
			AudioConfig: doubao.AudioConfig{
				Channel:    1,
				Format:     "pcm_s16le",
				SampleRate: 24000,
			},
		},
		// Dialog 配置 - 中英双语发音纠正教练
		Dialog: &doubao.DialogPayload{
			BotName:       "Emma",
			SystemRole:    "你是 Emma，一位专业的英语发音教练。你的学生是中国人，英语基础可能不太好，所以你需要用中文和英文混合教学。你的主要任务是评估和改善学生的英语发音。仔细聆听学生的发音，评估其清晰度、语调、重音模式和单个音素。当发现发音问题时，用中文清楚地解释哪些音或单词发音不标准以及原因，然后用英文示范正确发音，并让学生跟读练习。",
			SpeakingStyle: "专业但鼓励的态度，中英文混合使用。用中文解释发音问题和给予反馈，用英文示范正确发音。学生说完后，先用中文肯定他们的努力，然后具体指出发音问题。如果发音好，用中文表扬并鼓励继续。如果有问题，用中文用简单的语言解释（例如：'th 这个音需要把舌头放在牙齿之间'），然后用英文清晰、缓慢地示范正确发音，并要求学生重复。使用类似'我来示范一下：[英文单词/短语]。现在你跟着说。'或'注意我重读第一个音节：[英文单词]。你能重复一遍吗？'这样的表达。一次专注于一到两个发音要点，避免让学习者感到压力过大。记住：讲解用中文，示范用英文。",
			Extra: map[string]interface{}{
				"strict_audit":   false,
				"input_mod":      "audio",
				"model":          "O",
				"audit_response": "抱歉，我没听清楚。你能再说一遍吗？",
				// 网络搜索功能默认禁用
				"enable_volc_websearch": false,
			},
		},
		Reconnect: &doubao.ReconnectConfig{
			Enabled:           true,
			MaxAttempts:       5,
			InitialDelay:      2 * time.Second,
			MaxDelay:          30 * time.Second,
			BackoffMultiplier: 2.0,
		},
	}

	// 创建客户端
	client := doubao.NewRealtimeVoiceClient(cfg)

	p := &Portaudio{
		ctx:        context.Background(),
		log:        helper,
		client:     client,
		pcmFormat:  "pcm_s16le", //默认这个 需要跟上面客户端配置的一样
		buffer:     make([]float32, 0, sampleRate*bufferSeconds),
		s16Buffer:  make([]int16, 0, sampleRate*bufferSeconds),
		bufferLock: sync.Mutex{},
	}
	terminate := p.Init()
	defer func() {
		if err := terminate(); err != nil {
			fmt.Printf("❌ terminate 报错: %v\n", err)
			os.Exit(1)
		}

	}()

	// 设置事件回调
	setupCallbacks(client, p)

	// 启动 WebSocket 连接
	fmt.Println("正在连接豆包服务器...")
	if err := client.Start(); err != nil {
		fmt.Printf("❌ 连接失败: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()
	fmt.Println("✅ 连接成功！")

	// 启动实时对话会话
	if err := client.RealTimeDialog(); err != nil {
		fmt.Printf("❌ 启动对话失败: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("✅ 实时对话会话已启动！")
	fmt.Println()
	//开始麦克风监听
	go p.sendAudioByPortAudio()
	go p.startPlayer()
	// 说明信息
	printUsageInfo()
	// 等待用户中断

	// Emma 主动打招呼
	fmt.Println("\n🎬 Emma 正在向你问好...")
	time.Sleep(500 * time.Millisecond) // 等待音频流稳定
	if err := client.SayHello(&doubao.SayHelloPayload{Content: "你好！我是 Emma，你的英语发音教练。我会用中文和你交流，帮助你改善英语发音。我会仔细听你的发音，然后用中文告诉你哪里需要改进。当我示范正确发音的时候，会用英文清晰地读给你听，然后你跟着我练习就好。不用紧张，我们一起慢慢进步！现在，请用英语说点什么，我会给你反馈。"}); err != nil {
		fmt.Printf("⚠️  发送问候失败: %v\n", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\n正在关闭连接...")
}

func setupCallbacks(client *doubao.RealtimeVoiceClient, portaudio *Portaudio) {
	// 接收服务器音频数据（PortAudio 会自动播放）
	client.OnAudio = func(audioData []byte) {
		// PortAudio 启用时，音频会自动通过扬声器播放
		// 这里可以记录音频接收情况
		// fmt.Printf("  [音频] 接收 %d bytes\n", len(audioData))
		portaudio.handleIncomingAudio(audioData)
	}

	// ASR 最终识别结果（用户说的话）
	client.OnInputTranscript = func(text string, isFinal bool) {
		if isFinal {
			fmt.Printf("\n💬 [You] %s\n", text)
		}
	}

	// ASR 临时识别结果（实时反馈）
	client.OnInputTranscriptPartial = func(text string, isFinal bool) {
		// 可以在这里显示实时识别结果
		// fmt.Printf("  [识别中] %s\r", text)
	}

	// ASR 结束（用户停止说话）
	client.OnASREnded = func() {
		fmt.Printf("  [ASR] 识别结束:%s\n", client.LatestARSContent)

	}

	// TTS 开始（AI 开始说话）
	client.OnTTSStart = func(metadata *doubao.MsgMetadata) {
		fmt.Printf("\n🔊 [Emma] Speaking...\n")
		portaudio.clearBuffer()
	}

	// TTS 输出文本（AI 说的话）
	client.OnOutputTranscript = func(text string, metadata *doubao.MsgMetadata) {
		fmt.Printf("   %s\n", text)
	}

	// LLM 流式文本响应
	client.OnChatResponse = func(content string) {
		// 可以显示 LLM 的文本响应
		// fmt.Print(content)
	}

	// LLM 响应结束
	client.OnChatEnded = func(msg *doubao.Message) {
		fmt.Printf("  [响应] 完成: %s\n", client.LatestAIResponse)
	}

	// 用户打断
	client.OnInterrupt = func() {
		fmt.Println("\n  ⚠️  [打断] 检测到用户打断")
		portaudio.clearBuffer()
	}

	// 错误处理
	client.OnError = func(err error) {
		fmt.Printf("\n  ❌ [错误] %v\n", err)
	}

	// 连接状态变化
	client.OnConnectionStateChange = func(oldState, newState doubao.ConnectionState) {
		fmt.Printf("\n  🔗 [连接] %s -> %s\n", oldState, newState)
	}

	// 重连尝试
	client.OnReconnecting = func(attempt int, delay time.Duration) {
		fmt.Printf("  🔄 [重连] 第 %d 次尝试 (延迟 %v)\n", attempt, delay)
	}

	// 重连失败
	client.OnReconnectFailed = func(attempts int, err error) {
		fmt.Printf("  ❌ [重连失败] %d 次尝试后失败: %v\n", attempts, err)
	}
}

const (
	sampleRate       = 24000
	channels         = 1
	framesPerBuffer  = 1024 // 增大帧缓冲，减少CPU切换，提高流畅度
	bufferSeconds    = 100  // 最多缓冲100秒数据
	minBufferSamples = 4800 // 预缓冲阈值：200ms音频数据 (24000 * 0.2)
	preBufferSamples = 9600 // 初始预缓冲：400ms音频数据 (24000 * 0.4)
	DefaultPCM       = "pcm"
	PcmS16LE         = "pcm_s16le"
)

type Portaudio struct {
	ctx            context.Context
	log            *log.Helper
	client         *doubao.RealtimeVoiceClient
	pcmFormat      string
	buffer         []float32
	s16Buffer      []int16
	bufferLock     sync.Mutex
	isBuffering    bool      // 是否正在预缓冲
	bufferingSince time.Time // 开始缓冲的时间
}

func (this *Portaudio) Init() func() error {
	if err := portaudio.Initialize(); err != nil {
		this.log.Errorf("portaudio initialize error: %v", err)
		return func() error {
			return err
		}
	}
	return portaudio.Terminate

}
func (this *Portaudio) sendAudioByPortAudio() {
	defer func() {
		if err := recover(); err != nil {
			this.log.Errorf("panic: %v", err)
		}
	}()

	defaultInputDevice, err := portaudio.DefaultInputDevice()
	if err != nil {
		this.log.Errorf("Failed to get default input device: %v", err)
		return
	}
	this.log.Infof("Using default input device: %s", defaultInputDevice.Name)

	streamParameters := portaudio.StreamParameters{ //todo 配置参数
		Input: portaudio.StreamDeviceParameters{
			Device:   defaultInputDevice,
			Channels: 1,
			Latency:  defaultInputDevice.DefaultLowInputLatency,
		},
		SampleRate:      16000,
		FramesPerBuffer: 160,
	}

	// sayhello后模拟chatTextQuery
	stream, err := portaudio.OpenStream(streamParameters, func(in []int16) {
		// 1. 将 int16 音频数据转换为 []byte (PCM S16LE)
		audioBytes := make([]byte, len(in)*2)
		for i, sample := range in {
			audioBytes[i*2] = byte(sample & 0xff)
			audioBytes[i*2+1] = byte((sample >> 8) & 0xff)
		}
		if this.client != nil {
			err := this.client.SendAudio(audioBytes)
			if err != nil {
				this.log.Error(err)
				return
			}
		}
	})

	if err != nil {
		this.log.Errorf("Failed to open microphone input stream: %v", err)
		return
	}
	defer stream.Close()

	if err := stream.Start(); err != nil {
		this.log.Errorf("Failed to start microphone input stream: %v", err)
		return
	}
	this.log.Info("Microphone input stream started. please speak...")

	// 保持 goroutine 运行以允许回调处理音频
	select {
	case <-this.ctx.Done():
		this.log.Info("Stopping microphone input stream due to context cancellation...")
		if err := stream.Stop(); err != nil {
			this.log.Errorf("Failed to stop microphone input stream: %v", err)
		}
		if this.client != nil {
			err = this.client.Close()
			if err != nil {
				this.log.Errorf("Failed to finish session: %v", err)
			}
		}
	}
	this.log.Info("Microphone input stream stopped.")
	return
}

func (this *Portaudio) startPlayer() {
	outputDevice, err := portaudio.DefaultOutputDevice()
	if err != nil {
		this.log.Errorf("Failed to get default output device: %v", err)
		return
	}

	outputParameters := portaudio.StreamParameters{
		Output: portaudio.StreamDeviceParameters{
			Device:   outputDevice,
			Channels: channels,
			Latency:  10 * time.Millisecond,
		},
		SampleRate:      float64(sampleRate),
		FramesPerBuffer: framesPerBuffer,
	}

	var outputStream *portaudio.Stream
	switch this.pcmFormat {
	case DefaultPCM:
		outputStream, err = portaudio.OpenStream(outputParameters, func(out []float32) {
			this.bufferLock.Lock()
			defer this.bufferLock.Unlock()
			if len(this.buffer) < len(out) {
				copy(out, this.buffer)
				for i := len(this.buffer); i < len(out); i++ {
					out[i] = 0
				}
				this.buffer = this.buffer[:0]
			} else {
				copy(out, this.buffer)
				this.buffer = this.buffer[len(out):]
			}
		})
	case PcmS16LE:
		outputStream, err = portaudio.OpenStream(outputParameters, func(out []int16) {
			this.bufferLock.Lock()
			defer this.bufferLock.Unlock()

			// 如果正在预缓冲或缓冲不足，输出静音
			if this.isBuffering || len(this.s16Buffer) < minBufferSamples {
				for i := range out {
					out[i] = 0
				}
				return
			}

			// 缓冲充足，正常播放
			if len(this.s16Buffer) < len(out) {
				copy(out, this.s16Buffer)
				for i := len(this.s16Buffer); i < len(out); i++ {
					out[i] = 0
				}
				this.s16Buffer = this.s16Buffer[:0]
			} else {
				copy(out, this.s16Buffer)
				this.s16Buffer = this.s16Buffer[len(out):]
			}
		})
	}

	if outputStream == nil {
		this.log.Errorf("Failed to open PortAudio output stream: %v", err)
		return
	}
	if err != nil {
		this.log.Errorf("Failed to open PortAudio output stream: %v", err)
		return
	}
	defer outputStream.Close()

	if err := outputStream.Start(); err != nil {
		this.log.Errorf("Failed to start PortAudio output stream: %v", err)
		return
	}
	this.log.Info("PortAudio output stream started for playback.")

	// 保持流运行，防止函数返回导致流停止
	<-this.ctx.Done()
}

func (this *Portaudio) handleIncomingAudio(data []byte) {
	switch this.pcmFormat {
	case PcmS16LE:
		sampleCount := len(data) / 2
		samples := make([]int16, sampleCount)
		for i := 0; i < sampleCount; i++ {
			bits := binary.LittleEndian.Uint16(data[i*2 : (i+1)*2])
			samples[i] = int16(bits)
		}
		// 将音频加载到缓冲区
		this.bufferLock.Lock()
		defer this.bufferLock.Unlock()

		// 如果是第一次收到音频，开始预缓冲
		if len(this.s16Buffer) == 0 && !this.isBuffering {
			this.isBuffering = true
			this.bufferingSince = time.Now()
			this.log.Info("Starting audio pre-buffering for smooth playback...")
		}

		this.s16Buffer = append(this.s16Buffer, samples...)

		// 预缓冲完成检查
		if this.isBuffering && len(this.s16Buffer) >= preBufferSamples {
			this.isBuffering = false
			duration := time.Since(this.bufferingSince)
			this.log.Infof("Pre-buffering complete: %d samples buffered in %v", len(this.s16Buffer), duration)
		}

		if len(this.s16Buffer) > sampleRate*bufferSeconds {
			this.s16Buffer = this.s16Buffer[len(this.s16Buffer)-(sampleRate*bufferSeconds):]
		}
	case DefaultPCM:
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
	}

}

func (this *Portaudio) clearBuffer() {
	this.bufferLock.Lock()
	this.buffer = this.buffer[:0]
	this.s16Buffer = this.s16Buffer[:0]
	this.isBuffering = false // 重置缓冲状态
	this.bufferLock.Unlock()
	this.log.Info("Audio buffer cleared, ready for next playback")
}

func printUsageInfo() {
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("  💡 How to Use - Pronunciation Practice")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println()
	fmt.Println("  1. 🎤 Speak English into your microphone")
	fmt.Println("     - Emma will analyze your pronunciation")
	fmt.Println("     - She'll evaluate clarity, stress, and sounds")
	fmt.Println()
	fmt.Println("  2. � Receive pronunciation feedback")
	fmt.Println("     - Emma will point out any issues")
	fmt.Println("     - She'll explain what needs improvement")
	fmt.Println()
	fmt.Println("  3. 🔊 Listen and repeat")
	fmt.Println("     - Emma demonstrates correct pronunciation")
	fmt.Println("     - Practice by repeating after her")
	fmt.Println()
	fmt.Println("  4. 🎯 Focus on improvement")
	fmt.Println("     - Work on specific sounds or words")
	fmt.Println("     - Emma addresses 1-2 points at a time")
	fmt.Println()
	fmt.Println("  5. 🛑 Exit Program")
	fmt.Println("     - Press Ctrl+C to quit")
	fmt.Println()
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println()
	fmt.Println("🎙️  Ready for pronunciation practice. Speak clearly!")
	fmt.Println()
}
