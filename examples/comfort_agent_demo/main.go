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
	fmt.Println("  豆包实时语音对话示例 (PortAudio)")
	fmt.Println("==============================================")
	fmt.Println()

	// 配置客户端
	cfg := &doubao.RealtimeVoiceClientConfig{
		Appid:          appID,
		AccessToken:    accessToken,
		Speaker:        "zh_female_vv_jupiter_bigtts", // 音色配置
		Log:            helper,
		EnableEventLog: true,
		// 启用 PortAudio 实时音频捕获和播放
		// 可选：自定义重连配置
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
	time.Sleep(time.Second)
	// 要合成的文本列表
	go func() { //收到用户说话结束后才会开始触发！
		textsToSpeak := []string{
			"你好，我是小金，一个中文语音助手。",
			"今天天气真不错，适合出去走走。",
			"人工智能正在改变我们的生活。",
			"祝你有美好的一天！",
		}

		// 依次合成每段文本
		for i, text := range textsToSpeak {
			fmt.Printf("\n[文本 %d] %s\n", i+1, text)
			//client.SayHello(&doubao.SayHelloPayload{Content: text})
			client.SpeakText(text)
			// 等待 TTS 完成
			// 在实际应用中，应该使用事件回调来判断何时完成
			time.Sleep(10 * time.Second)
		}
	}()

	//// 依次合成每段文本
	//for i, text := range textsToSpeak {
	//	fmt.Printf("\n[文本 %d] %s\n", i+1, text)
	//
	//	if err := client.ChatTextQuery(&doubao.ChatTextQueryPayload{Content: text}); err != nil {
	//		fmt.Printf("  [错误] 发送失败: %v\n", err)
	//		continue
	//	}
	//
	//	// 等待 TTS 完成
	//	// 在实际应用中，应该使用事件回调来判断何时完成
	//	time.Sleep(5 * time.Second)
	//}
	// 等待用户中断
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
			fmt.Printf("\n💬 [你说] %s\n", text)
		}
	}

	// ASR 临时识别结果（实时反馈）
	client.OnInputTranscriptPartial = func(text string, isFinal bool) {
		// 可以在这里显示实时识别结果
		// fmt.Printf("  [识别中] %s\r", text)
	}

	// ASR 结束（用户停止说话）
	client.OnASREnded = func() {
		fmt.Println("  [ASR] 识别结束")
	}

	// TTS 开始（AI 开始说话）
	client.OnTTSStart = func(metadata *doubao.MsgMetadata) {
		fmt.Printf("\n🔊 [AI 说话] 开始播放...\n")
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
		fmt.Println("  [响应] 完成")
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
	sampleRate      = 24000
	channels        = 1
	framesPerBuffer = 512
	bufferSeconds   = 100 // 最多缓冲100秒数据
	DefaultPCM      = "pcm"
	PcmS16LE        = "pcm_s16le"
)

type Portaudio struct {
	ctx       context.Context
	log       *log.Helper
	client    *doubao.RealtimeVoiceClient
	pcmFormat string
	buffer    []float32
	s16Buffer []int16
	//使用播报语音的
	bufferLock sync.Mutex
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
		this.s16Buffer = append(this.s16Buffer, samples...)
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
	this.bufferLock.Unlock()
}

func printUsageInfo() {
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("  💡 使用说明")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println()
	fmt.Println("  1. 🎤 对着麦克风说话")
	fmt.Println("     - 系统会实时识别你的语音")
	fmt.Println("     - 识别结果会显示在屏幕上")
	fmt.Println()
	fmt.Println("  2. 🔊 AI 会通过扬声器回复")
	fmt.Println("     - 自动播放 AI 的语音回复")
	fmt.Println("     - 回复文本会同步显示")
	fmt.Println()
	fmt.Println("  3. ⚡ 实时交互")
	fmt.Println("     - 支持多轮对话")
	fmt.Println("     - 可以打断 AI 的回复")
	fmt.Println()
	fmt.Println("  4. 🛑 退出程序")
	fmt.Println("     - 按 Ctrl+C 退出")
	fmt.Println()
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println()
	fmt.Println("🎙️  麦克风已就绪，请开始说话...")
	fmt.Println()
}
