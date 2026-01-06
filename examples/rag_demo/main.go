package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
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
	fmt.Println(" RAG Demo")
	fmt.Println("==============================================")
	fmt.Println()

	// 配置客户端
	cfg := &doubao.RealtimeVoiceClientConfig{
		Appid:          appID,
		AccessToken:    accessToken,
		Speaker:        "zh_female_vv_jupiter_bigtts", // 音色配置
		Log:            helper,
		EnableEventLog: true,
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
		TTS: &doubao.TTSPayload{
			Speaker: "zh_female_vv_jupiter_bigtts",
			AudioConfig: doubao.AudioConfig{
				Channel:    1,
				Format:     "pcm_s16le", // 与JS版本一致
				SampleRate: 24000,
			},
		},
		Dialog: &doubao.DialogPayload{
			BotName:       "豆包",
			SystemRole:    "你是豆包，一个由字节跳动开发的智能助手",
			SpeakingStyle: "友好、专业、有帮助。回答简洁明了，善于解答各类问题。",
			Extra: map[string]interface{}{
				"strict_audit":   false,
				"input_mod":      "audio",
				"model":          "O",
				"audit_response": "抱歉，我暂时无法回答这个问题。让我们聊点别的吧。",
				// 网络搜索功能默认禁用，用户可以根据需要启用
				"enable_volc_websearch": false,
			},
		},
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
			client.SpeakText("好的 正常查询，稍等。。。")
			ragList := make([]doubao.RAGObject, 0)

			// 1. 高级群发推送概述
			ragList = append(ragList, doubao.RAGObject{
				Title:   "高级群发推送功能介绍",
				Content: "高级群发推送是一种模拟企业微信手动消息进行推送的功能，默认消息间隔为一秒。它可以批量群发客户或客户群消息。但如需大规模群发客户消息，更建议使用极速群发。高级群发支持企微推送客户、企微推送客户群、个微推送客户、个微推送客户群以及角色方案推送等多种场景。",
			})

			// 2. 企微推送客户
			ragList = append(ragList, doubao.RAGObject{
				Title:   "企微推送客户操作指南",
				Content: "创建推送客户计划的步骤：1. 创建推送客户计划；2. 编辑计划内容，并设置发送时间；3. 推送成功后，可在推送记录中查看详情。注意：高级群发推送客户较慢，建议使用极速群发。",
			})

			// 3. 企微推送客户群
			ragList = append(ragList, doubao.RAGObject{
				Title:   "企微推送客户群操作指南",
				Content: "推送客户群的步骤：1. 创建一个高级群发计划；2. 配置推送内容，可设置推送消息的延迟时间及计划推送时间。客户群推送方式包括：点击'社群'，单个客户群可点击右侧'推送'按钮；批量推送时，选中多个客户群前的复选框，然后点击右下方'推送'按钮。",
			})

			// 4. 个微推送功能
			ragList = append(ragList, doubao.RAGObject{
				Title:   "个微推送客户和客户群",
				Content: "个微推送客户：1. 创建推送高级群发计划；2. 对推送计划进行配置；3. 编辑推送内容，并选择推送时间，完成计划创建。个微推送客户群：1. 创建新的高级群发方案；2. 填写要推送的消息内容，按需设置消息间的延迟时间以及计划的推送时间，完成计划配置。",
			})

			// 5. 工作台功能
			ragList = append(ragList, doubao.RAGObject{
				Title:   "工作台高效推送",
				Content: "工作台功能旨在方便用户在一个屏幕下高效创建推送计划，无需来回切换群组。操作步骤：1. 进入工作台；2. 选择要推送的客户群；3. 选择推送的个微号，点击立即推送按钮；4. 推送成功后，展示在推送记录中。",
			})

			// 6. 跟帖功能
			ragList = append(ragList, doubao.RAGObject{
				Title:   "跟帖功能说明",
				Content: "跟帖即同条消息由多个微号发送。操作步骤：1. 点击创建计划按钮，创建一个新计划；2. 选择多个微号共有客户群，开启多选后选择多个发言人，默认使用跟帖进行发送；3. 编辑消息内容，并设置消息间的延迟时间及计划的推送时间，点击提交创建，完成计划创建。创建成功后，选择的微号会在相同客户群发送相同的消息内容。",
			})

			// 7. 推送状态说明
			ragList = append(ragList, doubao.RAGObject{
				Title:   "推送计划状态详解",
				Content: "推送计划有五种状态：1. 待推送：展示定时时间未到达的推送计划和手动推送计划，不能修改内容，需暂停后在已暂停状态中修改；2. 推送中：到达推送时间正在推送中，不能修改内容；3. 已暂停：展示待推送和推送中暂停的计划，可以修改内容并重新开启；4. 推送成功：推送完成的计划，不可修改或暂停；5. 推送失败：推送失败的计划，不可修改或暂停。所有状态均可复制推送计划。",
			})

			// 8. 批量管理
			ragList = append(ragList, doubao.RAGObject{
				Title:   "批量删除推送计划",
				Content: "批量删除操作：开启批量选择键，选中要删除的计划，点击右下删除按钮，完成计划批量删除。",
			})

			// 9. 注意事项
			ragList = append(ragList, doubao.RAGObject{
				Title:   "推送系统注意事项",
				Content: "1. 推送成功和失败、暂停推送及推送中的记录目前保存7天，定时后等待推送的计划不会删除；2. 不建议直接推送给客户，时间长且比较容易导致封号。如遇到问题，请参考常见问题中的推送部分。",
			})
			marshal, err := json.Marshal(ragList)
			if err != nil {
				fmt.Printf("❌ Marshal失败: %v\n", err)
				os.Exit(1)
			}

			if err := client.ChatRAGText(&doubao.ChatRAGTextPayload{ExternalRAG: string(marshal)}); err != nil {
				fmt.Printf("❌ ChatRAGText失败: %v\n", err)
				os.Exit(1)
			}

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
