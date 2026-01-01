package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/CoolBanHub/real-time-voice-sdk/doubao"
	"github.com/go-kratos/kratos/v2/log"
)

// PortAudio 实时语音对话示例
//
// 此示例展示如何使用 PortAudio 进行实时语音对话：
// 1. 从麦克风实时捕获音频并发送到服务器
// 2. 接收服务器的语音响应并通过扬声器播放
//
// 编译方法：
//   go build -tags portaudio
//
// 运行前准备：
//   macOS:   brew install portaudio
//   Ubuntu:  sudo apt-get install portaudio19-dev
//   Windows: pacman -S mingw-w64-x86_64-portaudio (MSYS2)
//
// 运行方法：
//   export DOUBAO_APP_ID=your-app-id
//   export DOUBAO_ACCESS_TOKEN=your-token
//   ./portaudio_realtime

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
	fmt.Println("  豆包实时语音对话示例 (PortAudio)")
	fmt.Println("==============================================")
	fmt.Println()

	// 配置客户端
	cfg := &doubao.RealtimeVoiceClientConfig{
		Appid:       appID,
		AccessToken: accessToken,
		Speaker:     "zh_female_vv_jupiter_bigtts", // 音色配置
		Log:         log.NewHelper(log.With(logger, "module", "doubao")),
		// 启用 PortAudio 实时音频捕获和播放
		// 注意：需要使用 -tags portaudio 编译
		IsUsePortAudio: true,
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

	// 设置事件回调
	setupCallbacks(client)

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

	// 说明信息
	printUsageInfo()

	// 等待用户中断
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\n正在关闭连接...")
}

func setupCallbacks(client *doubao.RealtimeVoiceClient) {
	// 接收服务器音频数据（PortAudio 会自动播放）
	client.OnAudio = func(audioData []byte) {
		// PortAudio 启用时，音频会自动通过扬声器播放
		// 这里可以记录音频接收情况
		// fmt.Printf("  [音频] 接收 %d bytes\n", len(audioData))
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
