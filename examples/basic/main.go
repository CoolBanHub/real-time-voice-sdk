package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/real-time-voice-sdk/doubao"
)

func main() {
	// 从环境变量读取配置（生产环境推荐做法）
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

	// 配置客户端
	cfg := &doubao.RealtimeVoiceClientConfig{
		Appid:       appID,
		AccessToken: accessToken,
		Speaker:     "zh_female_vv_jupiter_bigtts", // 女声音色（快捷配置）
		Log:         log.NewHelper(log.With(logger, "module", "doubao")),

		// 可选：自定义重连配置（默认已启用，可省略）
		// Reconnect: &doubao.ReconnectConfig{
		// 	Enabled:           true,
		// 	MaxAttempts:       10,
		// 	InitialDelay:      1 * time.Second,
		// 	MaxDelay:          60 * time.Second,
		// 	BackoffMultiplier: 2.0,
		// },

		// 可选：自定义 TTS 配置（如需精确控制音频格式）
		// TTS: &doubao.TTSPayload{
		// 	Speaker: "zh_female_vv_jupiter_bigtts",
		// 	AudioConfig: doubao.AudioConfig{
		// 		Channel:    1,
		// 		Format:     "pcm_s16le",  // 或 "pcm" (float32)
		// 		SampleRate: 24000,
		// 	},
		// },
	}

	// 创建客户端
	client := doubao.NewRealtimeVoiceClient(cfg)

	// 设置事件回调
	setupCallbacks(client)

	// 启动 WebSocket 连接
	fmt.Println("正在连接豆包服务器...")
	if err := client.Start(); err != nil {
		fmt.Printf("连接失败: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	fmt.Println("连接成功！")

	// 启动实时对话会话
	if err := client.RealTimeDialog(); err != nil {
		fmt.Printf("启动对话失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("对话会话已启动！")
	fmt.Println("发送测试消息...")

	// 发送文本查询示例
	testQueries := []string{
		"你好，请介绍一下你自己",
		"今天北京的天气怎么样？",
		"给我讲一个有趣的笑话",
	}

	for i, query := range testQueries {
		fmt.Printf("\n[查询 %d] %s\n", i+1, query)
		if err := client.SendText(query); err != nil {
			fmt.Printf("发送失败: %v\n", err)
		}

		// 等待一段时间让响应完成
		// 在实际应用中，应该使用更智能的等待机制
		// 可以通过 OnChatEnded 回调来判断响应是否结束
		fmt.Println("等待响应...")
		// time.Sleep(5 * time.Second)
	}

	// 等待用户中断
	fmt.Println("\n按 Ctrl+C 退出...")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\n正在关闭连接...")
}

func setupCallbacks(client *doubao.RealtimeVoiceClient) {
	// 接收音频数据
	client.OnAudio = func(audioData []byte) {
		fmt.Printf("  [音频] 接收 %d bytes\n", len(audioData))
		// 在这里可以：
		// 1. 保存到文件
		// 2. 发送到音频播放设备
		// 3. 转发到其他系统
	}

	// ASR 最终识别结果
	client.OnInputTranscript = func(text string, isFinal bool) {
		if isFinal {
			fmt.Printf("  [ASR-最终] %s\n", text)
		}
	}

	// ASR 临时识别结果
	client.OnInputTranscriptPartial = func(text string, isFinal bool) {
		fmt.Printf("  [ASR-临时] %s\n", text)
	}

	// ASR 结束（用户停止说话）
	client.OnASREnded = func() {
		fmt.Println("  [ASR] 用户停止说话")
	}

	// TTS 开始
	client.OnTTSStart = func(metadata *doubao.MsgMetadata) {
		fmt.Printf("  [TTS-开始] 类型=%s, TaskID=%s\n",
			metadata.TTSType, metadata.TTSTaskID)
	}

	// TTS 输出文本
	client.OnOutputTranscript = func(text string, metadata *doubao.MsgMetadata) {
		fmt.Printf("  [TTS-文本] %s\n", text)
	}

	// LLM 流式响应
	client.OnChatResponse = func(content string) {
		fmt.Print(content)
	}

	// LLM 响应结束
	client.OnChatEnded = func(msg *doubao.Message) {
		fmt.Println("\n  [LLM] 响应结束")
	}

	// 用户打断
	client.OnInterrupt = func() {
		fmt.Println("  [系统] 用户打断")
	}

	// 错误处理
	client.OnError = func(err error) {
		fmt.Printf("  [错误] %v\n", err)
	}

	// 连接状态变化
	client.OnConnectionStateChange = func(oldState, newState doubao.ConnectionState) {
		fmt.Printf("  [连接] 状态变化: %s -> %s\n", oldState, newState)
	}

	// 重连尝试
	client.OnReconnecting = func(attempt int, delay time.Duration) {
		fmt.Printf("  [重连] 第 %d 次尝试，延迟 %v\n", attempt, delay)
	}

	// 重连失败
	client.OnReconnectFailed = func(attempts int, err error) {
		fmt.Printf("  [重连失败] 所有 %d 次尝试均失败: %v\n", attempts, err)
	}
}
