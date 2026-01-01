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
	// 从环境变量读取配置
	appID := os.Getenv("DOUBAO_APP_ID")
	accessToken := os.Getenv("DOUBAO_ACCESS_TOKEN")

	if appID == "" || accessToken == "" {
		fmt.Println("请设置环境变量：DOUBAO_APP_ID 和 DOUBAO_ACCESS_TOKEN")
		os.Exit(1)
	}

	// 创建日志
	logger := log.NewStdLogger(os.Stdout)
	logger = log.With(logger, "caller", log.Caller(4), "ts", log.DefaultTimestamp)

	// 配置客户端
	cfg := &doubao.RealtimeVoiceClientConfig{
		Appid:       appID,
		AccessToken: accessToken,
		Speaker:     "zh_female_vv_jupiter_bigtts", // 可以改为其他音色
		Log:         log.NewHelper(log.With(logger, "module", "doubao")),
	}

	// 创建客户端
	client := doubao.NewRealtimeVoiceClient(cfg)

	// 音频输出文件
	outputFile, err := os.Create("tts_output.pcm")
	if err != nil {
		fmt.Printf("创建输出文件失败: %v\n", err)
		os.Exit(1)
	}
	defer outputFile.Close()

	// 设置回调
	audioReceived := false
	client.OnAudio = func(audioData []byte) {
		if !audioReceived {
			fmt.Println("  [开始接收音频]")
			audioReceived = true
		}
		if _, err := outputFile.Write(audioData); err != nil {
			fmt.Printf("  [错误] 写入音频失败: %v\n", err)
		}
	}

	client.OnTTSStart = func(metadata *doubao.MsgMetadata) {
		fmt.Printf("  [TTS开始] TaskID=%s\n", metadata.TTSTaskID)
	}

	client.OnOutputTranscript = func(text string, metadata *doubao.MsgMetadata) {
		fmt.Printf("  [TTS文本] %s\n", text)
	}

	client.OnError = func(err error) {
		fmt.Printf("  [错误] %v\n", err)
	}

	// 启动连接
	fmt.Println("正在连接豆包服务器...")
	if err := client.Start(); err != nil {
		fmt.Printf("连接失败: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	// 启动对话
	if err := client.RealTimeDialog(); err != nil {
		fmt.Printf("启动对话失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("TTS 纯语音合成示例")
	fmt.Println("====================")

	// 要合成的文本列表
	textsToSpeak := []string{
		"你好，我是小金，一个中文语音助手。",
		"今天天气真不错，适合出去走走。",
		"人工智能正在改变我们的生活。",
		"祝你有美好的一天！",
	}

	// 依次合成每段文本
	for i, text := range textsToSpeak {
		fmt.Printf("\n[文本 %d] %s\n", i+1, text)

		if err := client.SpeakText(text); err != nil {
			fmt.Printf("  [错误] 发送失败: %v\n", err)
			continue
		}

		// 等待 TTS 完成
		// 在实际应用中，应该使用事件回调来判断何时完成
		audioReceived = false
		time.Sleep(3 * time.Second)
	}

	fmt.Println("\n所有文本已发送，等待合成完成...")
	time.Sleep(2 * time.Second)

	fmt.Printf("\n音频已保存到: tts_output.pcm\n")
	fmt.Println("可以使用以下命令播放（需要安装 ffplay）：")
	fmt.Println("  ffplay -f s16le -ar 24000 -ac 1 tts_output.pcm")
	fmt.Println("\n或转换为 WAV 格式：")
	fmt.Println("  ffmpeg -f s16le -ar 24000 -ac 1 -i tts_output.pcm tts_output.wav")

	// 等待用户中断
	fmt.Println("\n按 Ctrl+C 退出...")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
}
