package main

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/real-time-voice-sdk/doubao"
)

const (
	// 音频参数（输入）
	sampleRate    = 16000 // 16kHz
	bitsPerSample = 16    // 16bit
	channels      = 1     // 单声道
	chunkSize     = 3200  // 每次发送的字节数 (100ms @ 16kHz/16bit/mono)
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("用法: go run main.go <audio.pcm>")
		fmt.Println("音频格式要求: 16000Hz, 16bit, 单声道 PCM")
		os.Exit(1)
	}

	audioFile := os.Args[1]

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
		Speaker:     "zh_female_vv_jupiter_bigtts",
		Log:         log.NewHelper(log.With(logger, "module", "doubao")),
	}

	// 创建客户端
	client := doubao.NewRealtimeVoiceClient(cfg)

	// 音频输出文件
	outputFile, err := os.Create("output.pcm")
	if err != nil {
		fmt.Printf("创建输出文件失败: %v\n", err)
		os.Exit(1)
	}
	defer outputFile.Close()

	// 设置回调
	setupCallbacks(client, outputFile)

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

	fmt.Printf("正在流式发送音频文件: %s\n", audioFile)

	// 在 goroutine 中发送音频
	go func() {
		if err := streamAudioFile(client, audioFile); err != nil {
			fmt.Printf("音频流式发送失败: %v\n", err)
		}
	}()

	// 等待用户中断
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\n正在关闭连接...")
	fmt.Printf("输出音频已保存到: output.pcm\n")
}

func streamAudioFile(client *doubao.RealtimeVoiceClient, filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("打开音频文件失败: %w", err)
	}
	defer file.Close()

	buffer := make([]byte, chunkSize)
	sentBytes := 0
	chunkCount := 0

	fmt.Println("开始发送音频数据...")

	for {
		n, err := file.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("读取音频文件失败: %w", err)
		}

		// 发送音频数据
		if err := client.SendAudio(buffer[:n]); err != nil {
			return fmt.Errorf("发送音频失败: %w", err)
		}

		sentBytes += n
		chunkCount++

		// 每秒钟发送10个块（100ms 间隔）
		time.Sleep(100 * time.Millisecond)

		if chunkCount%10 == 0 {
			fmt.Printf("  已发送 %d bytes (%d 块)\n", sentBytes, chunkCount)
		}
	}

	fmt.Printf("音频发送完成！总计: %d bytes (%d 块)\n", sentBytes, chunkCount)
	return nil
}

func setupCallbacks(client *doubao.RealtimeVoiceClient, outputFile *os.File) {
	// 接收音频数据并保存
	client.OnAudio = func(audioData []byte) {
		fmt.Printf("  [接收音频] %d bytes\n", len(audioData))
		if _, err := outputFile.Write(audioData); err != nil {
			fmt.Printf("  [错误] 写入音频失败: %v\n", err)
		}
	}

	// ASR 识别结果
	client.OnInputTranscript = func(text string, isFinal bool) {
		if isFinal {
			fmt.Printf("  [ASR识别] %s\n", text)
		}
	}

	// LLM 响应
	client.OnChatResponse = func(content string) {
		fmt.Print(content)
	}

	client.OnChatEnded = func(msg *doubao.Message) {
		fmt.Println("\n  [响应结束]")
	}

	// TTS 文本
	client.OnOutputTranscript = func(text string, metadata *doubao.MsgMetadata) {
		fmt.Printf("  [TTS文本] %s\n", text)
	}

	// 错误处理
	client.OnError = func(err error) {
		fmt.Printf("  [错误] %v\n", err)
	}
}
