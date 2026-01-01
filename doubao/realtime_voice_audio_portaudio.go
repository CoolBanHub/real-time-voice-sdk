//go:build portaudio

package doubao

import (
	"time"

	"github.com/gorilla/portaudio"
)

// PortAudio 相关的实现，只在编译时指定 -tags portaudio 时才会编译

func (this *RealtimeVoiceClient) startAudioCapture() {
	if !this.isUsePortAudio {
		return
	}
	go this.sendAudioByPortAudio()
}

func (this *RealtimeVoiceClient) startAudioPlayback() {
	if !this.isUsePortAudio {
		return
	}
	go this.startPlayer()
}

func (this *RealtimeVoiceClient) sendAudioByPortAudio() {
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

	streamParameters := portaudio.StreamParameters{
		Input: portaudio.StreamDeviceParameters{
			Device:   defaultInputDevice,
			Channels: 1,
			Latency:  defaultInputDevice.DefaultLowInputLatency,
		},
		SampleRate:      16000,
		FramesPerBuffer: 160,
	}

	// sayhello后模拟chatTextQuery
	time.Sleep(5 * time.Second)
	stream, err := portaudio.OpenStream(streamParameters, func(in []int16) {
		// 1. 将 int16 音频数据转换为 []byte (PCM S16LE)
		audioBytes := make([]byte, len(in)*2)
		for i, sample := range in {
			audioBytes[i*2] = byte(sample & 0xff)
			audioBytes[i*2+1] = byte((sample >> 8) & 0xff)
		}

		// 2. 设置序列化方式为原始数据
		this.protocol.SetSerialization(SerializationRaw)

		// 3. 创建并发送消息
		msg, err := NewMessage(MsgTypeAudioOnlyClient, MsgTypeFlagWithEvent)
		if err != nil {
			this.log.Errorf("Error creating audio message: %v", err)
			return // 从回调中退出
		}

		msg.Event = 200
		msg.SessionID = this.SessionId
		msg.Payload = audioBytes

		frame, err := this.protocol.Marshal(msg)
		if err != nil {
			this.log.Errorf("Error marshalling audio message: %v", err)
			return // 从回调中退出
		}

		if err := this.sendRequest(this.doubaoWSConn, frame); err != nil {
			this.log.Errorf("Error sending audio message: %v", err)
			return
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
		err = this.finishSession()
		if err != nil {
			this.log.Errorf("Failed to finish session: %v", err)
		}
	}
	this.log.Info("Microphone input stream stopped.")
}

func (this *RealtimeVoiceClient) startPlayer() {
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
