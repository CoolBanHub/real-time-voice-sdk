//go:build !portaudio

package doubao

// PortAudio 的空实现，在不使用 -tags portaudio 编译时使用
// 这样服务器端编译时不需要 CGO，也不会依赖 PortAudio

func (this *RealtimeVoiceClient) startAudioCapture() {
	// 空实现，不使用 PortAudio
	if this.isUsePortAudio {
		this.log.Warn("PortAudio support is not enabled. Compile with -tags portaudio to enable.")
	}
}

func (this *RealtimeVoiceClient) startAudioPlayback() {
	// 空实现，不使用 PortAudio
	if this.isUsePortAudio {
		this.log.Warn("PortAudio support is not enabled. Compile with -tags portaudio to enable.")
	}
}
