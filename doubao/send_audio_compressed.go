package doubao

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
)

// SendAudioWithCompression 发送音频数据（带GZIP压缩）
// 参考 JavaScript 实现: doubaoRealtimeClient.js:682
func (this *RealtimeVoiceClient) SendAudioWithCompression(content []byte) error {
	if this.doubaoWSConn == nil {
		return errors.New("doubaoWSConn is nil")
	}
	// 临时设置为 Raw + GZIP（仅用于音频）
	this.protocol.SetSerialization(SerializationRaw)
	this.protocol.SetCompression(CompressionGzip, nil)

	// 16000Hz, 16bit, 1 channel (PCM format matching client recording rate)
	this.log.Debugf("[SendAudioCompressed] 准备发送音频: %d bytes (%.2f samples)", len(content), float64(len(content))/2.0)

	// 1. 生成协议头（使用GZIP压缩）
	header := this.protocol.GenerateHeaderWithCompression(
		uint8(Version1),
		uint8(MsgTypeAudioOnlyClient),
		uint8(MsgTypeFlagWithEvent),
		uint8(SerializationRaw), // NO_SERIALIZATION for audio
		uint8(CompressionGzip),  // GZIP compression
	)

	// 2. Event ID (EVENT_TASK_REQUEST = 200)
	eventIdBuffer := make([]byte, 4)
	binary.BigEndian.PutUint32(eventIdBuffer, uint32(EVENT_TASK_REQUEST))

	// 3. Session ID
	sessionIdBytes := []byte(this.SessionId)
	sessionIdSize := make([]byte, 4)
	binary.BigEndian.PutUint32(sessionIdSize, uint32(len(sessionIdBytes)))

	// 4. 压缩音频数据
	var compressedAudio bytes.Buffer
	gzipWriter := gzip.NewWriter(&compressedAudio)
	if _, err := gzipWriter.Write(content); err != nil {
		this.log.Errorf("GZIP compress error: %v", err)
		return fmt.Errorf("compress audio: %w", err)
	}
	if err := gzipWriter.Close(); err != nil {
		this.log.Errorf("GZIP writer close error: %v", err)
		return fmt.Errorf("close gzip writer: %w", err)
	}

	compressedBytes := compressedAudio.Bytes()
	this.log.Debugf("[SendAudioCompressed] 压缩后: %d bytes (压缩率: %.1f%%)",
		len(compressedBytes),
		float64(len(compressedBytes))/float64(len(content))*100)

	// 5. Payload size
	payloadSize := make([]byte, 4)
	binary.BigEndian.PutUint32(payloadSize, uint32(len(compressedBytes)))

	// 6. 组装完整消息
	message := bytes.Join([][]byte{
		header,
		eventIdBuffer,
		sessionIdSize,
		sessionIdBytes,
		payloadSize,
		compressedBytes,
	}, nil)

	// 7. 发送
	if err := this.sendRequest(this.doubaoWSConn, message); err != nil {
		this.log.Errorf("[SendAudioCompressed] 发送失败: %v", err)
		return err
	}

	this.log.Debugf("[SendAudioCompressed] 成功发送: %d bytes (原始音频: %d bytes)",
		len(message), len(content))
	// 恢复默认设置（JSON + 无压缩）
	this.protocol.SetSerialization(SerializationJSON)
	this.protocol.SetCompression(CompressionNone, nil)
	return nil
}

// GenerateHeaderWithCompression 生成带压缩标志的协议头
func (p *BinaryProtocol) GenerateHeaderWithCompression(
	version uint8,
	messageType uint8,
	msgFlags uint8,
	serialization uint8,
	compression uint8,
) []byte {
	header := make([]byte, 4)

	// Byte 0: version (4 bits) + header size (4 bits)
	headerSize := uint8(1) // 基础头大小为 4 字节 = 1 * 4
	header[0] = (version << 4) | headerSize

	// Byte 1: message type (4 bits) + flags (4 bits)
	header[1] = (messageType << 4) | msgFlags

	// Byte 2: serialization (4 bits) + compression (4 bits)
	header[2] = (serialization << 4) | compression

	// Byte 3: reserved
	header[3] = 0x00

	return header
}
