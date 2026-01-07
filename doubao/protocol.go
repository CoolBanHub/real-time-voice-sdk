package doubao

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/CoolBanHub/real-time-voice-sdk/pkg/log"
)

// Protocol-level errors that can occur during binary protocol serialization/deserialization
var (
	// ErrProtocolNoVersionAndSize indicates missing protocol version and header size byte
	ErrProtocolNoVersionAndSize = errors.New("no protocol version and header size byte")

	// ErrProtocolNoTypeAndFlag indicates missing message type and specific flag byte
	ErrProtocolNoTypeAndFlag = errors.New("no message type and specific flag byte")

	// ErrProtocolNoSerializationAndCompression indicates missing serialization and compression method byte
	ErrProtocolNoSerializationAndCompression = errors.New("no serialization and compression method byte")

	// ErrProtocolRedundantBytes indicates there are redundant bytes in data
	ErrProtocolRedundantBytes = errors.New("there are redundant bytes in data")

	// ErrProtocolInvalidMessageType indicates invalid message type bits
	ErrProtocolInvalidMessageType = errors.New("invalid message type bits")

	// ErrProtocolInvalidSerialization indicates invalid serialization bits
	ErrProtocolInvalidSerialization = errors.New("invalid serialization bits")

	// ErrProtocolInvalidCompression indicates invalid compression bits
	ErrProtocolInvalidCompression = errors.New("invalid compression bits")

	// ErrProtocolNoEnoughHeaderBytes indicates not enough header bytes
	ErrProtocolNoEnoughHeaderBytes = errors.New("no enough header bytes")

	// ErrProtocolReadEvent indicates error reading event number
	ErrProtocolReadEvent = errors.New("read event number")

	// ErrProtocolReadSessionIDSize indicates error reading session ID size
	ErrProtocolReadSessionIDSize = errors.New("read session ID size")

	// ErrProtocolReadConnectIDSize indicates error reading connection ID size
	ErrProtocolReadConnectIDSize = errors.New("read connection ID size")

	// ErrProtocolReadPayloadSize indicates error reading payload size
	ErrProtocolReadPayloadSize = errors.New("read payload size")

	// ErrProtocolReadPayload indicates error reading payload
	ErrProtocolReadPayload = errors.New("read payload")

	// ErrProtocolReadSequence indicates error reading sequence number
	ErrProtocolReadSequence = errors.New("read sequence number")

	// ErrProtocolReadErrorCode indicates error reading error code
	ErrProtocolReadErrorCode = errors.New("read error code")

	// ErrProtocolReadErrorSize indicates error reading error size
	ErrProtocolReadErrorSize = errors.New("read error size")

	// ErrProtocolReadError indicates error reading error message
	ErrProtocolReadError = errors.New("read error")
)

// 客户端事件 ID（参考 Python demo）
const (
	// EVENT_START_CONNECTION (1) Websocket 阶段声明创建连接
	EVENT_START_CONNECTION = iota + 1
	// EVENT_FINISH_CONNECTION (2) 断开websocket连接，后面需要重新发起websocket连接
	EVENT_FINISH_CONNECTION

	// EVENT_START_SESSION (100) Websocket 阶段声明创建会话
	// 支持配置：end_smooth_window_ms、enable_custom_vad、enable_asr_twopass、bot_name、system_role、
	// speaking_style、dialog_id、character_manifest、location、strict_audit、audit_response、
	// enable_volc_websearch、volc_websearch_type、volc_websearch_api_key、volc_websearch_bot_id、
	// volc_websearch_result_count、volc_websearch_no_result_message、input_mod、enable_music、model等参数
	EVENT_START_SESSION = 100
	// EVENT_FINISH_SESSION (102) 客户端声明结束会话，后面可以复用websocket连接
	EVENT_FINISH_SESSION = 102

	// EVENT_TASK_REQUEST (200) 客户端上传音频二进制数据
	EVENT_TASK_REQUEST = 200

	// EVENT_SAY_HELLO (300) 客户端提交打招呼文本
	EVENT_SAY_HELLO = 300

	// EVENT_CHAT_TTS_TEXT (500) 用户query之后，模型会生成闲聊结果。如果客户判断用户query不需要闲聊结果，可以指定文本合成音频
	EVENT_CHAT_TTS_TEXT = 500
	// EVENT_CHAT_TEXT_QUERY (501) 用户输入文本query，模型输出闲聊结果。若用户判断不采用音频输入进行query，可使用该事件输入文本进行query
	EVENT_CHAT_TEXT_QUERY = 501
	// EVENT_CHAT_RAG_TEXT (502) 用户query之后，模型会生成闲聊结果。如果客户判断用户query不需要闲聊结果，可以输入外部RAG知识，
	// 通过模型的总结和口语化改写之后输出对应音频。外部RAG输入整体长度不超过4K个字符。（端到端模型O版本）
	EVENT_CHAT_RAG_TEXT = 502

	// EVENT_CONVERSATION_CREATE (510) 上下文追加规则：每次仅允许提交一条问答（QA）记录
	// 若未提供时间戳，则将该记录追加至当前上下文末尾
	// 若提供时间戳，则按时间顺序将该记录插入到上下文中
	// 时间戳策略需保持一致：要么所有记录均携带时间戳，要么全部不携带，不能混用
	EVENT_CONVERSATION_CREATE = 510
	// EVENT_CONVERSATION_UPDATE (511) 更新上下文规则（用于更新指定 item_id 对应消息的文本内容）
	// item_id 可从是question_id即更新用户问题，也可以是reply_id即更新模型回复内容
	// question_id表示当前轮次中用户query的item_id，在一轮对话中不会变化
	// reply_id：表示当前轮次中模型回复消息的item_id
	EVENT_CONVERSATION_UPDATE = 511
	// EVENT_CONVERSATION_RETRIEVE (512) 查询上下文规则：未传入item_id返回最近20轮完整对话上下文
	// 传入item_id返回指定item_id所在轮次的上下文记录
	EVENT_CONVERSATION_RETRIEVE = 512
	// EVENT_CONVERSATION_DELETE (514) 删除上下文规则：删除操作以对话轮为单位进行
	// 当传入某条 用户侧的 item_id 时，将同时删除与之成对的 助手回复记录（即整轮对话一起删除）
	// 同理，若传入助手侧 item_id，系统也会删除与其对应的用户消息，确保上下文不出现不完整对话
	EVENT_CONVERSATION_DELETE = 514
)

// 服务端事件 ID
const (
	// EVENT_CONNECTION_STARTED (50) 成功建立连接
	EVENT_CONNECTION_STARTED = 50
	// EVENT_CONNECTION_FAILED (51) 建立连接失败，返回错误信息
	EVENT_CONNECTION_FAILED = 51
	// EVENT_CONNECTION_FINISHED (52) 连接结束
	EVENT_CONNECTION_FINISHED = 52

	// EVENT_SESSION_STARTED (150) 成功启动会话，返回的dialog_id用于接续最近的对话内容，增加模型智能度
	EVENT_SESSION_STARTED = 150
	// EVENT_SESSION_FINISHED (152) 会话已结束
	EVENT_SESSION_FINISHED = 152
	// EVENT_SESSION_FAILED (153) 会话失败，返回错误信息
	EVENT_SESSION_FAILED = 153
	// EVENT_USAGE_INFO (154) 每一轮交互对应的用量信息，包含输入输出的文本和音频tokens
	EVENT_USAGE_INFO = 154

	// EVENT_TTS_SENTENCE_START (350) 合成音频的起始事件
	// tts_type取值：audit_content_risky（安全审核）、chat_tts_text（文本合成）、network（联网）、
	// external_rag（外部RAG）、sing（唱歌）、default（闲聊）
	EVENT_TTS_SENTENCE_START = 350
	// EVENT_TTS_SENTENCE_END (351) 合成音频的分句结束事件
	EVENT_TTS_SENTENCE_END = 351
	// EVENT_TTS_RESPONSE (352) 返回模型生成的音频数据，payload装载二进制音频数据
	EVENT_TTS_RESPONSE = 352
	// EVENT_TTS_ENDED (359) 模型一轮音频合成结束事件
	EVENT_TTS_ENDED = 359

	// EVENT_ASR_STARTED (450) 模型识别出音频流中的首字返回的事件，用于打断客户端的播报
	EVENT_ASR_STARTED = 450
	// EVENT_ASR_RESPONSE (451) 模型识别出用户说话的文本内容，包含识别结果和是否为中间结果
	EVENT_ASR_RESPONSE = 451
	// EVENT_ASR_ENDED (459) 模型认为用户说话结束的事件
	EVENT_ASR_ENDED = 459

	// EVENT_CHAT_RESPONSE (550) 模型回复的文本内容，包含question_id和reply_id
	EVENT_CHAT_RESPONSE = 550
	// EVENT_CHAT_TEXT_QUERY_CONFIRMED (553) ChatTextQuery请求对应的ack，返回question_id
	EVENT_CHAT_TEXT_QUERY_CONFIRMED = 553
	// EVENT_CHAT_ENDED (559) 模型回复文本结束事件，包含question_id和reply_id
	EVENT_CHAT_ENDED = 559

	// EVENT_CONVERSATION_CREATED (567) 增加上下文请求对应的ack，返回创建成功的上下文item数组
	EVENT_CONVERSATION_CREATED = 567
	// EVENT_CONVERSATION_UPDATED (568) 更新上下文请求对应的ack，更新成功返回空，失败返回错误信息
	EVENT_CONVERSATION_UPDATED = 568
	// EVENT_CONVERSATION_RETRIEVED (569) 查询上下文请求对应的ack，返回上下文item数组
	EVENT_CONVERSATION_RETRIEVED = 569
	// EVENT_CONVERSATION_DELETED (571) 删除上下文请求对应的ack，返回被删除的上下文item数组
	EVENT_CONVERSATION_DELETED = 571

	// EVENT_DIALOG_COMMON_ERROR (599) 实时通话过程中相关错误描述
	EVENT_DIALOG_COMMON_ERROR = 599
)

type (
	// MsgType defines message type which determines how the message will be
	// serialized with the protocol.
	MsgType int32
	// MsgTypeFlagBits defines the 4-bit message-type specific flags. The specific
	// values should be defined in each specific usage scenario.
	MsgTypeFlagBits uint8

	// VersionBits defines the 4-bit version type.
	VersionBits uint8
	// HeaderSizeBits defines the 4-bit header-size type.
	HeaderSizeBits uint8
	// SerializationBits defines the 4-bit serialization method type.
	SerializationBits uint8
	// CompressionBits defines the 4-bit compression method type.
	CompressionBits uint8
)

// Values that a MsgType variable can take.
const (
	MsgTypeInvalid MsgType = iota
	MsgTypeFullClient
	MsgTypeAudioOnlyClient
	MsgTypeFullServer
	MsgTypeAudioOnlyServer
	MsgTypeFrontEndResultServer
	MsgTypeError

	MsgTypeServerACK = MsgTypeAudioOnlyServer
)

func (t MsgType) String() string {
	switch t {
	case MsgTypeFullClient:
		return "FullClient"
	case MsgTypeAudioOnlyClient:
		return "AudioOnlyClient"
	case MsgTypeFullServer:
		return "FullServer"
	case MsgTypeAudioOnlyServer:
		return "AudioOnlyServer/ServerACK"
	case MsgTypeError:
		return "Error"
	case MsgTypeFrontEndResultServer:
		return "TtsFrontEndResult"
	default:
		return fmt.Sprintf("invalid message type: %d", t)
	}
}

// Values that a MsgTypeFlagBits variable can take.
const (
	// For common protocol.
	MsgTypeFlagNoSeq       MsgTypeFlagBits = 0     // Non-terminal packet with no sequence
	MsgTypeFlagPositiveSeq MsgTypeFlagBits = 0b1   // Non-terminal packet with sequence > 0
	MsgTypeFlagLastNoSeq   MsgTypeFlagBits = 0b10  // last packet with no sequence
	MsgTypeFlagNegativeSeq MsgTypeFlagBits = 0b11  // last packet with sequence < 0
	MsgTypeFlagWithEvent   MsgTypeFlagBits = 0b100 // Payload contains event number (int32)
)

// Values that a VersionBits variable can take.
const (
	Version1 VersionBits = (iota + 1) << 4
	Version2
	Version3
	Version4
)

// Values that a HeaderSizeBits variable can take.
const (
	HeaderSize4 HeaderSizeBits = iota + 1
	HeaderSize8
	HeaderSize12
	HeaderSize16
)

// Values that a SerializationBits variable can take.
const (
	SerializationRaw    SerializationBits = 0
	SerializationJSON   SerializationBits = 0b1 << 4
	SerializationThrift SerializationBits = 0b11 << 4
	SerializationCustom SerializationBits = 0b1111 << 4
)

// Values that a CompressionBits variable can take.
const (
	CompressionNone   CompressionBits = 0
	CompressionGzip   CompressionBits = 0b1
	CompressionCustom CompressionBits = 0b1111
)

var (
	msgTypeToBits = map[MsgType]uint8{
		MsgTypeFullClient:           0b1 << 4,
		MsgTypeAudioOnlyClient:      0b10 << 4,
		MsgTypeFullServer:           0b1001 << 4,
		MsgTypeAudioOnlyServer:      0b1011 << 4,
		MsgTypeFrontEndResultServer: 0b1100 << 4,
		MsgTypeError:                0b1111 << 4,
	}
	bitsToMsgType = make(map[uint8]MsgType, len(msgTypeToBits))

	serializations = map[SerializationBits]bool{
		SerializationRaw:    true,
		SerializationJSON:   true,
		SerializationThrift: true,
		SerializationCustom: true,
	}

	compressions = map[CompressionBits]bool{
		CompressionNone:   true,
		CompressionGzip:   true,
		CompressionCustom: true,
	}
)

func init() {
	// Construct inverse mapping of msgTypeToBits.
	for msgType, bits := range msgTypeToBits {
		bitsToMsgType[bits] = msgType
	}
}

// ContainsSequenceFunc defines the functional type that checks whether the
// MsgTypeFlagBits indicates the existence of a sequence number in serialized
// data. The background is that not all responses contain a sequence number,
// and whether a response contains one depends on the message type specific
// flag bits. What makes it more complicated is that this dependency varies in
// each use case (eg, TTS protocol has its own dependency specification, more
// details at: https://bytedance.feishu.cn/docs/doccn8MD4cZHQuvobbtouWfUVsV).
type ContainsSequenceFunc func(MsgTypeFlagBits) bool

// CompressFunc defines the functional type that does the compression operation.
type CompressFunc func([]byte) ([]byte, error)

type readFunc func(*bytes.Buffer) error
type writeFunc func(*bytes.Buffer) error

// Unmarshal deserializes the binary `data` into a Message and also returns
// the BinaryProtocol.
func Unmarshal(data []byte, containsSequence ContainsSequenceFunc) (*Message, *BinaryProtocol, error) {
	var (
		buf      = bytes.NewBuffer(data)
		readSize int
	)

	versionSize, err := buf.ReadByte()
	if err != nil {
		return nil, nil, ErrProtocolNoVersionAndSize
	}
	readSize++

	prot := &BinaryProtocol{
		versionAndHeaderSize: versionSize,
		containsSequence:     containsSequence,
	}
	log.Infof("Read version: %04b", versionSize>>4)
	log.Infof("Read size: %04b", versionSize&0b1111)

	typeAndFlag, err := buf.ReadByte()
	if err != nil {
		return nil, nil, ErrProtocolNoTypeAndFlag
	}
	readSize++
	log.Infof("Read message type: %04b", typeAndFlag>>4)
	log.Infof("Read message type specific flag: %04b", typeAndFlag&0b1111)

	msg, err := NewMessageFromByte(typeAndFlag)
	if err != nil {
		return nil, nil, err
	}

	serializationCompression, err := buf.ReadByte()
	if err != nil {
		return nil, nil, ErrProtocolNoSerializationAndCompression
	}
	log.Infof("Read serialization method: %04b", serializationCompression>>4)
	log.Infof("Read compression method: %04b", serializationCompression&0b1111)
	readSize++
	prot.serializationAndCompression = serializationCompression
	if _, ok := serializations[prot.Serialization()]; !ok {
		return nil, nil, fmt.Errorf("%w: %b", ErrProtocolInvalidSerialization, prot.Serialization())
	}
	if _, ok := compressions[prot.Compression()]; !ok {
		return nil, nil, fmt.Errorf("%w: %b", ErrProtocolInvalidCompression, prot.Compression())
	}

	// Read all the remaining zero-padding bytes in the header.
	if paddingSize := prot.HeaderSize() - readSize; paddingSize > 0 {
		if n, err := buf.Read(make([]byte, paddingSize)); err != nil || n < paddingSize {
			return nil, nil, fmt.Errorf("%w: %d", ErrProtocolNoEnoughHeaderBytes, n)
		}
	}

	readers, err := msg.readers(containsSequence)
	if err != nil {
		return nil, nil, err
	}
	for _, read := range readers {
		if err := read(buf); err != nil {
			return nil, nil, err
		}
	}

	if _, err := buf.ReadByte(); err != io.EOF {
		return nil, nil, ErrProtocolRedundantBytes
	}
	return msg, prot, nil
}

// Message defines the general message content type.
type Message struct {
	Type            MsgType
	typeAndFlagBits uint8

	Event     int32
	SessionID string
	ConnectID string
	Sequence  int32
	ErrorCode uint32
	// Raw payload (not Gzip compressed). BinaryProtocol.Marshal will do the
	// compression for you.
	Payload    []byte
	IsPrintLog bool
}

// NewMessage returns a new Message instance of the given message type with the
// specific flag.
func NewMessage(msgType MsgType, typeFlag MsgTypeFlagBits) (*Message, error) {
	bits, ok := msgTypeToBits[msgType]
	if !ok {
		return nil, fmt.Errorf("invalid message type: %d", msgType)
	}
	return &Message{
		Type:            msgType,
		typeAndFlagBits: bits + uint8(typeFlag),
	}, nil
}

// NewMessageFromByte reads the byte as the message type and specific flag bits
// and composes a new Message instance from them.
func NewMessageFromByte(typeAndFlag byte) (*Message, error) {
	bits := typeAndFlag &^ 0b00001111
	msgType, ok := bitsToMsgType[bits]
	if !ok {
		return nil, fmt.Errorf("%w: %b", ErrProtocolInvalidMessageType, bits>>4)
	}
	return &Message{
		Type:            msgType,
		typeAndFlagBits: typeAndFlag,
	}, nil
}

// TypeFlag returns the message type specific flag.
func (m *Message) TypeFlag() MsgTypeFlagBits {
	return MsgTypeFlagBits(m.typeAndFlagBits &^ 0b11110000)
}

func (m *Message) writers(compress CompressFunc) (writers []writeFunc, _ error) {
	if compress != nil {
		payload, err := compress(m.Payload)
		if err != nil {
			return nil, fmt.Errorf("compress payload failed: %w", err)
		}
		m.Payload = payload
	}

	if containsSequence(m.TypeFlag()) {
		writers = append(writers, m.writeSequence)
		if m.IsPrintLog {
			log.Info("Add Sequence writer.")
		}
	}

	if containsEvent(m.TypeFlag()) {
		writers = append(writers, m.writeEvent, m.writeSessionID)
		if m.IsPrintLog {
			log.Info("Add Event and SessionID writer.")
		}
	}

	writers = append(writers, m.writePayload)
	if m.IsPrintLog {
		log.Info("Add Payload writers.")
	}
	return writers, nil
}

func (m *Message) writeEvent(buf *bytes.Buffer) error {
	if err := binary.Write(buf, binary.BigEndian, m.Event); err != nil {
		return fmt.Errorf("write sequence number (%d): %w", m.Event, err)
	}
	return nil
}

func (m *Message) writeSessionID(buf *bytes.Buffer) error {
	switch m.Event {
	case 1, 2, 50, 51, 52: // StartConnection, FinishConnection, ConnectionStarted, ConnectionFailed, ConnectionFinished
		if m.IsPrintLog {
			log.Infof("Skip writing session ID for event: %d", m.Event)
		}
		return nil
	}

	size := len(m.SessionID)
	if size > math.MaxUint32 {
		return fmt.Errorf("payload size (%d) exceeds max(uint32)", size)
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(size)); err != nil {
		return fmt.Errorf("write payload size (%d): %w", size, err)
	}
	buf.WriteString(m.SessionID)
	return nil
}

func (m *Message) writeSequence(buf *bytes.Buffer) error {
	if err := binary.Write(buf, binary.BigEndian, m.Sequence); err != nil {
		return fmt.Errorf("write sequence number (%d): %w", m.Sequence, err)
	}
	return nil
}

func (m *Message) writeErrorCode(buf *bytes.Buffer) error {
	if err := binary.Write(buf, binary.BigEndian, m.ErrorCode); err != nil {
		return fmt.Errorf("write error code (%d): %w", m.ErrorCode, err)
	}
	return nil
}

func (m *Message) writePayload(buf *bytes.Buffer) error {
	size := len(m.Payload)
	if size > math.MaxUint32 {
		return fmt.Errorf("payload size (%d) exceeds max(uint32)", size)
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(size)); err != nil {
		return fmt.Errorf("write payload size (%d): %w", size, err)
	}
	buf.Write(m.Payload)
	return nil
}

func (m *Message) readers(containsSequence ContainsSequenceFunc) (readers []readFunc, _ error) {

	switch m.Type {
	case MsgTypeFullClient, MsgTypeFullServer, MsgTypeFrontEndResultServer:

	case MsgTypeAudioOnlyClient:
		if containsSequence == nil || containsSequence(m.TypeFlag()) {
			readers = append(readers, m.readSequence)
			if m.IsPrintLog {
				log.Info("AudioOnlyClient message: add Sequence reader.")
			}
		}

	case MsgTypeAudioOnlyServer:
		if containsSequence != nil && containsSequence(m.TypeFlag()) {
			readers = append(readers, m.readSequence)
			if m.IsPrintLog {
				log.Info("AudioOnlyServer message: add Sequence reader.")
			}
		}

	case MsgTypeError:
		readers = append(readers, m.readErrorCode)
		log.Info("Error message: add Error-Code reader.")

	default:
		return nil, fmt.Errorf("cannot deserialize message with invalid type: %d", m.Type)
	}

	if containsEvent(m.TypeFlag()) {
		readers = append(readers, m.readEvent, m.readSessionID, m.readConnectID)
		if m.IsPrintLog {
			log.Info("Add Event and SessionID readers.")
		}
	}

	readers = append(readers, m.readPayload)
	if m.IsPrintLog {
		log.Info("Add Payload reader.")
	}
	return readers, nil
}

func (m *Message) readEvent(buf *bytes.Buffer) error {
	if err := binary.Read(buf, binary.BigEndian, &m.Event); err != nil {
		return fmt.Errorf("%w: %v", ErrProtocolReadEvent, err)
	}
	if m.IsPrintLog {
		log.Infof("Read Event: %d", m.Event)
	}
	return nil
}

func (m *Message) readSessionID(buf *bytes.Buffer) error {
	switch m.Event {
	case 1, 2, 50, 51, 52: //StartConnection, FinishConnection, ConnectionStarted, ConnectionFailed, ConnectionFinished
		if m.IsPrintLog {
			log.Infof("Skip reading session ID for event: %d", m.Event)
		}
		return nil
	}

	var size uint32
	if err := binary.Read(buf, binary.BigEndian, &size); err != nil {
		return fmt.Errorf("%w: %v", ErrProtocolReadSessionIDSize, err)
	}
	if m.IsPrintLog {
		log.Infof("Read SessionID length: %d", size)
	}

	if size > 0 {
		m.SessionID = string(buf.Next(int(size)))
	}
	if m.IsPrintLog {
		log.Infof("Read SessionID content: %s", m.SessionID)
	}
	return nil
}

func (m *Message) readConnectID(buf *bytes.Buffer) error {
	switch m.Event {
	case 50, 51, 52: // ConnectionStarted, event.Type_ConnectionFailed, ConnectionFinished
	default:
		if m.IsPrintLog {
			log.Infof("Skip reading session ID for event: %d", m.Event)
		}
		return nil
	}

	var size uint32
	if err := binary.Read(buf, binary.BigEndian, &size); err != nil {
		return fmt.Errorf("%w: %v", ErrProtocolReadConnectIDSize, err)
	}
	if m.IsPrintLog {
		log.Infof("Read connection ID length: %d", size)
	}

	if size > 0 {
		m.ConnectID = string(buf.Next(int(size)))
	}
	if m.IsPrintLog {
		log.Infof("Read connection ID content: %s", m.ConnectID)
	}
	return nil
}

func (m *Message) readSequence(buf *bytes.Buffer) error {
	if err := binary.Read(buf, binary.BigEndian, &m.Sequence); err != nil {
		return fmt.Errorf("%w: %v", ErrProtocolReadSequence, err)
	}
	if m.IsPrintLog {
		log.Infof("Read Sequence: %d", m.Sequence)
	}
	return nil
}

func (m *Message) readErrorCode(buf *bytes.Buffer) error {
	if err := binary.Read(buf, binary.BigEndian, &m.ErrorCode); err != nil {
		return fmt.Errorf("%w: %v", ErrProtocolReadErrorCode, err)
	}
	if m.IsPrintLog {
		log.Infof("Read ErrorCode: %d", m.ErrorCode)
	}
	return nil
}

func (m *Message) readPayload(buf *bytes.Buffer) error {
	var size uint32
	if err := binary.Read(buf, binary.BigEndian, &size); err != nil {
		return fmt.Errorf("%w: %v", ErrProtocolReadPayloadSize, err)
	}
	if m.IsPrintLog {
		log.Infof("Read Payload length: %d", size)
	}

	if size > 0 {
		m.Payload = buf.Next(int(size))
	}
	if m.Type == MsgTypeFullClient || m.Type == MsgTypeFullServer || m.Type == MsgTypeError {
		if m.IsPrintLog {
			log.Infof("Read Payload content: %s", m.Payload)
		}
	}
	return nil
}

// ContainsSequence reports whether a message type specific flag indicates
// messages with this kind of flag contain a sequence number in its serialized
// value. This determiner function should be used for common binary protocol.
func ContainsSequence(bits MsgTypeFlagBits) bool {
	return bits&MsgTypeFlagPositiveSeq == MsgTypeFlagPositiveSeq || bits&MsgTypeFlagNegativeSeq == MsgTypeFlagNegativeSeq
}

func containsEvent(bits MsgTypeFlagBits) bool {
	return bits&MsgTypeFlagWithEvent == MsgTypeFlagWithEvent
}

func containsSequence(bits MsgTypeFlagBits) bool {
	return bits&MsgTypeFlagPositiveSeq == MsgTypeFlagPositiveSeq || bits&MsgTypeFlagNegativeSeq == MsgTypeFlagNegativeSeq
}

// BinaryProtocol implements the binary protocol serialization and deserialization
// used in Lab-Speech MDD, TTS, ASR, etc. services. For more details, read:
// https://bytedance.feishu.cn/docs/doccnT0t71J4LCQCS0cnB4Eca8D
type BinaryProtocol struct {
	versionAndHeaderSize        uint8
	serializationAndCompression uint8

	containsSequence ContainsSequenceFunc
	compress         CompressFunc
}

// NewBinaryProtocol returns a new BinaryProtocol instance.
func NewBinaryProtocol() *BinaryProtocol {
	return new(BinaryProtocol)
}

// Clone returns a clone of current BinaryProtocol
func (p *BinaryProtocol) Clone() *BinaryProtocol {
	clonedBinaryProtocal := new(BinaryProtocol)
	clonedBinaryProtocal.versionAndHeaderSize = p.versionAndHeaderSize
	clonedBinaryProtocal.serializationAndCompression = p.serializationAndCompression
	clonedBinaryProtocal.containsSequence = p.containsSequence
	clonedBinaryProtocal.compress = p.compress
	return clonedBinaryProtocal
}

// SetVersion sets the protocol version.
func (p *BinaryProtocol) SetVersion(v VersionBits) {
	// Clear the higher 4 bits in `p.versionAndHeaderSize` and reset them to `v`.
	p.versionAndHeaderSize = (p.versionAndHeaderSize &^ 0b11110000) + uint8(v)
}

// Version returns the integral version value.
func (p *BinaryProtocol) Version() int {
	return int(p.versionAndHeaderSize >> 4)
}

// SetHeaderSize sets the protocol header size.
func (p *BinaryProtocol) SetHeaderSize(s HeaderSizeBits) {
	// Clear the lower 4 bits in `p.versionAndHeaderSize` and reset them to `s`.
	p.versionAndHeaderSize = (p.versionAndHeaderSize &^ 0b00001111) + uint8(s)
}

// HeaderSize returns the protocol header size.
func (p *BinaryProtocol) HeaderSize() int {
	return 4 * int(p.versionAndHeaderSize&^0b11110000)
}

// SetSerialization sets the serialization method.
func (p *BinaryProtocol) SetSerialization(s SerializationBits) {
	// Clear the higher 4 bits in `p.serializationAndCompression` and reset them to `s`.
	p.serializationAndCompression = (p.serializationAndCompression &^ 0b11110000) + uint8(s)
}

// Serialization returns the bits value of protocol serialization method.
func (p *BinaryProtocol) Serialization() SerializationBits {
	return SerializationBits(p.serializationAndCompression &^ 0b00001111)
}

// SetCompression sets the compression method.
func (p *BinaryProtocol) SetCompression(c CompressionBits, f CompressFunc) {
	// Clear the lower 4 bits in `p.serializationAndCompression` and reset them to `c`.
	p.serializationAndCompression = (p.serializationAndCompression &^ 0b00001111) + uint8(c)
	p.compress = f
}

// Compression returns the bits value of protocol compression method.
func (p *BinaryProtocol) Compression() CompressionBits {
	return CompressionBits(p.serializationAndCompression &^ 0b11110000)
}

// Marshal serializes the message to a sequence of binary data.
func (p *BinaryProtocol) Marshal(msg *Message) ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := p.writeHeader(buf, msg); err != nil {
		return nil, fmt.Errorf("write header: %w", err)
	}

	writers, err := msg.writers(p.compress)
	if err != nil {
		return nil, err
	}
	for _, write := range writers {
		if err := write(buf); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (p *BinaryProtocol) writeHeader(buf *bytes.Buffer, msg *Message) error {
	return binary.Write(buf, binary.BigEndian, p.header(msg))
}

func (p *BinaryProtocol) header(msg *Message) []byte {
	header := []uint8{
		p.versionAndHeaderSize,
		msg.typeAndFlagBits,
		p.serializationAndCompression,
	}
	if padding := p.HeaderSize() - len(header); padding > 0 {
		header = append(header, make([]uint8, padding)...)
	}
	return header
}

// ==================== 事件响应结构定义 ====================

// ConnectionFailedResponse 连接失败响应
type ConnectionFailedResponse struct {
	Error string `json:"error"`
}

// SessionStartedResponse 会话开始响应
type SessionStartedResponse struct {
	DialogID string `json:"dialog_id"`
}

// SessionFailedResponse 会话失败响应
type SessionFailedResponse struct {
	Error string `json:"error"`
}

// UsageResponse 用量统计响应
type UsageResponse struct {
	Usage UsageInfo `json:"usage"`
}

// UsageInfo 用量详细信息
type UsageInfo struct {
	InputTextTokens   int `json:"input_text_tokens"`
	InputAudioTokens  int `json:"input_audio_tokens"`
	CachedTextTokens  int `json:"cached_text_tokens"`
	CachedAudioTokens int `json:"cached_audio_tokens"`
	OutputTextTokens  int `json:"output_text_tokens"`
	OutputAudioTokens int `json:"output_audio_tokens"`
}

// TTSSentenceStartResponse TTS合成音频起始事件响应
type TTSSentenceStartResponse struct {
	TTSType    string `json:"tts_type"`
	Text       string `json:"text"`
	QuestionID string `json:"question_id"`
	ReplyID    string `json:"reply_id"`
}

// TTSSentenceEndResponse TTS合成音频分句结束事件响应
type TTSSentenceEndResponse struct {
	SentenceText string `json:"sentence_text"`
	Text         string `json:"text"` // Fallback
	TTSType      string `json:"tts_type"`
	TTSTaskID    string `json:"tts_task_id"`
	QuestionID   string `json:"question_id"`
	ReplyID      string `json:"reply_id"`
}

// TTSEndedResponse TTS合成结束事件响应
type TTSEndedResponse struct {
	QuestionID string `json:"question_id"`
	ReplyID    string `json:"reply_id"`
}

// ASRInfoResponse ASR首字识别响应（用于打断）
type ASRInfoResponse struct {
	QuestionID string `json:"question_id"`
}

// ASRResponse ASR识别结果响应
type ASRResponse struct {
	Results    []ASRResult `json:"results"`
	QuestionID string      `json:"question_id"`
}

// ASRResult ASR识别结果项
type ASRResult struct {
	Text      string `json:"text"`
	IsInterim bool   `json:"is_interim"`
}

// ChatResponse 聊天回复响应
type ChatResponse struct {
	Content    string `json:"content"`
	QuestionID string `json:"question_id"`
	ReplyID    string `json:"reply_id"`
}

// ChatTextQueryConfirmedResponse 文本查询确认响应
type ChatTextQueryConfirmedResponse struct {
	QuestionID string `json:"question_id"`
}

// ChatEndedResponse 聊天结束响应
type ChatEndedResponse struct {
	QuestionID string `json:"question_id"`
	ReplyID    string `json:"reply_id"`
}

// ConversationItem 对话上下文项
type ConversationItem struct {
	ItemID    string `json:"item_id"`
	Role      string `json:"role"`
	Text      string `json:"text"`
	Timestamp int64  `json:"timestamp"`
}

// ConversationCreatedResponse 上下文创建响应
type ConversationCreatedResponse struct {
	Items []ConversationItem `json:"items"`
}

// ConversationUpdatedResponse 上下文更新响应
type ConversationUpdatedResponse struct {
	Message string `json:"message"` // 更新失败时返回错误信息，成功时为空
}

// ConversationRetrievedResponse 上下文查询响应
type ConversationRetrievedResponse struct {
	Items []ConversationItem `json:"items"`
}

// ConversationDeletedResponse 上下文删除响应
type ConversationDeletedResponse struct {
	StatusCode int                `json:"status_code,omitempty"`
	Message    string             `json:"message,omitempty"`
	Items      []ConversationItem `json:"items,omitempty"`
}

// DialogCommonErrorResponse 对话通用错误响应
type DialogCommonErrorResponse struct {
	StatusCode int    `json:"status_code"`
	Message    string `json:"message"`
}
