package doubao

// EventNameMap maps event IDs to human-readable names for debugging
var EventNameMap = map[int32]string{
	// Client events - 客户端事件
	EVENT_START_CONNECTION:      "创建连接",  // Websocket阶段声明创建连接
	EVENT_FINISH_CONNECTION:     "断开连接",  // 断开websocket连接，后面需要重新发起websocket连接
	EVENT_START_SESSION:         "开始会话",  // Websocket阶段声明创建会话
	EVENT_FINISH_SESSION:        "结束会话",  // 客户端声明结束会话，后面可以复用websocket连接
	EVENT_TASK_REQUEST:          "上传音频",  // 客户端上传音频二进制数据
	EVENT_SAY_HELLO:             "打招呼",   // 客户端提交打招呼文本
	EVENT_CHAT_TTS_TEXT:         "文本合成",  // 指定文本合成音频
	EVENT_CHAT_TEXT_QUERY:       "文本提问",  // 用户输入文本query进行提问
	EVENT_CHAT_RAG_TEXT:         "外部知识",  // 输入外部RAG知识进行总结和口语化改写
	EVENT_CONVERSATION_CREATE:   "创建上下文", // 上下文追加，每次仅允许提交一条问答记录
	EVENT_CONVERSATION_UPDATE:   "更新上下文", // 更新指定item_id对应消息的文本内容
	EVENT_CONVERSATION_RETRIEVE: "查询上下文", // 查询对话上下文记录
	EVENT_CONVERSATION_DELETE:   "删除上下文", // 删除上下文，以对话轮为单位

	// Server events - 服务端事件
	EVENT_CONNECTION_STARTED:        "连接成功",   // 成功建立连接
	EVENT_CONNECTION_FAILED:         "连接失败",   // 建立连接失败，返回错误信息
	EVENT_CONNECTION_FINISHED:       "连接结束",   // 连接结束
	EVENT_SESSION_STARTED:           "会话开始",   // 成功启动会话，返回dialog_id
	EVENT_SESSION_FINISHED:          "会话结束",   // 会话已结束
	EVENT_SESSION_FAILED:            "会话失败",   // 会话失败，返回错误信息
	EVENT_USAGE_INFO:                "用量统计",   // 每一轮交互对应的用量信息
	EVENT_TTS_SENTENCE_START:        "音频合成开始", // 合成音频的起始事件
	EVENT_TTS_SENTENCE_END:          "音频分句结束", // 合成音频的分句结束事件
	EVENT_TTS_RESPONSE:              "音频数据",   // 返回模型生成的音频数据
	EVENT_TTS_ENDED:                 "音频合成结束", // 模型一轮音频合成结束事件
	EVENT_ASR_STARTED:               "识别开始",   // 识别出音频流中的首字，用于打断播报
	EVENT_ASR_RESPONSE:              "识别结果",   // 模型识别出用户说话的文本内容
	EVENT_ASR_ENDED:                 "识别结束",   // 模型认为用户说话结束
	EVENT_CHAT_RESPONSE:             "文本回复",   // 模型回复的文本内容
	EVENT_CHAT_TEXT_QUERY_CONFIRMED: "文本确认",   // ChatTextQuery请求确认
	EVENT_CHAT_ENDED:                "回复结束",   // 模型回复文本结束事件
	EVENT_CONVERSATION_CREATED:      "上下文已创建", // 增加上下文请求确认
	EVENT_CONVERSATION_UPDATED:      "上下文已更新", // 更新上下文请求确认
	EVENT_CONVERSATION_RETRIEVED:    "上下文已查询", // 查询上下文请求确认
	EVENT_CONVERSATION_DELETED:      "上下文已删除", // 删除上下文请求确认
	EVENT_DIALOG_COMMON_ERROR:       "对话错误",   // 实时通话过程中相关错误描述
}

// GetEventName returns the event name for a given event ID
func GetEventName(eventID int32) string {
	if name, ok := EventNameMap[eventID]; ok {
		return name
	}
	return "UnknownEvent"
}
