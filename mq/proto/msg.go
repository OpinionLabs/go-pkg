package proto

type SNSEventDataType string

const (
	// SNSEventDataTypePositionOpen 开仓事件
	SNSEventDataTypePositionOpen SNSEventDataType = "positionOpen"
	// SNSEventDataTypePositionUpdate 仓位更新, 20220321新增: 支持跟单
	SNSEventDataTypePositionUpdate SNSEventDataType = "positionUpdate"
	// SNSEventDataTypePositionClose 平仓事件
	SNSEventDataTypePositionClose SNSEventDataType = "positionClose"
	// SNSEventDataTypeOrderNew 新的限价单事件
	SNSEventDataTypeOrderNew SNSEventDataType = "orderNew"
	// SNSEventDataTypeOrderFill 限价单成交事件
	SNSEventDataTypeOrderFill SNSEventDataType = "orderFill"
	// SNSEventDataTypeOrderCancel 限价单撤销事件
	SNSEventDataTypeOrderCancel SNSEventDataType = "orderCancel"
	// SNSEventDataTypeOrderUpdate 
	SNSEventDataTypeOrderUpdate SNSEventDataType = "orderUpdate"

)

// SNSEventMessage aws sns发送通知事件消息格式
type SNSEventMessage struct {
	WalletAddress       string `json:"wallet_address"`  // 请求唯一id
	WalletUser      	string `json:"wallet_user"`     // 请求用户id
	DataType 			string `json:"dataType"` 		// 事件类型
	Data     			string `json:"data"`     		// 数据结构体
	Time     			int64  `json:"time"`     		// 毫秒时间戳
}

type SQSMsg struct {
	UserID       int64  `json:"uid"`
	BizType      int    `json:"bizType"`
	ExchangeName string `json:"exchangeName"`
	Time         int64  `json:"time"`
	DataType     string `json:"dataType"`
	OriginData   string `json:"originData"`
}