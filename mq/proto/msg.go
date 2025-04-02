package proto

type SNSEventDataType string

const (
	// SNSEventDataTypePositionOpen Position opening event
	SNSEventDataTypePositionOpen SNSEventDataType = "positionOpen"
	// SNSEventDataTypePositionUpdate Position update, added 20220321: supports copy trading
	SNSEventDataTypePositionUpdate SNSEventDataType = "positionUpdate"
	// SNSEventDataTypePositionClose Position closing event
	SNSEventDataTypePositionClose SNSEventDataType = "positionClose"
	// SNSEventDataTypeOrderNew New limit order event
	SNSEventDataTypeOrderNew SNSEventDataType = "orderNew"
	// SNSEventDataTypeOrderFill Limit order fill event
	SNSEventDataTypeOrderFill SNSEventDataType = "orderFill"
	// SNSEventDataTypeOrderCancel Limit order cancellation event
	SNSEventDataTypeOrderCancel SNSEventDataType = "orderCancel"
	// SNSEventDataTypeOrderUpdate Order update event
	SNSEventDataTypeOrderUpdate SNSEventDataType = "orderUpdate"
)

// SNSEventMessage AWS SNS notification event message format
type SNSEventMessage struct {
	WalletAddress       string `json:"wallet_address"`  // Unique request ID
	WalletUser      	string `json:"wallet_user"`     // Request user ID
	DataType 			string `json:"dataType"` 		// Event type
	Data     			string `json:"data"`     		// Data structure
	Time     			int64  `json:"time"`     		// Timestamp in milliseconds
}

type SQSMsg struct {
	UserID       int64  `json:"uid"`
	BizType      int    `json:"bizType"`
	ExchangeName string `json:"exchangeName"`
	Time         int64  `json:"time"`
	DataType     string `json:"dataType"`
	OriginData   string `json:"originData"`
}
