package mq

// SNSConfig aws sns相关配置
type SNSConfig struct {
	ARN           string `mapstructure:"arn" json:"arn"`                       // topic的arn
	Region        string `mapstructure:"region" json:"region"`                 // 队列服务所属区域
	APIKey        string `mapstructure:"api_key" json:"api_key"`               // api key
	SecretKey     string `mapstructure:"secret_key" json:"secret_key"`         // secret key
	ProducerCnt   int    `mapstructure:"producer_cnt" json:"producer_cnt"`     // 生产者
}

// SQSConfig aws sqs相关配置
type SQSConfig struct {
	ARN           string `mapstructure:"arn" json:"arn"`                       // topic的arn
	Region        string `mapstructure:"region" json:"region"`                 // 队列服务所属区域
	APIKey        string `mapstructure:"api_key" json:"api_key"`               // api key
	SecretKey     string `mapstructure:"secret_key" json:"secret_key"`         // secret key
	QueueUrl      string `mapstructure:"queue_url" json:"queue_url"`           // 队列地址
	ConsumerCnt   int    `mapstructure:"consumer_cnt" json:"consumer_cnt"`     // 连接数量
}
