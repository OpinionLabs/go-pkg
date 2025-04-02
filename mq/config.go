package mq

type SNSConfig struct {
	ARN           string `mapstructure:"arn" json:"arn"`
	Region        string `mapstructure:"region" json:"region"`
	APIKey        string `mapstructure:"api_key" json:"api_key"`
	SecretKey     string `mapstructure:"secret_key" json:"secret_key"`
	ProducerCnt   int    `mapstructure:"producer_cnt" json:"producer_cnt"`
}

type SQSConfig struct {
	ARN           string `mapstructure:"arn" json:"arn"`
	Region        string `mapstructure:"region" json:"region"`
	APIKey        string `mapstructure:"api_key" json:"api_key"`
	SecretKey     string `mapstructure:"secret_key" json:"secret_key"`
	QueueUrl      string `mapstructure:"queue_url" json:"queue_url"`
	ConsumerCnt   int    `mapstructure:"consumer_cnt" json:"consumer_cnt"`
}
