package sns

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"github.com/ChewZ-life/go-pkg/mq/channel"
	"github.com/ChewZ-life/go-pkg/mq/utils/log"
)

const (
	TimeoutMS = int64(1000)
)

type keyValueReq struct {
	key   string
	value string
	errCh chan error
}

// SNSConfig aws sns相关配置
type SNSConfig struct {
	ARN           string `mapstructure:"arn" json:"arn"`                       // topic的arn
	Region        string `mapstructure:"region" json:"region"`                 // 队列服务所属区域
	APIKey        string `mapstructure:"api_key" json:"api_key"`               // api key
	SecretKey     string `mapstructure:"secret_key" json:"secret_key"`         // secret key
	ProducerCnt   int    `mapstructure:"producer_cnt" json:"producer_cnt"`     // 生产者
}

// Producer 生产者
type Producer struct {
	config   SNSConfig             // 配置
	logger   *log.Log            // 日志
	msgChans map[int]chan interface{} // 接收消息
	isFifo   bool
}

func NewProducer(snsConfig SNSConfig, logger *log.Log) *Producer {
	p := &Producer{
		config:   snsConfig,
		logger:   logger,
		msgChans: map[int]chan interface{}{},
		isFifo:   strings.HasSuffix(snsConfig.ARN, ".fifo"),
	}

	for i := 0; i < snsConfig.ProducerCnt; i++ {
		p.msgChans[i] = make(chan interface{})
		keyValueCh := channel.NoBlock(p.msgChans[i])
		go func(i int, keyValueCh chan interface{}) {
			go p.processMessages(i, keyValueCh)
		}(i, keyValueCh)
	}

	return p
}

func (p *Producer) Pub(key, value string) error {
	shard := 0 // 后续需要再处理-rogan
	errCh := make(chan error)
	p.msgChans[shard] <- keyValueReq{
		key:   key,
		value: value,
		errCh: errCh,
	}
	return <-errCh
}

func (p *Producer) processMessages(i int, keyValueCh chan interface{}) {
	p.logger.Infof("sqs Producer.processMessages start. task_id:%d", i)

	var cfgSession *session.Session
	var service *sns.SNS
	var err error
	var json = jsoniter.ConfigCompatibleWithStandardLibrary

	for {
		msg := <-keyValueCh

		func() {
			defer func() { msg.(keyValueReq).errCh <- err }()

			if cfgSession == nil {
				cfg := &aws.Config{
					Region: aws.String(p.config.Region),
					Credentials: credentials.NewStaticCredentials(
						p.config.APIKey, p.config.SecretKey, ""),
				}
				cfgSession, err = session.NewSession(cfg)
				if err != nil {
					err = errors.Wrap(err, "sns Producer.processMessages session")
					p.logger.ErrorWithFields("sns Producer.processMessages session", log.Fields{"err": err.Error()})
					return
				}
			}

			if service == nil {
				service = sns.New(cfgSession)
			}

			var msgData []byte
			msgInfo := &struct {
				MsgID            string `json:"msgId"`
				BornTimestamp    int64  `json:"bornTimestamp"`
				ReceiveTimestamp int64  `json:"receiveTimestamp"`
				Data             string `json:"data"`
			}{
				MsgID:         fmt.Sprint(time.Now().UnixNano()),
				BornTimestamp: time.Now().UnixNano() / int64(time.Millisecond),
				Data:          msg.(keyValueReq).value,
			}
			msgData, err = json.Marshal(msgInfo)
			if err != nil {
				err = errors.Wrap(err, "sns Producer.processMessages marshal")
				p.logger.ErrorWithFields("sns Producer.processMessages marshal", log.Fields{"err": err.Error()})
				return
			}

			tp := time.Now()

			const waitSeconds = 3
			ctx, cancel := context.WithTimeout(context.Background(), waitSeconds*time.Second)
			defer cancel()
			input := &sns.PublishInput{
				Message:  aws.String(string(msgData)),
				TopicArn: aws.String(p.config.ARN),
			}
			if p.isFifo {
				msgKey := msg.(keyValueReq).key
				input.MessageGroupId = aws.String(msgKey)
			}
			result, err := service.PublishWithContext(ctx, input)
			if err != nil {
				err = errors.Wrap(err, "sns Producer.processMessages send")
				p.logger.ErrorWithFields("sns Producer.processMessages send", log.Fields{"snsArn": p.config.ARN, "err": err.Error()})
				return
			}
			fmt.Println("Message ID:", *result.MessageId)

			cost := time.Since(tp).Milliseconds()
			if cost > TimeoutMS {
				p.logger.ErrorWithFields("sqs SNS.processMessages handle msg cost.", log.Fields{"sqsArn": p.config.ARN, "cost":cost})
			}
			p.logger.Infof("sqs Producer.processMessages pub end. msg:%s \n", string(msgData))
		}()
	}
}
