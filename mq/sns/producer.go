package sns

import (
	"context"
	"crypto/md5"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ChewZ-life/go-pkg/mq/channel"
	"github.com/ChewZ-life/go-pkg/mq/utils/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
)

const (
	TimeoutMS = int64(1000)
)

type keyValueReq struct {
	key   string
	value string
	errCh chan error
}

// SNSConfig AWS SNS related configuration
type SNSConfig struct {
	ARN         string `mapstructure:"arn" json:"arn"`             // Topic ARN
	Region      string `mapstructure:"region" json:"region"`       // Queue service region
	APIKey      string `mapstructure:"api_key" json:"api_key"`     // API key
	SecretKey   string `mapstructure:"secret_key" json:"secret_key"` // Secret key
	ProducerCnt int    `mapstructure:"producer_cnt" json:"producer_cnt"` // Number of producers
}

// Producer Message producer
type Producer struct {
	config   SNSConfig                // Configuration
	logger   *log.Log                 // Logger
	msgChans map[int]chan interface{} // Message channels
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

func (p *Producer) GetUserShard(key string) int {
	md5str1 := fmt.Sprintf("%x", md5.Sum([]byte(key)))
	ret, _ := strconv.ParseInt(md5str1[22:], 16, 0)
	return int(ret % int64(p.config.ProducerCnt))
}

func (p *Producer) Pub(key, value string) error {
	shard := p.GetUserShard(key)
	errCh := make(chan error)
	p.msgChans[shard] <- keyValueReq{
		key:   key,
		value: value,
		errCh: errCh,
	}
	return <-errCh
}

func (p *Producer) processMessages(i int, keyValueCh chan interface{}) {
	p.logger.Infof("sns Producer.processMessages start. task_id:%d", i)

	var cfgSession *session.Session
	var service *sns.SNS
	var err error
	var json = jsoniter.ConfigCompatibleWithStandardLibrary

	for {
		msg := <-keyValueCh

		func() {
			defer func() { msg.(keyValueReq).errCh <- err }()

			if cfgSession == nil {
				cfg := new(aws.Config)
				if p.config.APIKey != "" && p.config.SecretKey != "" {
					cfg = &aws.Config{
						Region: aws.String(p.config.Region),
						Credentials: credentials.NewStaticCredentials(
							p.config.APIKey, p.config.SecretKey, ""),
					}
				} else {
					cfg = &aws.Config{
						Region: aws.String(p.config.Region),
					}
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
			_, err := service.PublishWithContext(ctx, input)
			if err != nil {
				err = errors.Wrap(err, "sns Producer.processMessages send")
				p.logger.ErrorWithFields("sns Producer.processMessages send", log.Fields{"snsArn": p.config.ARN, "err": err.Error()})
				return
			}
			// fmt.Println("Message ID:", *result.MessageId)

			cost := time.Since(tp).Milliseconds()
			if cost > TimeoutMS {
				p.logger.ErrorWithFields("sqs SNS.processMessages handle msg cost.", log.Fields{"sqsArn": p.config.ARN, "cost": cost})
			}
			// p.logger.Infof("sqs Producer.processMessages pub end. msg:%s \n", string(msgData))
		}()
	}
}
