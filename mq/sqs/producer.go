package sqs

import (
	"context"
	"fmt"
	"time"

	"github.com/ChewZ-life/go-pkg/mq/channel"
	"github.com/ChewZ-life/go-pkg/mq/utils/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
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

// Producer 生产者
type Producer struct {
	config   SQSConfig                // 配置
	logger   *log.Log                 // 日志
	msgChans map[int]chan interface{} // 接收消息
}

func NewProducer(sqsConfig SQSConfig, logger *log.Log) *Producer {
	p := &Producer{
		config:   sqsConfig,
		logger:   logger,
		msgChans: map[int]chan interface{}{},
	}

	for i := 0; i < sqsConfig.ProducerCnt; i++ {
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
	var service *sqs.SQS
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
					err = errors.Wrap(err, "sqs Producer.processMessages session")
					p.logger.ErrorWithFields("sqs Producer.processMessages session", log.Fields{"err": err.Error()})
					return
				}
			}

			if service == nil {
				service = sqs.New(cfgSession)
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
				err = errors.Wrap(err, "sqs Producer.processMessages marshal")
				p.logger.ErrorWithFields("sqs Producer.processMessages marshal", log.Fields{"err": err.Error()})
				return
			}

			tp := time.Now()

			const waitSeconds = 5
			ctx, cancel := context.WithTimeout(context.Background(), waitSeconds*time.Second)
			defer cancel()
			input := &sqs.SendMessageInput{
				QueueUrl:       aws.String(p.config.QueueUrl),
				MessageGroupId: p.config.MessageGroupId,
				MessageBody:    aws.String(string(msgData)),
			}
			_, err := service.SendMessageWithContext(ctx, input)
			if err != nil {
				err = errors.Wrap(err, "sqs Producer.processMessages send")
				p.logger.ErrorWithFields("sqs Producer.processMessages send", log.Fields{"snsArn": p.config.ARN, "err": err.Error()})
				return
			}
			// fmt.Println("sqs Message ID:", *result.MessageId)

			cost := time.Since(tp).Milliseconds()
			if cost > TimeoutMS {
				p.logger.ErrorWithFields("sqs processMessages handle msg cost.", log.Fields{"sqsArn": p.config.ARN, "cost": cost})
			}
			p.logger.Infof("sqs Producer.processMessages pub end. msg:%s \n", string(msgData))
		}()
	}
}
