package sqs

import (
	"context"
	"strings"
	"time"

	"github.com/ChewZ-life/go-pkg/mq/utils/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
)

const (
	HandleTimeoutMS = int64(1000)
)

// MessageCB
type MessageCB func(msg string) error

// SQS AWS SQS wrapper
type SQS struct {
	config    SQSConfig
	logger    *log.Log
	messageCB MessageCB
}

// SQSConfig AWS SQS related configuration
type SQSConfig struct {
	ARN            string  `mapstructure:"arn" json:"arn"`
	Region         string  `mapstructure:"region" json:"region"`
	APIKey         string  `mapstructure:"api_key" json:"api_key"`
	SecretKey      string  `mapstructure:"secret_key" json:"secret_key"`
	QueueUrl       string  `mapstructure:"queue_url" json:"queue_url"`
	MessageGroupId *string `mapstructure:"message_group_id" json:"message_group_id"`
	ConsumerCnt    int     `mapstructure:"consumer_cnt" json:"consumer_cnt"`
	ProducerCnt    int     `mapstructure:"producer_cnt" json:"producer_cnt"`
}

// Process sns->sqs messages
func NewSQS(sqsConfig SQSConfig, logger *log.Log, messageCB MessageCB) *SQS {
	s := &SQS{
		config:    sqsConfig,
		logger:    logger,
		messageCB: messageCB,
	}

	for i := 0; i < sqsConfig.ConsumerCnt; i++ {
		go s.processMessages(i)
	}

	return s
}

// Process sqs->sqs messages
func NewSQSV1(sqsConfig SQSConfig, logger *log.Log, messageCB MessageCB) *SQS {
	s := &SQS{
		config:    sqsConfig,
		logger:    logger,
		messageCB: messageCB,
	}

	for i := 0; i < sqsConfig.ConsumerCnt; i++ {
		go s.processMessagesV1(i)
	}

	return s
}

func (s *SQS) processMessages(i int) {
	s.logger.Infof("sqs SQS.processMessages start. task_id:%d", i)

	var cfgSession *session.Session
	var service *sqs.SQS
	var err error
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	for {
		func() {
			if cfgSession == nil {
				cfg := new(aws.Config)
				if s.config.APIKey != "" && s.config.SecretKey != "" {
					cfg = &aws.Config{
						Region: aws.String(s.config.Region),
						Credentials: credentials.NewStaticCredentials(
							s.config.APIKey, s.config.SecretKey, ""),
					}
				} else {
					cfg = &aws.Config{
						Region: aws.String(s.config.Region),
					}
				}

				cfgSession, err = session.NewSession(cfg)
				if err != nil {
					err = errors.Wrap(err, "sqs SQS.processMessages session")
					s.logger.ErrorWithFields("sqs SQS.processMessages session", log.Fields{"err": err.Error()})
					return
				}
				s.logger.Info("sqs SQS.processMessages session init success")
			}

			if service == nil {
				service = sqs.New(cfgSession)
			}

			// Fetch messages
			const waitSeconds = 20
			const messageCount = 10
			ctx, cancel := context.WithTimeout(context.Background(), (waitSeconds+1)*time.Second)
			defer cancel()

			msgResult, err := service.ReceiveMessageWithContext(ctx,
				&sqs.ReceiveMessageInput{
					QueueUrl:            aws.String(s.config.QueueUrl),
					MaxNumberOfMessages: aws.Int64(messageCount),
					WaitTimeSeconds:     aws.Int64(waitSeconds),
				})
			if err != nil {
				s.logger.ErrorWithFields("sqs SQS.processMessages receive message fail.", log.Fields{"err": err.Error()})
				return
			}

			if len(msgResult.Messages) == 0 {
				return
			}

			// Process messages
			var deleteEntries []*sqs.DeleteMessageBatchRequestEntry
			for _, msg := range msgResult.Messages {
				if msg.Body == nil {
					continue
				}

				rawMessage := &struct {
					Message   string `json:"Message"`
					Timestamp string `json:"Timestamp"`
				}{}
				if err = json.Unmarshal([]byte(*msg.Body), rawMessage); err != nil {
					// If message deserialization fails, treat it as an invalid message and delete it
					deleteEntries = append(deleteEntries, &sqs.DeleteMessageBatchRequestEntry{
						Id:            msg.MessageId,
						ReceiptHandle: msg.ReceiptHandle,
					})
					s.logger.ErrorWithFields("sqs SQS.processMessages unmarshal raw message fail.", log.Fields{"err": err.Error(), "msg": *msg.Body})
					continue
				}

				// Process callback result
				if s.messageCB != nil {
					tp := time.Now()
					err = s.messageCB(rawMessage.Message)
					if err != nil {
						s.logger.ErrorWithFields("sqs SQS.processMessages handle msg fail.", log.Fields{"err": err.Error(), "msg": rawMessage.Message})
						continue
					}
					cost := time.Since(tp).Milliseconds()
					if cost > HandleTimeoutMS {
						s.logger.ErrorWithFields("sqs SQS.processMessages handle msg cost.", log.Fields{"sqsArn": s.config.ARN, "cost": cost})
					}
					// Delete message after successful callback
					deleteEntries = append(deleteEntries, &sqs.DeleteMessageBatchRequestEntry{
						Id:            msg.MessageId,
						ReceiptHandle: msg.ReceiptHandle,
					})
				}
			}

			if len(deleteEntries) == 0 {
				return
			}

			// Delete messages
			ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
			defer cancel2()

			_, err = service.DeleteMessageBatchWithContext(ctx2,
				&sqs.DeleteMessageBatchInput{
					Entries:  deleteEntries,
					QueueUrl: &s.config.QueueUrl,
				})
			if err != nil {
				s.logger.ErrorWithFields("sqs SQS.processMessages delete message fail.", log.Fields{"error": err.Error()})
				return
			}
		}()
	}
}

func (s *SQS) processMessagesV1(i int) {
	s.logger.Infof("sqs SQS.processMessagesV1 start. task_id:%d", i)

	var cfgSession *session.Session
	var service *sqs.SQS
	var err error
	for {
		func() {
			if cfgSession == nil {
				cfg := new(aws.Config)
				if s.config.APIKey != "" && s.config.SecretKey != "" {
					cfg = &aws.Config{
						Region: aws.String(s.config.Region),
						Credentials: credentials.NewStaticCredentials(
							s.config.APIKey, s.config.SecretKey, ""),
					}
				} else {
					cfg = &aws.Config{
						Region: aws.String(s.config.Region),
					}
				}
				cfgSession, err = session.NewSession(cfg)
				if err != nil {
					err = errors.Wrap(err, "sqs SQS.processMessagesV1 session")
					s.logger.ErrorWithFields("sqs SQS.processMessagesV1 session", log.Fields{"err": err.Error()})
					return
				}
				s.logger.Info("sqs SQS.processMessagesV1 session init success")
			}

			if service == nil {
				service = sqs.New(cfgSession)
			}

			// Fetch messages
			const waitSeconds = 5
			const messageCount = 10
			ctx, cancel := context.WithTimeout(context.Background(), (waitSeconds+1)*time.Second)
			defer cancel()

			msgResult, err := service.ReceiveMessageWithContext(ctx,
				&sqs.ReceiveMessageInput{
					QueueUrl:            aws.String(s.config.QueueUrl),
					MaxNumberOfMessages: aws.Int64(messageCount),
					WaitTimeSeconds:     aws.Int64(waitSeconds),
				})
			if err != nil {
				if !strings.Contains(err.Error(), "context deadline exceeded") {
					s.logger.ErrorWithFields("sqs SQS.processMessagesV1 receive message fail.", log.Fields{"err": err.Error()})
				}
				return
			}

			if len(msgResult.Messages) == 0 {
				return
			}

			// s.logger.InfoWithFields("sqs SQS.processMessagesV1 received messages", log.Fields{"len": len(msgResult.Messages)})

			// Process messages
			var deleteEntries []*sqs.DeleteMessageBatchRequestEntry
			for _, msg := range msgResult.Messages {
				if msg.Body == nil {
					continue
				}

				// s.logger.InfoWithFields("sqs SQS.processMessagesV1 handle message", log.Fields{"index": i, "msg": *msg})

				// Process callback result
				if s.messageCB != nil {
					tp := time.Now()
					err = s.messageCB(*msg.Body)
					if err != nil {
						s.logger.ErrorWithFields("sqs SQS.processMessagesV1 handle msg fail.", log.Fields{"err": err.Error(), "msg": *msg.Body})
						continue
					}
					cost := time.Since(tp).Milliseconds()
					if cost > HandleTimeoutMS {
						s.logger.ErrorWithFields("sqs SQS.processMessagesV1 handle msg cost.", log.Fields{"sqsArn": s.config.ARN, "cost": cost})
					}
					// Delete message after successful callback
					deleteEntries = append(deleteEntries, &sqs.DeleteMessageBatchRequestEntry{
						Id:            msg.MessageId,
						ReceiptHandle: msg.ReceiptHandle,
					})
				}
			}

			if len(deleteEntries) == 0 {
				return
			}

			// Delete messages
			ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
			defer cancel2()

			_, err = service.DeleteMessageBatchWithContext(ctx2,
				&sqs.DeleteMessageBatchInput{
					Entries:  deleteEntries,
					QueueUrl: &s.config.QueueUrl,
				})
			if err != nil {
				s.logger.ErrorWithFields("sqs SQS.processMessagesV1 delete message fail.", log.Fields{"error": err.Error()})
				return
			}
		}()
	}
}
