package main

import (
	"fmt"
	"time"
	"encoding/json"

	"github.com/ChewZ-life/go-pkg/mq/utils/log"
	"github.com/ChewZ-life/go-pkg/mq/sqs"
	"github.com/ChewZ-life/go-pkg/mq/sns"
	"github.com/ChewZ-life/go-pkg/mq/proto"
)

// 定义结构体与字段 for test
type OderInfo struct {
    TopicID         int     `json:"topicId"`
    ContractAddress string  `json:"contractAddress"`
    Price           string  `json:"price"`
    TradingMethod   int     `json:"tradingMethod"`
    OrderId 		int64 	`json:"orderId"`
}


func testPubMsg(producer *sns.Producer, logger *log.Log) {
	i := 1
    for {
    	position := OderInfo{
        	TopicID:         55,
        	Price:           "0.5",
        	TradingMethod:   2,
        	OrderId:111,
    	}
    	jsonData, err := json.Marshal(position)
    	if err != nil {
    		logger.ErrorWithFields("pushMsg sns pub fail", log.Fields{"err": err.Error()})
    	}

	  	walletAddress := "abcdefg"
    	snsMsg := proto.SNSEventMessage{
			WalletAddress:	walletAddress,
			WalletUser:	"bbbb",
			DataType:	"orderNew",
			Data:	string(jsonData),
			Time:	time.Now().UnixMilli(),
    	}

    	msg, _ := json.Marshal(snsMsg)
		err = producer.Pub(fmt.Sprint(walletAddress), string(msg))
		if err != nil {
			logger.ErrorWithFields("pushMsg sns pub fail", log.Fields{"err": err.Error()})
			continue
		}

		time.Sleep(2 * time.Second) // 休眠 2 秒
		i++
		if i > 10 {
			break
		}
    }
}

func startSubscribeSQS(sqs_config sqs.SQSConfig, logger *log.Log) {

	_ = sqs.NewSQS(sqs_config, logger, func(msg string) error {
		logger.Info("receive msg start")
		msgInfo := &struct {
			MsgID            string `json:"msgId"`
			BornTimestamp    int64  `json:"bornTimestamp"`
			ReceiveTimestamp int64  `json:"receiveTimestamp"`
			Data             string `json:"data"`
		}{}
		if err := json.Unmarshal([]byte(msg), msgInfo); err != nil {
			logger.ErrorWithFields("unmarshal sqs msg fail", log.Fields{"err": err.Error(), "msg":string(msg)})
			return err
		}

		var sqsMsg proto.SQSMsg
		err := json.Unmarshal([]byte(msgInfo.Data), &sqsMsg)
		if err != nil {
			logger.ErrorWithFields("unmarshal sqs msg fail", log.Fields{"err": err.Error(), "msg":string(msg)})
			return err
		}

		logger.Infof("receive msg success. msg:%s \n", string(msg))
		return nil
	})
}

func main(){
	sns_config := sns.SNSConfig{
		ARN: "",
		Region:"ap-southeast-1",
		APIKey:"",
		SecretKey:"",
		ProducerCnt:10,
	}


	sqs_config := sqs.SQSConfig{
		ARN: "",
		Region:"ap-southeast-1",
		APIKey:"",
		SecretKey:"",
		QueueUrl:"",
		ConsumerCnt:10,
	}

	logger, err := log.NewLog("sns_sqs", "system", "", 0)
	if err != nil {
		fmt.Println("init log error:", err.Error())
	}

	sns_producer := sns.NewProducer(sns_config, logger)

	// testPubMsg(sns_producer, lg)
	go testPubMsg(sns_producer, logger)
	startSubscribeSQS(sqs_config, logger)

	select {}
}

