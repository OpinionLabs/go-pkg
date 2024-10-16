package main

import (
	"context"
	"log"

	"github.com/ChewZ-life/go-pkg/database/dynamo"
)

type Item struct {
	OrderID  int64  `dynamodbav:"order_id"`
	UserID   int64  `dynamodbav:"user_id"`
	NumAttr1 int    `dynamodbav:"num_attr1"`
	NumAttr2 int    `dynamodbav:"num_attr2"`
	StrAttr1 string `dynamodbav:"str_attr1"`
	StrAttr2 string `dynamodbav:"str_attr2"`
}

func main() {
	tableName := "tb_test"
	cfg := dynamo.Config{
		Region:    "ap-southeast-1",
		APIKey:    "",
		SecretKey: "",
		PoolSize:  1,
		TableName: tableName,
	}

	insertInfos := []dynamo.TxInsertInfo[Item]{
		{
			Item: Item{
				OrderID:  123,
				UserID:   234,
				NumAttr1: 11,
				StrAttr1: "aa",
			},
		},
		{
			Item: Item{
				OrderID:  321,
				UserID:   432,
				NumAttr2: 11,
				StrAttr2: "aa",
			},
		},
	}
	d := dynamo.NewDynamo[Item](cfg)
	for i := 0; i < 10; i++ {
		err := d.TxInsertItems(context.TODO(), insertInfos)
		log.Println("index:", i, ", err:", err)
	}
}
