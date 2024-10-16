package main

import (
	"context"
	"log"

	"github.com/ChewZ-life/go-pkg/database/dynamo"
)

type Item struct {
        WalletUser    string `json:"wallet_user"`
        Points    	  int    `json:"points"`
        Version       int    `json:"version"`
        UpdateTime    string `json:"update_time"`
}


func main() {
	tableName := "test"
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
				Points:  123,
				WalletUser:   "abcdefg",
				UpdateTime: 11111,
				Version: 1,
			},
		},
		{
			Item: Item{
				Points:  456,
				WalletUser:   "hijklmn",
				UpdateTime: 22222,
				Version: 2,
			},
		},
	}
	d := dynamo.NewDynamo[Item](cfg)
	for i := 0; i < 10; i++ {
		err := d.TxInsertItems(context.TODO(), insertInfos)
		log.Println("index:", i, ", err:", err)
	}
}
