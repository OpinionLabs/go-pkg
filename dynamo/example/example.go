package main

import (
	"context"
	"log"

	"github.com/ChewZ-life/go-pkg/dynamo"
)

type Item struct {
        WalletUser    string `json:"wallet_user"`
        Points    	  int    `json:"points"`
        Version       int    `json:"version"`
        UpdateTime    string `json:"update_time"`
}


func main() {
	tableName := "Test"
	cfg := dynamo.Config{
		Region:    "ap-southeast-1",
		// APIKey:"",
		// SecretKey:"",
		PoolSize:  1,
		TableName: tableName,
	}

	insertInfos := []dynamo.InsertInfo[Item]{
		{
			Item: Item{
				Points:  123,
				WalletUser:   "abcdefg",
				UpdateTime: "1111111",
				Version: 1,
			},
		},
		{
			Item: Item{
				Points:  456,
				WalletUser:   "hijklmn",
				UpdateTime: "222222",
				Version: 2,
			},
		},
	}
	d := dynamo.NewDynamo[Item](cfg)
	for i := 0; i < 1; i++ {
		err := d.InsertItems(context.TODO(), insertInfos)
		log.Println("index:", i, ", err:", err)
	}

	txInsertInfos := []dynamo.TxInsertInfo[Item]{
		{
			Item: Item{
				Points:  123,
				WalletUser:   "abcdefg-tx",
				UpdateTime: "1111111",
				Version: 1,
			},
		},
		{
			Item: Item{
				Points:  456,
				WalletUser:   "hijklmn-tx",
				UpdateTime: "222222",
				Version: 2,
			},
		},
	}

	for i := 0; i < 1; i++ {
		err := d.TxInsertItems(context.TODO(), txInsertInfos)
		log.Println("index:", i, ", err:", err)
	}
}
