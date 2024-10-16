package dynamo

import (
	"context"
	"fmt"
	"log"
	"time"

	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"github.com/ChewZ-life/go-pkg/monitor"
)

func getTestCfg(tableName string) Config {
	return Config{
		Region:    "binbintest",
		APIKey:    "binbintest",
		SecretKey: "binbintest",
		Endpoint:  "http://localhost:8000",
		PoolSize:  1,
		TableName: tableName,
		Session:   "test",
	}
}

type TestItem0 struct {
	NumKey int    `dynamodbav:"num_key"`
	Other  string `dynamodbav:"other"`
}

// 建一个表, 分区键数字, 排序建是字符串
func createTableType1(tableName string, hashKey, sortKey string) {
	d := NewDynamo[TestItem0](getTestCfg(tableName))
	defer d.Exit()
	_, err := d.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(hashKey),
				AttributeType: types.ScalarAttributeTypeN,
			}, {
				AttributeName: aws.String(sortKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(hashKey),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String(sortKey),
				KeyType:       types.KeyTypeRange,
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{
			{
				IndexName: aws.String("idx_num_key_other"),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String(hashKey),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String(sortKey),
						KeyType:       types.KeyTypeRange,
					},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
			},
		},
		TableName: aws.String(tableName),
	})
	log.Println("createTableType1 table:", tableName, ", hashKey:", hashKey,
		", sortKey:", sortKey, ", err:", err)
}

// 建一个表, 分区键字符串, 没有排序建
func createTableType2(tableName string, hashKey string) {
	d := NewDynamo[TestItem0](getTestCfg(tableName))
	defer d.Exit()
	_, err := d.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(hashKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(hashKey),
				KeyType:       types.KeyTypeHash,
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
		TableName: aws.String(tableName),
	})
	log.Println("createTableType2 table:", tableName, ", hashKey:", hashKey, ", err:", err)
}

// 建一个表, 分区键字符串, 排序建是数字
func createTableType3(tableName string, hashKey, sortKey string) {
	d := NewDynamo[TestItem0](getTestCfg(tableName))
	defer d.Exit()
	_, err := d.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(hashKey),
				AttributeType: types.ScalarAttributeTypeS,
			}, {
				AttributeName: aws.String(sortKey),
				AttributeType: types.ScalarAttributeTypeN,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(hashKey),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String(sortKey),
				KeyType:       types.KeyTypeRange,
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
		TableName: aws.String(tableName),
	})
	log.Println("createTableType3 table:", tableName, ", hashKey:", hashKey,
		", sortKey:", sortKey, ", err:", err)
}

func deleteTable(tableName string) {
	d := NewDynamo[TestItem0](getTestCfg(tableName))
	defer d.Exit()
	_, err := d.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	})
	log.Println("deleteTable table:", tableName, ", err:", err)
}

func testDynamo_InsertItems_Case0(t *testing.T, tableName, indexName string) {
	d := NewDynamo[TestItem0](getTestCfg(tableName))
	defer d.Exit()

	var infos []InsertInfo[TestItem0]
	for i := 0; i < 25; i++ {
		infos = append(infos, InsertInfo[TestItem0]{
			Item: TestItem0{
				NumKey: i,
				Other:  fmt.Sprint(10 + i),
			},
		})
	}

	assert := require.New(t)
	err := d.InsertItems(context.TODO(), infos)
	assert.Nil(err)

	checkGetItems := func(getItems []TestItem0) {
		getKeyToItem := lo.SliceToMap(getItems, func(v TestItem0) (int, TestItem0) {
			return v.NumKey, v
		},
		)
		assert.Equal(len(getKeyToItem), len(getItems))
		for _, expectInfo := range infos {
			getItem, ok := getKeyToItem[expectInfo.Item.NumKey]
			assert.True(ok)
			assert.Equal(expectInfo.Item, getItem)
		}
	}

	// 测试查询, 使用不同的limit测试分页的正确性
	for _, limit := range []int{1, 2, 3, 4, 5, 6, 1000} {
		var getItems []TestItem0
		var fromKey interface{}
		for {
			getItemsIn, lastKey, err := d.ScanIndex(context.TODO(), indexName, fromKey, limit)
			assert.Nil(err)
			getItems = append(getItems, getItemsIn...)
			if lastKey == nil {
				break
			}
			fromKey = lastKey
		}
		assert.Equal(len(infos), len(getItems))

		checkGetItems(getItems)
	}
}

// 测试写入一批记录, 然后使用全表扫描获取结果
func TestDynamo_InsertItems_Case0(t *testing.T) {
	/*
		// 新建表格
		 aws dynamodb create-table --endpoint-url http://localhost:8000 \
		     --table-name tb_test_item0 \
		     --attribute-definitions \
		         AttributeName=num_key,AttributeType=N \
		         AttributeName=other,AttributeType=S \
		     --key-schema \
		         AttributeName=num_key,KeyType=HASH \
		         AttributeName=other,KeyType=RANGE \
		     --provisioned-throughput \
		         ReadCapacityUnits=5,WriteCapacityUnits=5 \
		     --table-class STANDARD

		 // 列出表格
		 aws dynamodb list-tables --endpoint-url http://localhost:8000

		 // 删除表格
		 aws dynamodb delete-table --endpoint-url http://localhost:8000 \
		 --table-name tb_test_item0
	*/
	monitor.InitForTest()
	tableName := "tb_test_item0"
	deleteTable(tableName)
	createTableType1(tableName, "num_key", "other")
	testDynamo_InsertItems_Case0(t, tableName, "")
}

// 测试写入一批记录, 然后使用扫描本地二级索引获取结果
func TestDynamo_InsertItems_Case1(t *testing.T) {
	/*
	   // 新建表格
	   aws dynamodb create-table --endpoint-url http://localhost:8000 \
	       --table-name tb_test_item01 \
	       --attribute-definitions \
	           AttributeName=num_key,AttributeType=N \
	           AttributeName=other,AttributeType=S \
	       --key-schema \
	           AttributeName=num_key,KeyType=HASH \
	           AttributeName=other,KeyType=RANGE \
	       --provisioned-throughput \
	           ReadCapacityUnits=5,WriteCapacityUnits=5 \
	       --table-class STANDARD \
	       --local-secondary-indexes \
	          "[{\"IndexName\": \"idx_num_key_other\",
	            \"KeySchema\":[{\"AttributeName\":\"num_key\",\"KeyType\":\"HASH\"},
	                           {\"AttributeName\":\"other\",\"KeyType\":\"RANGE\"}],
	            \"Projection\":{\"ProjectionType\":\"ALL\"}}]"

	   // 列出表格
	   aws dynamodb list-tables --endpoint-url http://localhost:8000

	   // 删除表格
	   aws dynamodb delete-table --endpoint-url http://localhost:8000 \
	   --table-name tb_test_item01
	*/
	monitor.InitForTest()
	tableName := "tb_test_item01"
	deleteTable(tableName)
	createTableType1(tableName, "num_key", "other")
	testDynamo_InsertItems_Case0(t, tableName, "idx_num_key_other")
}

func TestDynamo_TxInsertItems_Case0(t *testing.T) {
	monitor.InitForTest()
	tableName := "tb_test_item01"
	deleteTable(tableName)
	createTableType1(tableName, "num_key", "other")

	d := NewDynamo[TestItem0](getTestCfg(tableName))
	defer d.Exit()

	var infos []TxInsertInfo[TestItem0]
	for i := 0; i < 25; i++ {
		infos = append(infos, TxInsertInfo[TestItem0]{
			Item: TestItem0{
				NumKey: i,
				Other:  fmt.Sprint(10 + i),
			},
		})
	}

	assert := require.New(t)
	err := d.TxInsertItems(context.TODO(), infos) // 跟上一个case的区别在这里, 使用了事务写
	assert.Nil(err)

	checkGetItems := func(getItems []TestItem0) {
		getKeyToItem := lo.SliceToMap(getItems, func(v TestItem0) (int, TestItem0) {
			return v.NumKey, v
		},
		)
		assert.Equal(len(getKeyToItem), len(getItems))
		for _, expectInfo := range infos {
			getItem, ok := getKeyToItem[expectInfo.Item.NumKey]
			assert.True(ok)
			assert.Equal(expectInfo.Item, getItem)
		}
	}

	// 测试查询, 使用不同的limit测试分页的正确性
	for _, limit := range []int{1, 2, 3, 4, 5, 6, 1000} {
		var getItems []TestItem0
		var fromKey interface{}
		for {
			getItemsIn, lastKey, err := d.ScanIndex(context.TODO(), "", fromKey, limit)
			assert.Nil(err)
			getItems = append(getItems, getItemsIn...)
			if lastKey == nil {
				break
			}
			fromKey = lastKey
		}
		assert.Equal(len(infos), len(getItems))

		checkGetItems(getItems)
	}
}

// TestDynamo_TxInsertItems_Case1 测试条件表达式, 写入的记录存在时直接报错
func TestDynamo_TxInsertItems_Case1(t *testing.T) {
	monitor.InitForTest()
	tableName := "tb_test_item01"
	deleteTable(tableName)
	createTableType1(tableName, "num_key", "other")

	d := NewDynamo[TestItem0](getTestCfg(tableName))
	defer d.Exit()

	item := TestItem0{
		NumKey: 11,
		Other:  "abc",
	}
	conditions := Conditions{
		AttributeNotExists: map[string]any{
			"num_key": struct{}{},
		},
	}

	var insertInfos []TxInsertInfo[TestItem0]
	insertInfos = append(insertInfos, TxInsertInfo[TestItem0]{
		Item:       item,
		Conditions: conditions,
	})

	assert := require.New(t)
	err := d.TxInsertItems(context.TODO(), insertInfos)
	assert.Nil(err)

	// 重复写入, 会报错
	err = d.TxInsertItems(context.TODO(), insertInfos)
	assert.NotNil(err, err)
}

// TestDynamo_TxInsertItems_Case2 测试条件表达式, 写入的记录不存在时直接报错
func TestDynamo_TxInsertItems_Case2(t *testing.T) {
	monitor.InitForTest()
	tableName := "tb_test_item01"
	deleteTable(tableName)
	createTableType1(tableName, "num_key", "other")

	d := NewDynamo[TestItem0](getTestCfg(tableName))
	defer d.Exit()

	item := TestItem0{
		NumKey: 11,
		Other:  "abc",
	}
	conditions := Conditions{
		AttributeExists: map[string]any{
			"num_key": struct{}{},
		},
	}

	var insertInfos []TxInsertInfo[TestItem0]
	insertInfos = append(insertInfos, TxInsertInfo[TestItem0]{
		Item:       item,
		Conditions: conditions,
	})

	assert := require.New(t)
	err := d.TxInsertItems(context.TODO(), insertInfos)
	assert.NotNil(err)
}

// TestDynamo_TxInsertItems_Case3 测试幂等性
func TestDynamo_TxInsertItems_Case3(t *testing.T) {
	monitor.InitForTest()
	tableName := "tb_test_item01"
	deleteTable(tableName)
	createTableType1(tableName, "num_key", "other")

	d := NewDynamo[TestItem0](getTestCfg(tableName))
	defer d.Exit()

	item := TestItem0{
		NumKey: 11,
		Other:  "abc",
	}
	conditions := Conditions{
		AttributeNotExists: map[string]any{
			"num_key": struct{}{},
		},
	}

	var insertInfos []TxInsertInfo[TestItem0]
	insertInfos = append(insertInfos, TxInsertInfo[TestItem0]{
		Item:       item,
		Conditions: conditions,
	})

	assert := require.New(t)
	assert.Nil(d.TxInsertItems(context.TODO(), insertInfos))
}

type TestItem02 struct {
	TestItem0
	StrAttr100 string `json:"str_attr100,omitempty"`
	IntAttr100 int    `json:"int_attr100,omitempty"`
}

func TestDynamo_DeleteItems_Case0(t *testing.T) {
	/*
	   // 新建表格
	   aws dynamodb create-table --endpoint-url http://localhost:8000 \
	       --table-name tb_test_item03 \
	       --attribute-definitions \
	           AttributeName=num_key,AttributeType=N \
	           AttributeName=other,AttributeType=S \
	       --key-schema \
	           AttributeName=num_key,KeyType=HASH \
	           AttributeName=other,KeyType=RANGE \
	       --provisioned-throughput \
	           ReadCapacityUnits=5,WriteCapacityUnits=5 \
	       --table-class STANDARD \
	       --local-secondary-indexes \
	          "[{\"IndexName\": \"idx_num_key_other\",
	            \"KeySchema\":[{\"AttributeName\":\"num_key\",\"KeyType\":\"HASH\"},
	                           {\"AttributeName\":\"other\",\"KeyType\":\"RANGE\"}],
	            \"Projection\":{\"ProjectionType\":\"ALL\"}}]"

	   // 列出表格
	   aws dynamodb list-tables --endpoint-url http://localhost:8000

	   // 删除表格
	   aws dynamodb delete-table --endpoint-url http://localhost:8000 \
	   --table-name tb_test_item03
	*/
	monitor.InitForTest()
	tableName := "tb_test_item03"
	deleteTable(tableName)
	createTableType1(tableName, "num_key", "other")

	d := NewDynamo[TestItem02](getTestCfg(tableName))
	defer d.Exit()

	var items []InsertInfo[TestItem02]
	for i := 0; i < 25; i++ {
		items = append(items, InsertInfo[TestItem02]{
			Item: TestItem02{
				TestItem0: TestItem0{
					NumKey: i,
					Other:  fmt.Sprint(10 + i),
				},
				StrAttr100: "aa",
				IntAttr100: 11,
			},
		})
	}

	assert := require.New(t)
	err := d.InsertItems(context.TODO(), items)
	assert.Nil(err)

	keys := make([]map[string]interface{}, 0)
	for _, item := range items {
		keys = append(keys, map[string]interface{}{
			"num_key": item.Item.NumKey,
			"other":   item.Item.Other,
		})
	}
	err = d.DeleteItems(context.TODO(), keys)
	assert.Nil(err)

	getItems, lastKey, err := d.ScanTable(context.TODO(), nil, 1000)
	assert.Nil(err)
	assert.Nil(lastKey)
	assert.Equal(0, len(getItems))
}

type TestItem1 struct {
	StrKey   string `dynamodbav:"str_key"`
	NumAttr1 int    `dynamodbav:"num_attr1"`
	NumAttr2 int    `dynamodbav:"num_attr2"`
	StrAttr1 string `dynamodbav:"str_attr1"`
	StrAttr2 string `dynamodbav:"str_attr2"`
}

// 测试查询单个项目, 只有分区键 但 没有排序键
func TestDynamo_QueryItem_Case0(t *testing.T) {
	/*
		// 新建表格
		 aws dynamodb create-table --endpoint-url http://localhost:8000 \
		     --table-name tb_test_item1 \
		     --attribute-definitions \
		         AttributeName=str_key,AttributeType=S \
		     --key-schema \
		         AttributeName=str_key,KeyType=HASH \
		     --provisioned-throughput \
		         ReadCapacityUnits=5,WriteCapacityUnits=5 \
		     --table-class STANDARD

		 // 列出表格
		 aws dynamodb list-tables --endpoint-url http://localhost:8000

		 // 删除表格
		 aws dynamodb delete-table --endpoint-url http://localhost:8000 \
		 --table-name tb_test_item1
	*/
	monitor.InitForTest()
	tableName := "tb_test_item1"
	deleteTable(tableName)
	createTableType2(tableName, "str_key")

	d := NewDynamo[TestItem1](getTestCfg(tableName))
	defer d.Exit()

	item := TestItem1{
		StrKey:   "test_key",
		NumAttr1: 10,
		NumAttr2: 11,
		StrAttr1: "aa",
		StrAttr2: "bb",
	}

	assert := require.New(t)
	err := d.InsertItems(context.TODO(), []InsertInfo[TestItem1]{
		{
			Item: item,
		},
	})
	assert.Nil(err)

	exist, getItem, err := d.QueryItem(context.TODO(), map[string]interface{}{
		"str_key": "test_key",
	})
	assert.Nil(err)
	assert.True(exist)
	assert.Equal(item, getItem)

	exist, _, err = d.QueryItem(context.TODO(), map[string]interface{}{
		"str_key": "not_exist_key",
	})
	assert.Nil(err)
	assert.False(exist)

	_, _, err = d.QueryItem(context.TODO(), map[string]interface{}{
		"not_exist_str_key": "test_key", // 无效key值, 也会检查错误
	})
	assert.NotNil(err)
}

type TestItem2 struct {
	StrKey   string `dynamodbav:"str_key"`
	NumKey   int    `dynamodbav:"num_key"`
	NumAttr1 int    `dynamodbav:"num_attr1"`
	NumAttr2 int    `dynamodbav:"num_attr2"`
	StrAttr1 string `dynamodbav:"str_attr1"`
	StrAttr2 string `dynamodbav:"str_attr2"`
}

// 测试查询单个项目, 有分区键 也有排序键
func TestDynamo_QueryItem_Case1(t *testing.T) {
	/*
		// 新建表格
		 aws dynamodb create-table --endpoint-url http://localhost:8000 \
		     --table-name tb_test_item2 \
		     --attribute-definitions \
		         AttributeName=str_key,AttributeType=S \
				 AttributeName=num_key,AttributeType=N \
		     --key-schema \
		         AttributeName=str_key,KeyType=HASH \
				 AttributeName=num_key,KeyType=RANGE \
		     --provisioned-throughput \
		         ReadCapacityUnits=5,WriteCapacityUnits=5 \
		     --table-class STANDARD

		 // 列出表格
		 aws dynamodb list-tables --endpoint-url http://localhost:8000

		 // 删除表格
		 aws dynamodb delete-table --endpoint-url http://localhost:8000 \
		 --table-name tb_test_item2
	*/
	monitor.InitForTest()
	tableName := "tb_test_item2"
	deleteTable(tableName)
	createTableType3(tableName, "str_key", "num_key")

	d := NewDynamo[TestItem2](getTestCfg(tableName))
	defer d.Exit()

	item := TestItem2{
		StrKey:   "test_key",
		NumKey:   1,
		NumAttr1: 10,
		NumAttr2: 11,
		StrAttr1: "aa",
		StrAttr2: "bb",
	}

	assert := require.New(t)
	err := d.InsertItems(context.TODO(), []InsertInfo[TestItem2]{
		{
			Item: item,
		},
	})
	assert.Nil(err)

	exist, getItem, err := d.QueryItem(context.TODO(), map[string]interface{}{
		"str_key": "test_key",
		"num_key": 1, // key的数量必须和表的唯一键相同
	})
	assert.Nil(err)
	assert.True(exist)
	assert.Equal(item, getItem)

	exist, _, err = d.QueryItem(context.TODO(), map[string]interface{}{
		"str_key": "not_exist_key",
		"num_key": 1,
	})
	assert.Nil(err)
	assert.False(exist)

	exist, _, err = d.QueryItem(context.TODO(), map[string]interface{}{
		"str_key": "test_key",
		"num_key": 2,
	})
	assert.Nil(err)
	assert.False(exist)

	_, _, err = d.QueryItem(context.TODO(), map[string]interface{}{
		"str_key": "test_key",
	})
	assert.NotNil(err)
}

func TestDynamo_UpdateItem_Case0(t *testing.T) {
	monitor.InitForTest()
	tableName := "tb_test_item2"
	deleteTable(tableName)
	createTableType3(tableName, "str_key", "num_key")

	d := NewDynamo[TestItem2](getTestCfg(tableName))
	defer d.Exit()

	item := TestItem2{
		StrKey:   "test_key",
		NumKey:   1,
		NumAttr1: 10,
		NumAttr2: 11,
		StrAttr1: "aa",
		StrAttr2: "bb",
	}

	assert := require.New(t)
	err := d.InsertItems(context.TODO(), []InsertInfo[TestItem2]{
		{
			Item: item,
		},
	})
	assert.Nil(err)

	key := map[string]interface{}{
		"str_key": "test_key",
		"num_key": 1,
	}
	sets := map[string]interface{}{
		"num_attr1": 20,
		"str_attr1": "aaaa",
	}
	updateInfo := UpdateInfo{
		Key:     key,
		Sets:    sets,
		Removes: nil,
	}
	err = d.UpdateItem(context.TODO(), updateInfo)
	assert.Nil(err)

	exist, retItem, err := d.QueryItem(context.TODO(), key)
	assert.Nil(err)
	assert.True(exist)
	assert.Equal(20, retItem.NumAttr1)
	assert.Equal(11, retItem.NumAttr2)
	assert.Equal("aaaa", retItem.StrAttr1)
	assert.Equal("bb", retItem.StrAttr2)
}

func TestDynamo_UpdateItem_Case1(t *testing.T) {
	monitor.InitForTest()
	tableName := "tb_test_item2"
	deleteTable(tableName)
	createTableType3(tableName, "str_key", "num_key")

	d := NewDynamo[TestItem2](getTestCfg(tableName))
	defer d.Exit()

	item := TestItem2{
		StrKey:   "test_key2",
		NumKey:   2,
		NumAttr1: 10,
		NumAttr2: 11,
		StrAttr1: "aa",
		StrAttr2: "bb",
	}

	assert := require.New(t)
	err := d.InsertItems(context.TODO(), []InsertInfo[TestItem2]{
		{
			Item: item,
		},
	})
	assert.Nil(err)

	key := map[string]interface{}{
		"str_key": "test_key2",
		"num_key": 2,
	}
	removes := map[string]interface{}{
		"num_attr2": nil,
		"str_attr2": nil,
	}
	updateInfo := UpdateInfo{
		Key:     key,
		Sets:    nil,
		Removes: removes,
	}
	err = d.UpdateItem(context.TODO(), updateInfo)
	assert.Nil(err)

	exist, retItem, err := d.QueryItem(context.TODO(), key)
	assert.Nil(err)
	assert.True(exist)
	assert.Equal(10, retItem.NumAttr1)
	assert.Equal(0, retItem.NumAttr2) // 属性被删除了
	assert.Equal("aa", retItem.StrAttr1)
	assert.Equal("", retItem.StrAttr2) // 属性被删除了
}

// TestDynamo_UpdateItem_Case2 测试条件表达式, 条件满足的场景
func TestDynamo_UpdateItem_Case2(t *testing.T) {
	monitor.InitForTest()
	tableName := "tb_test_item2"
	deleteTable(tableName)
	createTableType3(tableName, "str_key", "num_key")

	d := NewDynamo[TestItem2](getTestCfg(tableName))
	defer d.Exit()

	item := TestItem2{
		StrKey:   "test_key2",
		NumKey:   2,
		NumAttr1: 10,
		NumAttr2: 11,
		StrAttr1: "aa",
		StrAttr2: "bb",
	}

	assert := require.New(t)
	err := d.InsertItems(context.TODO(), []InsertInfo[TestItem2]{
		{
			Item: item,
		},
	})
	assert.Nil(err)

	key := map[string]interface{}{
		"str_key": "test_key2",
		"num_key": 2,
	}
	removes := map[string]interface{}{
		"num_attr2": nil,
		"str_attr2": nil,
	}
	conditions := Conditions{
		AttributeEqual: map[string]any{
			"num_attr1": 10,
			"str_attr1": "aa",
		},
	}
	updateInfo := UpdateInfo{
		Key:        key,
		Sets:       nil,
		Removes:    removes,
		Conditions: conditions,
	}
	err = d.UpdateItem(context.TODO(), updateInfo)
	assert.Nil(err)

	exist, retItem, err := d.QueryItem(context.TODO(), key)
	assert.Nil(err)
	assert.True(exist)
	assert.Equal(10, retItem.NumAttr1)
	assert.Equal(0, retItem.NumAttr2) // 属性被删除了
	assert.Equal("aa", retItem.StrAttr1)
	assert.Equal("", retItem.StrAttr2) // 属性被删除了
}

// TestDynamo_UpdateItem_Case3 测试条件表达式, 条件不满足的场景
func TestDynamo_UpdateItem_Case3(t *testing.T) {
	monitor.InitForTest()
	tableName := "tb_test_item2"
	deleteTable(tableName)
	createTableType3(tableName, "str_key", "num_key")

	d := NewDynamo[TestItem2](getTestCfg(tableName))
	defer d.Exit()

	item := TestItem2{
		StrKey:   "test_key2",
		NumKey:   2,
		NumAttr1: 10,
		NumAttr2: 11,
		StrAttr1: "aa",
		StrAttr2: "bb",
	}

	assert := require.New(t)
	err := d.InsertItems(context.TODO(), []InsertInfo[TestItem2]{
		{
			Item: item,
		},
	})
	assert.Nil(err)

	key := map[string]interface{}{
		"str_key": "test_key2",
		"num_key": 2,
	}
	removes := map[string]interface{}{
		"num_attr2": nil,
		"str_attr2": nil,
	}

	nameAttr1s := []int{10, 11, 12}
	strAttr1s := []string{"a", "aa", ""}
	for i := 0; i < len(nameAttr1s); i++ {
		conditions := Conditions{
			AttributeEqual: map[string]any{
				"num_attr1": nameAttr1s[i],
				"str_attr1": strAttr1s[i],
			},
		}
		updateInfo := UpdateInfo{
			Key:        key,
			Sets:       nil,
			Removes:    removes,
			Conditions: conditions,
		}
		err = d.UpdateItem(context.TODO(), updateInfo)
		assert.NotNil(err)
	}

	// 属性没有变
	exist, retItem, err := d.QueryItem(context.TODO(), key)
	assert.Nil(err)
	assert.True(exist)
	assert.Equal(10, retItem.NumAttr1)
	assert.Equal(11, retItem.NumAttr2)
	assert.Equal("aa", retItem.StrAttr1)
	assert.Equal("bb", retItem.StrAttr2)
}

func TestDynamo_TxUpdateItems_Case0(t *testing.T) {
	monitor.InitForTest()
	tableName := "tb_test_item2"
	deleteTable(tableName)
	createTableType3(tableName, "str_key", "num_key")

	d := NewDynamo[TestItem2](getTestCfg(tableName))
	defer d.Exit()

	items := []InsertInfo[TestItem2]{
		{
			Item: TestItem2{
				StrKey:   "test_key2",
				NumKey:   2,
				NumAttr1: 10,
				NumAttr2: 11,
				StrAttr1: "aa",
				StrAttr2: "bb",
			},
		},
		{
			Item: TestItem2{
				StrKey:   "test_key22",
				NumKey:   20,
				NumAttr1: 100,
				NumAttr2: 110,
				StrAttr1: "aaaa",
				StrAttr2: "bbbb",
			},
		},
	}

	assert := require.New(t)
	err := d.InsertItems(context.TODO(), items)
	assert.Nil(err)

	updates := []UpdateInfo{
		{
			Key: map[string]any{
				"str_key": "test_key2",
				"num_key": 2,
			},
			Sets: map[string]any{
				"num_attr1": 100,
			},
			Removes: map[string]any{
				"num_attr2": nil,
				"str_attr2": nil,
			},
		},
		{
			Key: map[string]any{
				"str_key": "test_key22",
				"num_key": 20,
			},
			Sets: map[string]any{
				"num_attr1": 1000,
			},
			Removes: map[string]any{
				"num_attr2": nil,
				"str_attr2": nil,
			},
		},
	}

	err = d.TxUpdateItems(context.TODO(), updates)
	assert.Nil(err)

	exist, retItem, err := d.QueryItem(context.TODO(), updates[0].Key)
	assert.Nil(err)
	assert.True(exist)
	assert.Equal(100, retItem.NumAttr1)
	assert.Equal(0, retItem.NumAttr2) // 属性被删除了
	assert.Equal("aa", retItem.StrAttr1)
	assert.Equal("", retItem.StrAttr2) // 属性被删除了

	exist, retItem, err = d.QueryItem(context.TODO(), updates[1].Key)
	assert.Nil(err)
	assert.True(exist)
	assert.Equal(1000, retItem.NumAttr1)
	assert.Equal(0, retItem.NumAttr2) // 属性被删除了
	assert.Equal("aaaa", retItem.StrAttr1)
	assert.Equal("", retItem.StrAttr2) // 属性被删除了
}

// TestDynamo_TxUpdateItems_Case1 测试条件表达式满足的场景
func TestDynamo_TxUpdateItems_Case1(t *testing.T) {
	monitor.InitForTest()
	tableName := "tb_test_item2"
	deleteTable(tableName)
	createTableType3(tableName, "str_key", "num_key")

	d := NewDynamo[TestItem2](getTestCfg(tableName))
	defer d.Exit()

	items := []InsertInfo[TestItem2]{
		{
			Item: TestItem2{
				StrKey:   "test_key2",
				NumKey:   2,
				NumAttr1: 10,
				NumAttr2: 11,
				StrAttr1: "aa",
				StrAttr2: "bb",
			},
		},
	}

	assert := require.New(t)
	err := d.InsertItems(context.TODO(), items)
	assert.Nil(err)

	updates := []UpdateInfo{
		{
			Key: map[string]any{
				"str_key": "test_key2",
				"num_key": 2,
			},
			Sets: map[string]any{
				"num_attr1": 100,
			},
			Removes: map[string]any{
				"num_attr2": nil,
				"str_attr2": nil,
			},
			Conditions: Conditions{
				AttributeEqual: map[string]any{
					"num_attr1": 10,
					"str_attr1": "aa",
				},
			},
		},
	}

	err = d.TxUpdateItems(context.TODO(), updates)
	assert.Nil(err)

	exist, retItem, err := d.QueryItem(context.TODO(), updates[0].Key)
	assert.Nil(err)
	assert.True(exist)
	assert.Equal(100, retItem.NumAttr1)
	assert.Equal(0, retItem.NumAttr2) // 属性被删除了
	assert.Equal("aa", retItem.StrAttr1)
	assert.Equal("", retItem.StrAttr2) // 属性被删除了
}

// TestDynamo_TxUpdateItems_Case2 测试条件表达式不满足的场景
func TestDynamo_TxUpdateItems_Case2(t *testing.T) {
	monitor.InitForTest()
	tableName := "tb_test_item2"
	deleteTable(tableName)
	createTableType3(tableName, "str_key", "num_key")

	d := NewDynamo[TestItem2](getTestCfg(tableName))
	defer d.Exit()

	items := []InsertInfo[TestItem2]{
		{
			Item: TestItem2{
				StrKey:   "test_key2",
				NumKey:   2,
				NumAttr1: 10,
				NumAttr2: 11,
				StrAttr1: "aa",
				StrAttr2: "bb",
			},
		},
	}

	assert := require.New(t)
	err := d.InsertItems(context.TODO(), items)
	assert.Nil(err)

	nameAttr1s := []int{10, 11, 12}
	strAttr1s := []string{"a", "aa", ""}
	for i := 0; i < len(nameAttr1s); i++ {
		conditions := Conditions{
			AttributeEqual: map[string]any{
				"num_attr1": nameAttr1s[i],
				"str_attr1": strAttr1s[i],
			},
		}
		updates := []UpdateInfo{
			{
				Key: map[string]any{
					"str_key": "test_key2",
					"num_key": 2,
				},
				Sets: map[string]any{
					"num_attr1": 100,
				},
				Removes: map[string]any{
					"num_attr2": nil,
					"str_attr2": nil,
				},
				Conditions: conditions,
			},
		}
		err = d.TxUpdateItems(context.TODO(), updates)
		assert.NotNil(err)
	}

	key := map[string]any{
		"str_key": "test_key2",
		"num_key": 2,
	}
	exist, retItem, err := d.QueryItem(context.TODO(), key)
	assert.Nil(err)
	assert.True(exist)
	assert.Equal(10, retItem.NumAttr1)
	assert.Equal(11, retItem.NumAttr2)
	assert.Equal("aa", retItem.StrAttr1)
	assert.Equal("bb", retItem.StrAttr2)
}

// TestDynamo_TxUpdateItems_Case3 测试幂等
func TestDynamo_TxUpdateItems_Case3(t *testing.T) {
	monitor.InitForTest()
	tableName := "tb_test_item2"
	deleteTable(tableName)
	createTableType3(tableName, "str_key", "num_key")

	d := NewDynamo[TestItem2](getTestCfg(tableName))
	defer d.Exit()

	items := []InsertInfo[TestItem2]{
		{
			Item: TestItem2{
				StrKey:   "test_key2",
				NumKey:   2,
				NumAttr1: 10,
				NumAttr2: 11,
				StrAttr1: "aa",
				StrAttr2: "bb",
			},
		},
	}

	assert := require.New(t)
	err := d.InsertItems(context.TODO(), items)
	assert.Nil(err)

	updates := []UpdateInfo{
		{
			Key: map[string]any{
				"str_key": "test_key2",
				"num_key": 2,
			},
			Sets: map[string]any{
				"num_attr1": 100,
			},
			Removes: map[string]any{
				"num_attr2": nil,
				"str_attr2": nil,
			},
			Conditions: Conditions{
				AttributeEqual: map[string]any{
					"num_attr1": 10,
				},
			},
		},
	}

	// txID := WithTxID(fmt.Sprint(time.Now().UnixNano()))
	assert.Nil(d.TxUpdateItems(context.TODO(), updates))    // 首次请求,可以成功
	assert.NotNil(d.TxUpdateItems(context.TODO(), updates)) // 条件表达式不满足条件
}

// TestDynamo_TxUpdateItems_Case4 测试条件表达式超过100个的场景
func TestDynamo_TxUpdateItems_Case4(t *testing.T) {
	monitor.InitForTest()
	tableName := "tb_test_item4"
	deleteTable(tableName)
	createTableType3(tableName, "str_key", "num_key")

	d := NewDynamo[TestItem2](getTestCfg(tableName))
	defer d.Exit()

	items := []InsertInfo[TestItem2]{}
	for i := 0; i < 200; i++ {
		items = append(items, InsertInfo[TestItem2]{
			Item: TestItem2{
				StrKey:   fmt.Sprintf("test_key%d", i),
				NumKey:   i,
				NumAttr1: i,
				NumAttr2: 11,
				StrAttr1: "aa",
				StrAttr2: "bb",
			},
		})
	}

	assert := require.New(t)
	err := d.InsertItems(context.TODO(), items)
	assert.Nil(err)

	updates := []UpdateInfo{}
	for i := 0; i < 200; i++ {
		updates = append(updates, UpdateInfo{
			Key: map[string]any{
				"str_key": fmt.Sprintf("test_key%d", i),
				"num_key": i,
			},
			Sets: map[string]any{
				"num_attr1": 10000 + i,
			},
		})
	}

	err = d.TxUpdateItems(context.TODO(), updates)
	assert.NotNil(err)
}

func TestDynamo_UpdateItems_Case0(t *testing.T) {
	monitor.InitForTest()
	tableName := "tb_test_item2"
	deleteTable(tableName)
	createTableType3(tableName, "str_key", "num_key")

	d := NewDynamo[TestItem2](getTestCfg(tableName))
	defer d.Exit()

	items := []InsertInfo[TestItem2]{
		{
			Item: TestItem2{
				StrKey:   "test_key2",
				NumKey:   2,
				NumAttr1: 10,
				NumAttr2: 11,
				StrAttr1: "aa",
				StrAttr2: "bb",
			},
		},
		{
			Item: TestItem2{
				StrKey:   "test_key22",
				NumKey:   20,
				NumAttr1: 100,
				NumAttr2: 110,
				StrAttr1: "aaaa",
				StrAttr2: "bbbb",
			},
		},
	}

	assert := require.New(t)
	err := d.InsertItems(context.TODO(), items)
	assert.Nil(err)

	updates := []UpdateInfo{
		{
			Key: map[string]any{
				"str_key": "test_key2",
				"num_key": 2,
			},
			Sets: map[string]any{
				"num_attr1": 100,
			},
			Removes: map[string]any{
				"num_attr2": nil,
				"str_attr2": nil,
			},
		},
		{
			Key: map[string]any{
				"str_key": "test_key22",
				"num_key": 20,
			},
			Sets: map[string]any{
				"num_attr1": 1000,
			},
			Removes: map[string]any{
				"num_attr2": nil,
				"str_attr2": nil,
			},
		},
	}

	succKeys, err := d.UpdateItems(context.TODO(), updates)
	assert.Equal(2, len(succKeys))
	assert.Nil(err)

	exist, retItem, err := d.QueryItem(context.TODO(), updates[0].Key)
	assert.Nil(err)
	assert.True(exist)
	assert.Equal(100, retItem.NumAttr1)
	assert.Equal(0, retItem.NumAttr2) // 属性被删除了
	assert.Equal("aa", retItem.StrAttr1)
	assert.Equal("", retItem.StrAttr2) // 属性被删除了

	exist, retItem, err = d.QueryItem(context.TODO(), updates[1].Key)
	assert.Nil(err)
	assert.True(exist)
	assert.Equal(1000, retItem.NumAttr1)
	assert.Equal(0, retItem.NumAttr2) // 属性被删除了
	assert.Equal("aaaa", retItem.StrAttr1)
	assert.Equal("", retItem.StrAttr2) // 属性被删除了
}

// TestDynamo_UpdateItems_Case1 测试条件表达式满足的场景
func TestDynamo_UpdateItems_Case1(t *testing.T) {
	monitor.InitForTest()
	tableName := "tb_test_item2"
	deleteTable(tableName)
	createTableType3(tableName, "str_key", "num_key")

	d := NewDynamo[TestItem2](getTestCfg(tableName))
	defer d.Exit()

	items := []InsertInfo[TestItem2]{
		{
			Item: TestItem2{
				StrKey:   "test_key2",
				NumKey:   2,
				NumAttr1: 10,
				NumAttr2: 11,
				StrAttr1: "aa",
				StrAttr2: "bb",
			},
		},
	}

	assert := require.New(t)
	err := d.InsertItems(context.TODO(), items)
	assert.Nil(err)

	updates := []UpdateInfo{
		{
			Key: map[string]any{
				"str_key": "test_key2",
				"num_key": 2,
			},
			Sets: map[string]any{
				"num_attr1": 100,
			},
			Removes: map[string]any{
				"num_attr2": nil,
				"str_attr2": nil,
			},
			Conditions: Conditions{
				AttributeEqual: map[string]any{
					"num_attr1": 10,
					"str_attr1": "aa",
				},
			},
		},
	}

	succKeys, err := d.UpdateItems(context.TODO(), updates)
	assert.Equal(1, len(succKeys))
	assert.Nil(err)

	exist, retItem, err := d.QueryItem(context.TODO(), updates[0].Key)
	assert.Nil(err)
	assert.True(exist)
	assert.Equal(100, retItem.NumAttr1)
	assert.Equal(0, retItem.NumAttr2) // 属性被删除了
	assert.Equal("aa", retItem.StrAttr1)
	assert.Equal("", retItem.StrAttr2) // 属性被删除了
}

// TestDynamo_UpdateItems_Case2 测试条件表达式不满足的场景
func TestDynamo_UpdateItems_Case2(t *testing.T) {
	monitor.InitForTest()
	tableName := "tb_test_item2"
	deleteTable(tableName)
	createTableType3(tableName, "str_key", "num_key")

	d := NewDynamo[TestItem2](getTestCfg(tableName))
	defer d.Exit()

	items := []InsertInfo[TestItem2]{
		{
			Item: TestItem2{
				StrKey:   "test_key2",
				NumKey:   2,
				NumAttr1: 10,
				NumAttr2: 11,
				StrAttr1: "aa",
				StrAttr2: "bb",
			},
		},
	}

	assert := require.New(t)
	err := d.InsertItems(context.TODO(), items)
	assert.Nil(err)

	nameAttr1s := []int{10, 11, 12}
	strAttr1s := []string{"a", "aa", ""}
	for i := 0; i < len(nameAttr1s); i++ {
		conditions := Conditions{
			AttributeEqual: map[string]any{
				"num_attr1": nameAttr1s[i],
				"str_attr1": strAttr1s[i],
			},
		}
		updates := []UpdateInfo{
			{
				Key: map[string]any{
					"str_key": "test_key2",
					"num_key": 2,
				},
				Sets: map[string]any{
					"num_attr1": 100,
				},
				Removes: map[string]any{
					"num_attr2": nil,
					"str_attr2": nil,
				},
				Conditions: conditions,
			},
		}
		succKeys, err := d.UpdateItems(context.TODO(), updates)
		assert.Equal(0, len(succKeys))
		assert.NotNil(err)
	}

	key := map[string]any{
		"str_key": "test_key2",
		"num_key": 2,
	}
	exist, retItem, err := d.QueryItem(context.TODO(), key)
	assert.Nil(err)
	assert.True(exist)
	assert.Equal(10, retItem.NumAttr1)
	assert.Equal(11, retItem.NumAttr2)
	assert.Equal("aa", retItem.StrAttr1)
	assert.Equal("bb", retItem.StrAttr2)
}

// TestDynamo_UpdateItems_Case3 测试条件表达式超过100个的场景
func TestDynamo_UpdateItems_Case3(t *testing.T) {
	monitor.InitForTest()
	tableName := "tb_test_item3"
	deleteTable(tableName)
	createTableType3(tableName, "str_key", "num_key")

	d := NewDynamo[TestItem2](getTestCfg(tableName))
	defer d.Exit()

	items := []InsertInfo[TestItem2]{}
	for i := 0; i < 200; i++ {
		items = append(items, InsertInfo[TestItem2]{
			Item: TestItem2{
				StrKey:   fmt.Sprintf("test_key%d", i),
				NumKey:   i,
				NumAttr1: i,
				NumAttr2: 11,
				StrAttr1: "aa",
				StrAttr2: "bb",
			},
		})
	}

	assert := require.New(t)
	err := d.InsertItems(context.TODO(), items)
	assert.Nil(err)

	updates := []UpdateInfo{}
	for i := 0; i < 200; i++ {
		updates = append(updates, UpdateInfo{
			Key: map[string]any{
				"str_key": fmt.Sprintf("test_key%d", i),
				"num_key": i,
			},
			Sets: map[string]any{
				"num_attr1": 10000 + i,
			},
		})
	}

	succKeys, err := d.UpdateItems(context.TODO(), updates)
	assert.Equal(200, len(succKeys))
	assert.Nil(err)

	for i := 0; i < 200; i++ {
		key := map[string]any{
			"str_key": fmt.Sprintf("test_key%d", i),
			"num_key": i,
		}
		exist, retItem, err := d.QueryItem(context.TODO(), key)
		assert.Nil(err)
		assert.True(exist)
		assert.Equal(10000+i, retItem.NumAttr1)
		assert.Equal(11, retItem.NumAttr2)
		assert.Equal("aa", retItem.StrAttr1)
		assert.Equal("bb", retItem.StrAttr2)
	}
}

type FillOrder struct {
	ID         int64     `orm:"column(id);pk;auto;" dynamodbav:"id"`          // 主键
	UserID     int64     `orm:"column(user_id)" dynamodbav:"user_id"`         // 用户id
	OrderID    int64     `orm:"column(order_id)" dynamodbav:"order_id"`       // 订单id
	CreateTime time.Time `orm:"column(create_time)" dynamodbav:"create_time"` // 记录生成时间
	UpdateTime time.Time `orm:"column(update_time)" dynamodbav:"update_time"` // 记录更新时间
}

type FillOrderWrap struct {
	FillOrder
	SortID string `dynamodbav:"sort_id"`
	TTL    int    `dynamodbav:"ttl,omitempty"`
}

func createFillOrder(tableName string, hashKey, sortKey, globalHash, globalSort string) {
	d := NewDynamo[FillOrderWrap](getTestCfg(tableName))
	defer d.Exit()
	_, err := d.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(hashKey),
				AttributeType: types.ScalarAttributeTypeN,
			},
			{
				AttributeName: aws.String(sortKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(globalHash),
				AttributeType: types.ScalarAttributeTypeN,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(hashKey),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String(sortKey),
				KeyType:       types.KeyTypeRange,
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("idx_orderid_sortid"),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String(globalHash),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String(globalSort),
						KeyType:       types.KeyTypeRange,
					},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
				ProvisionedThroughput: &types.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(5),
					WriteCapacityUnits: aws.Int64(5),
				},
			},
		},
		TableName: aws.String(tableName),
	})
	log.Println("createFillOrder table:", tableName, ", hashKey:", hashKey,
		", sortKey:", sortKey, ", err:", err)
}

func TestDynamo_TxRawExec_Case0(t *testing.T) {
	monitor.InitForTest()
	tableName0 := "tb_fill_order0"
	deleteTable(tableName0)
	createFillOrder(tableName0, "user_id", "sort_id", "order_id", "sort_id")

	tableName1 := "tb_fill_order1"
	deleteTable(tableName1)
	createFillOrder(tableName1, "user_id", "sort_id", "order_id", "sort_id")

	// 插入测试数据
	d := NewDynamo[FillOrderWrap](getTestCfg(tableName0))
	defer d.Exit()

	insertItems := []TxRawInsert{
		TxRawInsert{
			TableName: "tb_fill_order0",
			Item: FillOrderWrap{
				FillOrder: FillOrder{
					ID:      10,
					UserID:  10,
					OrderID: 10,
				},
				SortID: fmt.Sprintf("%d#%d", 10, 10),
				TTL:    int(time.Now().Add(32 * 24 * time.Hour).UnixMilli()),
			},
		},
		TxRawInsert{
			TableName: "tb_fill_order1",
			Item: FillOrderWrap{
				FillOrder: FillOrder{
					ID:      20,
					UserID:  20,
					OrderID: 20,
				},
				SortID: fmt.Sprintf("%d#%d", 20, 20),
				TTL:    int(time.Now().Add(32 * 24 * time.Hour).UnixMilli()),
			},
		},
	}

	assert := require.New(t)
	err := d.TxRawExec(context.TODO(), insertItems, nil)
	assert.Nil(err)

	insertItems = []TxRawInsert{
		TxRawInsert{
			TableName: "tb_fill_order0",
			Item: FillOrderWrap{
				FillOrder: FillOrder{
					ID:      30,
					UserID:  30,
					OrderID: 30,
				},
				SortID: fmt.Sprintf("%d#%d", 30, 30),
				TTL:    int(time.Now().Add(32 * 24 * time.Hour).UnixMilli()),
			},
		},
		TxRawInsert{
			TableName: "tb_fill_order1",
			Item: FillOrderWrap{
				FillOrder: FillOrder{
					ID:      40,
					UserID:  40,
					OrderID: 40,
				},
				SortID: fmt.Sprintf("%d#%d", 40, 40),
				TTL:    int(time.Now().Add(32 * 24 * time.Hour).UnixMilli()),
			},
		},
	}
	updateItems := []TxRawUpdate{
		TxRawUpdate{
			TableName: "tb_fill_order0",
			UpdateInfo: UpdateInfo{
				Key: map[string]interface{}{
					"user_id": 10,
					"sort_id": fmt.Sprintf("%d#%d", 10, 10),
				},
				Sets: map[string]interface{}{
					"order_id": 100,
				},
			},
		},
	}
	err = d.TxRawExec(context.TODO(), insertItems, updateItems)
	assert.Nil(err)
}

func TestDynamo_QueryItems_Case0(t *testing.T) {
	monitor.InitForTest()
	tableName := "tb_fill_order0"
	deleteTable(tableName)
	createFillOrder(tableName, "user_id", "sort_id", "order_id", "sort_id")

	// 插入测试数据
	d := NewDynamo[FillOrderWrap](getTestCfg(tableName))
	defer d.Exit()

	var infos []InsertInfo[FillOrderWrap]
	startTm := time.Now()
	count := 200
	endTm := startTm.Add(time.Duration(count) * time.Millisecond)
	for i := int64(0); i < 220; i++ {
		fill := FillOrder{
			ID:         i + 1,
			UserID:     1,
			OrderID:    1,
			CreateTime: startTm.Add(time.Millisecond * time.Duration(i)),
		}
		infos = append(infos, InsertInfo[FillOrderWrap]{
			Item: FillOrderWrap{
				FillOrder: fill,
				SortID:    fmt.Sprintf("%d#%d", fill.CreateTime.UnixMilli(), fill.ID),
				TTL:       int(time.Now().Add(32 * 24 * time.Hour).UnixMilli()),
			},
		})
	}
	for i := int64(0); i < 220; i++ {
		fill := FillOrder{
			ID:         i + 1,
			UserID:     2,
			OrderID:    1,
			CreateTime: startTm.Add(time.Millisecond * time.Duration(i)),
		}
		infos = append(infos, InsertInfo[FillOrderWrap]{
			Item: FillOrderWrap{
				FillOrder: fill,
				SortID:    fmt.Sprintf("%d#%d", fill.CreateTime.UnixMilli(), fill.ID),
				TTL:       int(time.Now().Add(32 * 24 * time.Hour).UnixMilli()),
			},
		})
	}

	assert := require.New(t)
	err := d.InsertItems(context.TODO(), infos)
	assert.Nil(err)

	// 根据用户ID搜索
	var lastKey interface{}
	index := ""
	condition := `(user_id = :user_id) and (sort_id between :start_tm and :end_tm)`
	expression := map[string]any{
		":user_id":  1,
		":start_tm": fmt.Sprintf("%d#0", startTm.UnixMilli()),
		":end_tm":   fmt.Sprintf("%d#0", endTm.UnixMilli()+1),
	}
	var fos []FillOrder
	for {
		items, newKey, err := d.QueryItems(context.TODO(), index, condition, expression, lastKey, 15)
		assert.Nil(err)
		for _, item := range items {
			if item.CreateTime.UnixMilli() == startTm.UnixMilli() {
				continue
			}
			fos = append(fos, item.FillOrder)
		}
		if newKey == nil {
			break
		}
		lastKey = newKey
	}
	fmt.Println(len(fos), count)
	assert.True(len(fos) == count)
	assert.True(fos[0].CreateTime.UnixMilli() == startTm.UnixMilli()+1)
	assert.True(fos[len(fos)-1].CreateTime.UnixMilli() == startTm.UnixMilli()+int64(count))
}

func TestDynamo_QueryItems_Case1(t *testing.T) {
	monitor.InitForTest()
	tableName := "tb_fill_order1"
	deleteTable(tableName)
	createFillOrder(tableName, "user_id", "sort_id", "order_id", "sort_id")

	// 插入测试数据
	d := NewDynamo[FillOrderWrap](getTestCfg(tableName))
	defer d.Exit()

	var infos []InsertInfo[FillOrderWrap]
	startTm := time.Now()
	count := 200
	endTm := startTm.Add(time.Duration(count) * time.Millisecond)
	for i := int64(0); i < 220; i++ {
		fill := FillOrder{
			ID:         i + 1,
			UserID:     1,
			OrderID:    1,
			CreateTime: startTm.Add(time.Millisecond * time.Duration(i)),
		}
		infos = append(infos, InsertInfo[FillOrderWrap]{
			Item: FillOrderWrap{
				FillOrder: fill,
				SortID:    fmt.Sprintf("%d#%d", fill.CreateTime.UnixMilli(), fill.ID),
				TTL:       int(time.Now().Add(32 * 24 * time.Hour).UnixMilli()),
			},
		})
	}
	for i := int64(0); i < 220; i++ {
		fill := FillOrder{
			ID:         i + 1,
			UserID:     2,
			OrderID:    2,
			CreateTime: startTm.Add(time.Millisecond * time.Duration(i)),
		}
		infos = append(infos, InsertInfo[FillOrderWrap]{
			Item: FillOrderWrap{
				FillOrder: fill,
				SortID:    fmt.Sprintf("%d#%d", fill.CreateTime.UnixMilli(), fill.ID),
				TTL:       int(time.Now().Add(32 * 24 * time.Hour).UnixMilli()),
			},
		})
	}

	assert := require.New(t)
	err := d.InsertItems(context.TODO(), infos)
	assert.Nil(err)

	// 根据订单ID搜索
	var lastKey interface{}
	index := "idx_orderid_sortid"
	condition := `(order_id = :order_id) and (sort_id between :start_tm and :end_tm)`
	expression := map[string]any{
		":order_id": 1,
		":start_tm": fmt.Sprintf("%d#0", startTm.UnixMilli()),
		":end_tm":   fmt.Sprintf("%d#0", endTm.UnixMilli()+1),
	}
	var fos []FillOrder
	for {
		items, newKey, err := d.QueryItems(context.TODO(), index, condition, expression, lastKey, 15)
		assert.Nil(err)
		for _, item := range items {
			if item.CreateTime.UnixMilli() == startTm.UnixMilli() {
				continue
			}
			fos = append(fos, item.FillOrder)
		}
		if newKey == nil {
			break
		}
		lastKey = newKey
	}
	assert.True(len(fos) == count)
	assert.True(fos[0].CreateTime.UnixMilli() == startTm.UnixMilli()+1)
	assert.True(fos[len(fos)-1].CreateTime.UnixMilli() == startTm.UnixMilli()+int64(count))
}
