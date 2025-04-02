package dynamo

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/ChewZ-life/go-pkg/concurrency/go_pool"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/logging"
	"github.com/pkg/errors"
)

var _ Dynamo[struct{}] = (*dynamo[struct{}])(nil)

// Dynamo wraps aws dynamodb sdk, one object corresponds to one table, can be called concurrently
type Dynamo[T any] interface {
	// ScanTable scans the entire table, starts scanning from fromKey, scans at least limit records
	// For first call, set fromKey to nil, for subsequent calls use the lastKey from previous Scan
	// Scan results are unordered
	ScanTable(ctx context.Context, fromKey any, limit int) (items []T, lastKey any, err error)
	// ScanIndex Scan all indexes of the table, start scanning from fromKey, scan at least limit records
	// For first call, set fromKey to nil, for subsequent calls use the lastKey from previous Scan
	// Scan results are unordered
	ScanIndex(ctx context.Context, index string, fromKey any, limit int) (items []T, lastKey any, err error)
	// QueryItem Query a single record, parameter key needs to fill in partition key and sort key field values
	// Partition key is required, sort key is optional
	QueryItem(ctx context.Context, key map[string]any) (exist bool, retItem T, err error)
	// QueryItems Query multiple records that meet conditions
	// Parameter key needs to fill in partition key and sort key field values
	// Partition key is required, sort key is optional
	QueryItems(ctx context.Context, index, condition string, expression map[string]any, fromKey any, limit int) (items []T, lastKey any, err error)
	// QueryItemWithTable Query a single record with specified table
	// Parameter key needs to fill in partition key and sort key field values
	// Partition key is required, sort key is optional
	QueryItemWithTable(ctx context.Context, key map[string]any, table string) (exist bool, retItem T, err error)
	// QueryItemsWithTable Query multiple records that meet the conditions with specified table. 
	// The parameter 'key' needs to fill in the values of partition key and sort key fields. 
	// Partition key is required, sort key is optional.
	QueryItemsWithTable(ctx context.Context, qcs QueryItemCondition) (items []T, lastKey any, err error)

	// InsertItems Batch write a set of records, maximum 25 records per write
	InsertItems(ctx context.Context, items []InsertInfo[T]) (err error)
	// TxInsertItems Batch write a set of records using transaction
	TxInsertItems(ctx context.Context, items []TxInsertInfo[T], opts ...Option) (err error)
	// DeleteItems Batch delete a set of records, maximum 25 records per deletion
	DeleteItems(ctx context.Context, keys []map[string]any) (err error)
	// UpdateItem Update a single record, key contains partition key and sort key, 
	// updates contains attributes to update, deletes contains attributes to delete
	UpdateItem(ctx context.Context, update UpdateInfo) (err error)
	// UpdateItems Update a batch of records, calls UpdateItem concurrently internally
	UpdateItems(ctx context.Context, updates []UpdateInfo) (succKeys []map[string]any, err error)
	// TxUpdateItems Update multiple records using transaction, key contains partition key and sort key,
	// updates contains attributes to update, deletes contains attributes to delete
	TxUpdateItems(ctx context.Context, updates []UpdateInfo, opts ...Option) (err error)
	// TxRawExec Custom transactions, such as multi-table transactions
	TxRawExec(ctx context.Context, insertItems []TxRawInsert, updateItems []TxRawUpdate, opts ...Option) (err error)

	// For unit testing
	CreateTable(ctx context.Context, input *dynamodb.CreateTableInput) (output *dynamodb.CreateTableOutput, err error)
	DeleteTable(ctx context.Context, input *dynamodb.DeleteTableInput) (output *dynamodb.DeleteTableOutput, err error)
	// Exit Close related connection pools
	Exit()
}

type eventCB func()

type QueryItemCondition struct {
	Index      string
	Condition  string
	Expression map[string]any
	FromKey    any
	Limit      int
	TableName  string
}

func NewDynamo[T any](cfg Config) Dynamo[T] {
	d := &dynamo[T]{
		cfg: cfg,
	}

	// Initialize connection pool
	{
		d.pool = go_pool.NewPool(
			go_pool.WithSize[eventCB](cfg.PoolSize),
			go_pool.WithTaskCB(func(cb eventCB, i int) {
				cb() // Execute callback
			}),
		)
	}

	// Initialize aws dynamodb client
	{
		//var sess *session.Session
		var err error
		if cfg.Endpoint == "" {
			//sess, err = session.NewSession(&aws.Config{
			//	Region: aws.String(cfg.Region),
			//	Credentials: credentials.NewStaticCredentials(
			//		cfg.APIKey, cfg.SecretKey, ""),
			//})
		} else {
			//sess, err = session.NewSession(&aws.Config{
			//	Region: aws.String(cfg.Region),
			//	Credentials: credentials.NewStaticCredentials(
			//		cfg.APIKey, cfg.SecretKey, ""),
			//	Endpoint: aws.String(cfg.Endpoint),
			//})
		}
		if err != nil {
			log.Fatal("NewDynamo new session fail, err:", err)
		}

		// Need to override default http client, default config has high time-wait issues
		// Reference: http://tleyden.github.io/blog/2016/11/21/tuning-the-go-http-client-library-for-load-testing/
		defaultRoundTripper := http.DefaultTransport
		defaultTransportPointer, ok := defaultRoundTripper.(*http.Transport)
		if !ok {
			log.Fatal("NewDynamo defaultRoundTripper not an *http.Transport")
		}
		defaultTransport := *defaultTransportPointer
		defaultTransport.MaxIdleConns = cfg.PoolSize
		defaultTransport.MaxIdleConnsPerHost = cfg.PoolSize

		logFile, err := os.Create("dynamodb.log")
		if err != nil {
			panic(err)
		}

		awsCfg, err := config.LoadDefaultConfig(context.TODO(), func(options *config.LoadOptions) error {
			// config.WithRegion(cfg.Region)
			// config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.APIKey, cfg.SecretKey, ""))
			// config.WithHTTPClient(&http.Client{Transport: &defaultTransport})
			// config.WithLogger(logging.NewStandardLogger(logFile))
			options.HTTPClient = &http.Client{Transport: &defaultTransport}
			return nil
			},
        	// Configure retry strategy
        	config.WithRetryer(func() aws.Retryer {
            	return retry.NewStandard(func(o *retry.StandardOptions) {
                	o.MaxAttempts = 3 // Set maximum retry attempts
            	})
        	}),
		)
		if err != nil {
			log.Fatalf("unable to load SDK config, %v", err)
		}
		d.svc = dynamodb.NewFromConfig(awsCfg, func(options *dynamodb.Options) {
			options.Region = cfg.Region
			if cfg.APIKey != "" &&  cfg.SecretKey != "" {
				options.Credentials = credentials.NewStaticCredentialsProvider(cfg.APIKey, cfg.SecretKey, cfg.Session)
			}
			options.DefaultsMode = aws.DefaultsModeStandard
			options.Logger = logging.NewStandardLogger(logFile)
			if cfg.Endpoint != "" {
				options.EndpointResolver = dynamodb.EndpointResolverFromURL(cfg.Endpoint)
			}
		})
	}

	return d
}

type dynamo[T any] struct {
	cfg  Config
	pool *go_pool.Pool[eventCB]
	svc  *dynamodb.Client
}

func (d *dynamo[T]) Exit() {
	d.pool.Exit()
}

func (d *dynamo[T]) scan(ctx context.Context, index string, fromKey any, limit int) (items []T, lastKey any, err error) {
	var scanFrom map[string]types.AttributeValue
	var ok bool
	if fromKey != nil {
		scanFrom, ok = fromKey.(map[string]types.AttributeValue)
		if !ok {
			return nil, nil, errors.New("fromKey is a invalid param")
		}
	}

	for {
		scanInput := &dynamodb.ScanInput{
			ConsistentRead:    aws.Bool(true), // For current scenarios, only consider consistent reads, read business does not use nosql
			ExclusiveStartKey: scanFrom,
			Limit:             aws.Int32(int32(limit)),
			TableName:         aws.String(d.cfg.TableName),
		}
		if index != "" {
			// Scan index table
			scanInput.IndexName = aws.String(index)
		}
		res, err := d.svc.Scan(ctx, scanInput)
		if err != nil {
			return nil, nil, errors.Wrap(err, "scan scan fail")
		}

		for i := range res.Items {
			var item T
			err = attributevalue.UnmarshalMap(res.Items[i], &item)
			if err != nil {
				return nil, nil, errors.Wrap(err, "scan unmarshal fail")
			}
			items = append(items, item)
		}

		lastKey = nil
		if len(res.LastEvaluatedKey) == 0 {
			// No more data, iteration complete
			break
		}
		lastKey = res.LastEvaluatedKey

		if len(items) >= limit {
			// Already got desired amount of data, exit loop
			break
		}
		scanFrom = res.LastEvaluatedKey
	}
	return items, lastKey, nil
}

func (d *dynamo[T]) CreateTable(ctx context.Context, input *dynamodb.CreateTableInput) (
	output *dynamodb.CreateTableOutput, err error) {
	doneCh := make(chan struct{})
	d.pool.New(func() {
		output, err = d.svc.CreateTable(ctx, input)
		doneCh <- struct{}{}
	})
	<-doneCh
	return
}

func (d *dynamo[T]) DeleteTable(ctx context.Context, input *dynamodb.DeleteTableInput) (
	output *dynamodb.DeleteTableOutput, err error) {
	doneCh := make(chan struct{})
	d.pool.New(func() {
		output, err = d.svc.DeleteTable(ctx, input)
		doneCh <- struct{}{}
	})
	<-doneCh
	return
}

func (d *dynamo[T]) ScanTable(ctx context.Context, fromKey any, limit int) (items []T, lastKey any, err error) {
	doneCh := make(chan struct{})
	d.pool.New(func() {
		items, lastKey, err = d.scan(ctx, "", fromKey, limit)
		doneCh <- struct{}{}
	})
	<-doneCh
	return
}

func (d *dynamo[T]) ScanIndex(ctx context.Context, index string, fromKey any, limit int) (items []T, lastKey any, err error) {
	doneCh := make(chan struct{})
	d.pool.New(func() {
		items, lastKey, err = d.scan(ctx, index, fromKey, limit)
		doneCh <- struct{}{}
	})
	<-doneCh
	return
}

func (d *dynamo[T]) QueryItem(ctx context.Context, key map[string]any) (exist bool, retItem T, err error) {
	retCh := make(chan struct {
		Exist   bool
		RetItem T
		Err     error
	})
	d.pool.New(func() {
		var ret struct {
			Exist   bool
			RetItem T
			Err     error
		}
		defer func() {
			retCh <- ret
		}()

		keyData, err := attributevalue.MarshalMap(key)
		if err != nil {
			ret.Err = errors.Wrap(err, "QueryItem marshal")
			return
		}
		res, err := d.svc.GetItem(ctx, &dynamodb.GetItemInput{
			ConsistentRead: aws.Bool(true),
			TableName:      aws.String(d.cfg.TableName),
			Key:            keyData,
		})
		if err != nil {
			ret.Err = errors.Wrap(err, "QueryItem getItem")
			return
		}
		if res.Item == nil || len(res.Item) == 0 {
			return
		}
		var item T
		err = attributevalue.UnmarshalMap(res.Item, &item)
		if err != nil {
			ret.Err = errors.Wrap(err, "QueryItem unmarshal")
			return
		}
		ret.Exist = true
		ret.RetItem = item
	})
	ret := <-retCh
	return ret.Exist, ret.RetItem, ret.Err
}

func (d *dynamo[T]) QueryItems(ctx context.Context, index, condition string, expression map[string]any, fromKey any, limit int) (items []T, lastKey any, err error) {
	doneCh := make(chan struct{})
	d.pool.New(func() {
		items, lastKey, err = d.queryItems(ctx, index, condition, expression, fromKey, limit)
		doneCh <- struct{}{}
	})
	<-doneCh
	return
}

func (d *dynamo[T]) queryItems(ctx context.Context, index string, condition string, expression map[string]any, fromKey any, limit int) (items []T, lastKey any, err error) {
	var queryFrom map[string]types.AttributeValue
	var ok bool
	if fromKey != nil {
		queryFrom, ok = fromKey.(map[string]types.AttributeValue)
		if !ok {
			return nil, nil, errors.New("fromKey is a invalid param")
		}
	}

	for {
		attributeValues, err := attributevalue.MarshalMap(expression)
		if err != nil {
			return nil, nil, errors.Wrap(err, "QueryItems marshal")
		}
		queryInput := &dynamodb.QueryInput{
			TableName:                 aws.String(d.cfg.TableName),
			ConsistentRead:            aws.Bool(false), // Not suitable to use strong consistency when scanning large amounts of data
			ExclusiveStartKey:         queryFrom,
			KeyConditionExpression:    aws.String(condition),
			ExpressionAttributeValues: attributeValues,
			Limit:                     aws.Int32(int32(limit)),
		}
		if index != "" {
			// Scan index table
			queryInput.IndexName = aws.String(index)
		}
		res, err := d.svc.Query(ctx, queryInput)
		if err != nil {
			return nil, nil, errors.Wrap(err, "QueryItems query fail")
		}

		for i := range res.Items {
			var item T
			err = attributevalue.UnmarshalMap(res.Items[i], &item)
			if err != nil {
				return nil, nil, errors.Wrap(err, "QueryItems unmarshal fail")
			}
			items = append(items, item)
		}

		lastKey = nil
		if len(res.LastEvaluatedKey) == 0 {
			// No more data, iteration complete
			break
		}
		lastKey = res.LastEvaluatedKey

		if len(items) >= limit {
			// Already got desired amount of data, exit loop
			break
		}
		queryFrom = res.LastEvaluatedKey
	}
	return items, lastKey, nil
}

func (d *dynamo[T]) QueryItemWithTable(ctx context.Context, key map[string]any, table string) (exist bool, retItem T, err error) {
	retCh := make(chan struct {
		Exist   bool
		RetItem T
		Err     error
	})
	d.pool.New(func() {
		var ret struct {
			Exist   bool
			RetItem T
			Err     error
		}
		defer func() {
			retCh <- ret
		}()

		keyData, err := attributevalue.MarshalMap(key)
		if err != nil {
			ret.Err = errors.Wrap(err, "QueryItem marshal")
			return
		}
		res, err := d.svc.GetItem(ctx, &dynamodb.GetItemInput{
			ConsistentRead: aws.Bool(true),
			TableName:      &table,
			Key:            keyData,
		})
		if err != nil {
			ret.Err = errors.Wrap(err, "QueryItem getItem")
			return
		}
		if res.Item == nil || len(res.Item) == 0 {
			return
		}
		var item T
		err = attributevalue.UnmarshalMap(res.Item, &item)
		if err != nil {
			ret.Err = errors.Wrap(err, "QueryItem unmarshal")
			return
		}
		ret.Exist = true
		ret.RetItem = item
	})
	ret := <-retCh
	return ret.Exist, ret.RetItem, ret.Err
}

func (d *dynamo[T]) QueryItemsWithTable(ctx context.Context, qcs QueryItemCondition) (items []T, lastKey any, err error) {
	doneCh := make(chan struct{})
	d.pool.New(func() {
		items, lastKey, err = d.queryItemsWithTable(ctx, qcs)
		doneCh <- struct{}{}
	})
	<-doneCh
	return
}

func (d *dynamo[T]) queryItemsWithTable(ctx context.Context, qc QueryItemCondition) (items []T, lastKey any, err error) {
	var queryFrom map[string]types.AttributeValue
	var ok bool
	if qc.FromKey != nil {
		queryFrom, ok = qc.FromKey.(map[string]types.AttributeValue)
		if !ok {
			return nil, nil, errors.New("fromKey is a invalid param")
		}
	}

	for {
		attributeValues, err := attributevalue.MarshalMap(qc.Expression)
		if err != nil {
			return nil, nil, errors.Wrap(err, "QueryItems marshal")
		}
		queryInput := &dynamodb.QueryInput{
			TableName:                 aws.String(qc.TableName),
			ConsistentRead:            aws.Bool(false), // Not suitable to use strong consistency when scanning large amounts of data
			ExclusiveStartKey:         queryFrom,
			KeyConditionExpression:    aws.String(qc.Condition),
			ExpressionAttributeValues: attributeValues,
			Limit:                     aws.Int32(int32(qc.Limit)),
		}
		if qc.Index != "" {
			// Scan index table
			queryInput.IndexName = aws.String(qc.Index)
		}
		res, err := d.svc.Query(ctx, queryInput)
		if err != nil {
			return nil, nil, errors.Wrap(err, "QueryItems query fail")
		}

		for i := range res.Items {
			var item T
			err = attributevalue.UnmarshalMap(res.Items[i], &item)
			if err != nil {
				return nil, nil, errors.Wrap(err, "QueryItems unmarshal fail")
			}
			items = append(items, item)
		}

		lastKey = nil
		if len(res.LastEvaluatedKey) == 0 {
			// No more data, iteration complete
			break
		}
		lastKey = res.LastEvaluatedKey

		if len(items) >= qc.Limit {
			// Already got desired amount of data, exit loop
			break
		}
		queryFrom = res.LastEvaluatedKey
	}
	return items, lastKey, nil
}

func (d *dynamo[T]) batchWriteItemWithRetry(ctx context.Context, request map[string][]types.WriteRequest) (err error) {
	for len(request) > 0 {
		res, err := d.svc.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems:                request,
			ReturnConsumedCapacity:      types.ReturnConsumedCapacityTotal,
			ReturnItemCollectionMetrics: "NONE",
		})
		if err != nil {
			return errors.Wrap(err, "batchWriteItemWithRetry batch write")
		}
		if d.cfg.Debug {
			log.Println("[database/dynamo]batchWriteItemWithRetry resource consume: ", consumedInfo(res.ConsumedCapacity), ", table:", d.cfg.TableName)
		}
		if len(res.UnprocessedItems) == 0 {
			break
		}
		request = res.UnprocessedItems
	}
	return nil
}

func (d *dynamo[T]) InsertItems(ctx context.Context, insertInfos []InsertInfo[T]) (err error) {
	maxBatch := 25
	for i := 0; i < len(insertInfos); i += maxBatch {
		start, end := i, i+maxBatch
		if end > len(insertInfos) {
			end = len(insertInfos)
		}
		err = d.insertItems(ctx, insertInfos[start:end])
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *dynamo[T]) insertItems(ctx context.Context, insertInfos []InsertInfo[T]) (err error) {
	if len(insertInfos) == 0 {
		return
	}
	errCh := make(chan error)
	d.pool.New(func() {
		var requests []types.WriteRequest
		for i := range insertInfos {
			var data map[string]types.AttributeValue
			data, err := attributevalue.MarshalMapWithOptions(insertInfos[i].Item)
			if err != nil {
				errCh <- errors.Wrap(err, "InsertItems marshal fail")
				return
			}
			requests = append(requests, types.WriteRequest{
				PutRequest: &types.PutRequest{
					Item: data,
				},
			})
		}
		err := d.batchWriteItemWithRetry(ctx, map[string][]types.WriteRequest{
			d.cfg.TableName: requests,
		})
		if err != nil {
			errCh <- errors.Wrap(err, "InsertItems write fail")
			return
		}
		errCh <- nil
	})
	return <-errCh
}

// insertInfos cannot exceed 100, otherwise dynamodb will report an error
func (d *dynamo[T]) TxInsertItems(ctx context.Context, insertInfos []TxInsertInfo[T], opts ...Option) (err error) {
	if len(insertInfos) > 100 {
		return errors.New("insertInfos must have length less than or equal to 100")
	}
	errCh := make(chan error)
	d.pool.New(func() {
		optsIn := options{}
		for _, opt := range opts {
			opt.apply(&optsIn)
		}
		err := d.txInsertItems(ctx, insertInfos, optsIn)
		if err != nil {
			errCh <- errors.Wrap(err, "TxInsertItems insert fail")
			return
		}
		errCh <- nil
	})
	return <-errCh
}

func (d *dynamo[T]) getInsertTx(item any, conditions Conditions, tableName string) (tx types.TransactWriteItem, err error) {
	var data map[string]types.AttributeValue

	data, err = attributevalue.MarshalMap(item)
	if err != nil {
		err = errors.Wrap(err, "getInsertPut marshal data")
		return
	}

	put := &types.Put{
		Item:      data,
		TableName: aws.String(tableName),
	}

	names, values := make(map[string]string), make(map[string]any)
	conditionExpression := getConditionExpression(conditions, names, values)
	if conditionExpression != "" {
		put.ConditionExpression = aws.String(conditionExpression)
		put.ExpressionAttributeNames = names
		var valueData map[string]types.AttributeValue
		if len(values) > 0 {
			valueData, err = attributevalue.MarshalMap(values)
			if err != nil {
				err = errors.Wrap(err, "getInsertPut marshal conds value")
				return
			}
		}
		put.ExpressionAttributeValues = valueData
	}

	tx = types.TransactWriteItem{
		Put: put,
	}
	return
}

func (d *dynamo[T]) txInsertItems(ctx context.Context, insertInfos []TxInsertInfo[T], opts options) (err error) {
	items := make([]types.TransactWriteItem, 0)
	for i := range insertInfos {
		tx, err := d.getInsertTx(insertInfos[i].Item, insertInfos[i].Conditions, d.cfg.TableName)
		if err != nil {
			return err
		}
		items = append(items, tx)
	}
	// Maximum 100 items supported
	input := dynamodb.TransactWriteItemsInput{
		TransactItems: items,
	}
	if opts.txID != "" {
		input.ClientRequestToken = aws.String(opts.txID)
	}
	res, err := d.svc.TransactWriteItems(ctx, &input)
	if err == nil && d.cfg.Debug {
		log.Println("[database/dynamo]txInsertItems resource consume", consumedInfo(res.ConsumedCapacity), ", table:", d.cfg.TableName)
	}
	return err
}

// insertItems and updateItems total cannot exceed 100, otherwise dynamodb will report an error
func (d *dynamo[T]) TxRawExec(ctx context.Context, insertItems []TxRawInsert, updateItems []TxRawUpdate, opts ...Option) (err error) {
	if len(insertItems)+len(updateItems) > 100 {
		return errors.New("insertItems and updateItems must have length less than or equal to 100")
	}
	errCh := make(chan error)
	d.pool.New(func() {
		optsIn := options{}
		for _, opt := range opts {
			opt.apply(&optsIn)
		}
		err := d.txRawExec(ctx, insertItems, updateItems, optsIn)
		if err != nil {
			errCh <- errors.Wrap(err, "TxRawExec insert fail")
			return
		}
		errCh <- nil
	})
	return <-errCh
}

func (d *dynamo[T]) getUpdateTx(info UpdateInfo, tableName string) (tx types.TransactWriteItem, err error) {
	names, values := make(map[string]string), make(map[string]any)

	updateExpression := getUpdateExpression(info.Sets, info.Removes, names, values)
	conditionExpression := getConditionExpression(info.Conditions, names, values)

	var keyData map[string]types.AttributeValue
	var valueData map[string]types.AttributeValue
	keyData, err = attributevalue.MarshalMap(info.Key)
	if err != nil {
		err = errors.Wrap(err, "getUpdateTx marshal key")
		return
	}
	if len(values) > 0 {
		valueData, err = attributevalue.MarshalMap(values)
		if err != nil {
			err = errors.Wrap(err, "getUpdateTx marshal values")
			return
		}
	}

	update := &types.Update{
		Key:                       keyData,
		TableName:                 aws.String(tableName),
		UpdateExpression:          aws.String(updateExpression),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: valueData,
	}
	if conditionExpression != "" {
		update.ConditionExpression = aws.String(conditionExpression)
	}

	tx = types.TransactWriteItem{
		Update: update,
	}
	return
}

func (d *dynamo[T]) txRawExec(ctx context.Context, insertItems []TxRawInsert, updateItems []TxRawUpdate, opts options) (err error) {
	items := make([]types.TransactWriteItem, 0)
	for i := range insertItems {
		tx, err := d.getInsertTx(insertItems[i].Item, insertItems[i].Conditions, insertItems[i].TableName)
		if err != nil {
			return err
		}
		items = append(items, tx)
	}
	for i := range updateItems {
		tx, err := d.getUpdateTx(updateItems[i].UpdateInfo, updateItems[i].TableName)
		if err != nil {
			return err
		}
		items = append(items, tx)
	}

	// Maximum 100 items supported
	input := dynamodb.TransactWriteItemsInput{
		TransactItems: items,
	}
	if opts.txID != "" {
		input.ClientRequestToken = aws.String(opts.txID)
	}
	res, err := d.svc.TransactWriteItems(ctx, &input)
	if err == nil && d.cfg.Debug {
		log.Println("[database/dynamo]txRawExec resource consume", consumedInfo(res.ConsumedCapacity), ", table:", d.cfg.TableName)
	}
	return err
}

func (d *dynamo[T]) DeleteItems(ctx context.Context, keys []map[string]any) (err error) {
	if len(keys) == 0 {
		return
	}
	errCh := make(chan error)
	d.pool.New(func() {
		var requests []types.WriteRequest
		for i := range keys {
			var data map[string]types.AttributeValue
			data, err = attributevalue.MarshalMap(keys[i])
			if err != nil {
				errCh <- errors.Wrap(err, "DeleteItems marshal fail")
				return
			}
			requests = append(requests, types.WriteRequest{
				DeleteRequest: &types.DeleteRequest{
					Key: data,
				},
			})
		}
		err := d.batchWriteItemWithRetry(ctx, map[string][]types.WriteRequest{
			d.cfg.TableName: requests,
		})
		if err != nil {
			errCh <- errors.Wrap(err, "DeleteItems write fail")
			return
		}
		errCh <- nil
	})
	return <-errCh
}

func (d *dynamo[T]) UpdateItem(ctx context.Context, update UpdateInfo) (err error) {
	errCh := make(chan error)
	d.pool.New(func() {
		key, sets, removes := update.Key, update.Sets, update.Removes
		names, values := make(map[string]string), make(map[string]any)

		updateExpression := getUpdateExpression(sets, removes, names, values)
		conditionExpression := getConditionExpression(update.Conditions, names, values)

		keyData, err := attributevalue.MarshalMap(key)
		if err != nil {
			errCh <- errors.Wrap(err, "UpdateItem marshal key")
			return
		}
		var valueData map[string]types.AttributeValue
		if len(values) > 0 {
			valueData, err = attributevalue.MarshalMap(values)
			if err != nil {
				errCh <- errors.Wrap(err, "UpdateItem marshal values")
				return
			}
		}

		updateInput := &dynamodb.UpdateItemInput{
			TableName:                 aws.String(d.cfg.TableName),
			Key:                       keyData,
			ExpressionAttributeNames:  names,
			ExpressionAttributeValues: valueData,
			UpdateExpression:          aws.String(updateExpression),
		}
		if update.TableName != "" {
			updateInput.TableName = aws.String(update.TableName)
		}
		if conditionExpression != "" {
			updateInput.ConditionExpression = aws.String(conditionExpression)
		}
		_, err = d.svc.UpdateItem(ctx, updateInput)
		if err != nil {
			errCh <- errors.Wrap(err, "UpdateItem update item")
			return
		}
		errCh <- nil
	})
	return <-errCh
}

func (d *dynamo[T]) UpdateItems(ctx context.Context, updates []UpdateInfo) (succKeys []map[string]any, err error) {
	resCh := make(chan interface{}, len(updates))
	{
		// Concurrent update requests
		wg := sync.WaitGroup{}
		for i := range updates {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				err := d.UpdateItem(ctx, updates[i])
				if err != nil {
					resCh <- err
					return
				}
				resCh <- updates[i].Key
			}(i)
		}
		wg.Wait()
	}
	close(resCh)

	var msgs []string
	for {
		e, ok := <-resCh
		if !ok {
			// All request results processed
			break
		}
		if err, ok := e.(error); ok {
			// Request failed
			msgs = append(msgs, err.Error())
			continue
		}
		// Request succeeded
		succKeys = append(succKeys, e.(map[string]any))
	}

	if len(msgs) != 0 {
		err = errors.New(strings.Join(msgs, " | "))
	}
	return
}

// updates cannot exceed 100, otherwise dynamodb will report an error
func (d *dynamo[T]) TxUpdateItems(ctx context.Context, updates []UpdateInfo, opts ...Option) (err error) {
	if len(updates) > 100 {
		return errors.New("updates must have length less than or equal to 100")
	}
	errCh := make(chan error)
	d.pool.New(func() {
		optsIn := options{}
		for _, opt := range opts {
			opt.apply(&optsIn)
		}
		err := d.txUpdateItems(ctx, updates, optsIn)
		if err != nil {
			errCh <- errors.Wrap(err, "TxUpdateItems update fail")
			return
		}
		errCh <- nil
	})
	return <-errCh
}

func (d *dynamo[T]) txUpdateItems(ctx context.Context, updates []UpdateInfo, opts options) (err error) {
	items := make([]types.TransactWriteItem, 0)
	for i := range updates {
		tx, err := d.getUpdateTx(updates[i], d.cfg.TableName)
		if err != nil {
			return err
		}
		items = append(items, tx)
	}
	// Maximum 100 items supported
	input := &dynamodb.TransactWriteItemsInput{
		TransactItems:          items,
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	}
	if opts.txID != "" {
		input.ClientRequestToken = aws.String(opts.txID)
	}
	res, err := d.svc.TransactWriteItems(ctx, input)
	if err == nil && d.cfg.Debug {
		log.Println("[database/dynamo]txUpdateItems resource consume", consumedInfo(res.ConsumedCapacity), ", table:", d.cfg.TableName)
	}
	return err
}
