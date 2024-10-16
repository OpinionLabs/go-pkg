package dynamo

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/aws/smithy-go/logging"
	"github.com/pkg/errors"
	"github.com/ChewZ-life/go-pkg/concurrency/go_pool"
)

var _ Streams = (*streams)(nil)

// Streams 封装aws dynamodbstreams的sdk, 一个表对应一个对象, 使用者可并发调用
type Streams interface {
	ListStreams(ctx context.Context, fromKey any, limit int) (items []Stream, lastKey any, err error)
	DescribeStream(ctx context.Context, streamArn string, fromKey any, limit int) (items []StreamDescription, lastKey any, err error)
	GetShardIterator(ctx context.Context, iteratorPut ShardIterator) (shardIterator any, err error)
	GetRecords(ctx context.Context, shardIterator any, limit int) (items []any, err error)

	// Exit 退出相关连接池
	Exit()
}

func NewStreams(cfg Config) Streams {
	d := &streams{
		cfg: cfg,
	}

	// 初始化连接池
	{
		d.pool = go_pool.NewPool(
			go_pool.WithSize[eventCB](cfg.PoolSize),
			go_pool.WithTaskCB(func(cb eventCB, i int) {
				cb() // 执行一下回调就可以
			}),
		)
	}

	// 初始化aws dynamodb客户端
	{
		//// 需要覆盖默认的http的客户端, 默认的配置会有time-wait过高问题, 原因参考下面的链接
		//// http://tleyden.github.io/blog/2016/11/21/tuning-the-go-http-client-library-for-load-testing/
		defaultRoundTripper := http.DefaultTransport
		defaultTransportPointer, ok := defaultRoundTripper.(*http.Transport)
		if !ok {
			log.Fatal("NewStreams defaultRoundTripper not an *http.Transport")
		}
		defaultTransport := *defaultTransportPointer
		defaultTransport.MaxIdleConns = cfg.PoolSize
		defaultTransport.MaxIdleConnsPerHost = cfg.PoolSize

		logFile, err := os.Create("streams.log")
		if err != nil {
			panic(err)
		}

		awsCfg, err := config.LoadDefaultConfig(context.TODO(), func(options *config.LoadOptions) error {
			options.HTTPClient = &http.Client{Transport: &defaultTransport}
			return nil
		})
		if err != nil {
			log.Fatalf("unable to load SDK config, %v", err)
		}
		d.svc = dynamodbstreams.NewFromConfig(awsCfg, func(options *dynamodbstreams.Options) {
			options.Region = cfg.Region
			options.Credentials = credentials.NewStaticCredentialsProvider(cfg.APIKey, cfg.SecretKey, cfg.Session)
			options.DefaultsMode = aws.DefaultsModeStandard
			options.Logger = logging.NewStandardLogger(logFile)
			if cfg.Endpoint != "" {
				options.EndpointResolver = dynamodbstreams.EndpointResolverFromURL(cfg.Endpoint)
			}
		})
	}

	return d
}

type streams struct {
	cfg  Config
	pool *go_pool.Pool[eventCB]
	svc  *dynamodbstreams.Client
}

func (d *streams) Exit() {
	d.pool.Exit()
}

func (d *streams) listStreams(ctx context.Context, fromKey any, limit int) (items []Stream, lastKey any, err error) {
	var listFrom string
	var ok bool
	if fromKey != nil {
		listFrom, ok = fromKey.(string)
		if !ok {
			return nil, nil, errors.New("fromKey is a invalid param")
		}
	}

	for {
		listInput := &dynamodbstreams.ListStreamsInput{
			Limit: aws.Int32(int32(limit)),
		}
		if listFrom != "" {
			listInput.ExclusiveStartStreamArn = aws.String(listFrom)
		}
		// 不带表名，可以获取所有流
		if d.cfg.TableName != "" {
			listInput.TableName = aws.String(d.cfg.TableName)
		}

		res, err := d.svc.ListStreams(ctx, listInput)
		if err != nil {
			return nil, nil, errors.Wrap(err, "list streams fail")
		}

		for _, stream := range res.Streams {
			items = append(items, Stream{
				Arn:       aws.ToString(stream.StreamArn),
				Label:     aws.ToString(stream.StreamLabel),
				TableName: aws.ToString(stream.TableName),
			})
		}

		lastKey = nil
		if res.LastEvaluatedStreamArn == nil {
			// 后面没有数据, 表示遍历完成
			break
		}
		lastKey = res.LastEvaluatedStreamArn

		if len(items) >= limit {
			// 已经取到想要的数据量, 退出循环
			break
		}
		listFrom = aws.ToString(res.LastEvaluatedStreamArn)
	}
	return items, lastKey, nil
}

func (d *streams) ListStreams(ctx context.Context, fromKey any, limit int) (items []Stream, lastKey any, err error) {
	doneCh := make(chan struct{})
	d.pool.New(func() {
		items, lastKey, err = d.listStreams(ctx, fromKey, limit)
		doneCh <- struct{}{}
	})
	<-doneCh
	return
}

func (d *streams) describeStream(ctx context.Context, streamArn string, fromKey any, limit int) (items []StreamDescription, lastKey any, err error) {
	var desFrom string
	var ok bool
	if fromKey != nil {
		desFrom, ok = fromKey.(string)
		if !ok {
			return nil, nil, errors.New("fromKey is a invalid param")
		}
	}

	for {
		desInput := &dynamodbstreams.DescribeStreamInput{
			StreamArn: aws.String(streamArn),
			Limit:     aws.Int32(int32(limit)),
		}
		if desFrom != "" {
			desInput.ExclusiveStartShardId = aws.String(desFrom)
		}

		res, err := d.svc.DescribeStream(ctx, desInput)
		if err != nil {
			return nil, nil, errors.Wrap(err, "describe stream fail")
		}

		des := res.StreamDescription
		if des == nil {
			break
		}
		var shards []Shard
		for _, shard := range des.Shards {
			shards = append(shards, Shard{
				ShardId:                aws.ToString(shard.ShardId),
				ParentShardId:          aws.ToString(shard.ParentShardId),
				StartingSequenceNumber: aws.ToString(shard.SequenceNumberRange.StartingSequenceNumber),
				EndingSequenceNumber:   aws.ToString(shard.SequenceNumberRange.EndingSequenceNumber),
			})
		}

		items = append(items, StreamDescription{
			LastShardId: aws.ToString(des.LastEvaluatedShardId),
			Shards:      shards,
		})

		lastKey = nil
		if des.LastEvaluatedShardId == nil {
			// 后面没有数据, 表示遍历完成
			break
		}
		lastKey = des.LastEvaluatedShardId

		if len(items) >= limit {
			// 已经取到想要的数据量, 退出循环
			break
		}
		desFrom = aws.ToString(des.LastEvaluatedShardId)
	}
	return items, lastKey, nil
}

func (d *streams) DescribeStream(ctx context.Context, streamArn string, fromKey any, limit int) (items []StreamDescription, lastKey any, err error) {
	doneCh := make(chan struct{})
	d.pool.New(func() {
		items, lastKey, err = d.describeStream(ctx, streamArn, fromKey, limit)
		doneCh <- struct{}{}
	})
	<-doneCh
	return
}

func (d *streams) getShardIterator(ctx context.Context, iteratorPut ShardIterator) (shardIterator any, err error) {
	shardInput := &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         aws.String(iteratorPut.StreamArn),
		ShardId:           aws.String(iteratorPut.ShardId),
		ShardIteratorType: types.ShardIteratorType(iteratorPut.ShardIteratorType),
	}
	// 没有该参数，会从头开始读取
	if iteratorPut.SequenceNumber != "" {
		shardInput.SequenceNumber = aws.String(iteratorPut.SequenceNumber)
	}

	res, err := d.svc.GetShardIterator(ctx, shardInput)
	if err != nil {
		return nil, errors.Wrap(err, "get shard iterator fail")
	}
	shardIterator = aws.ToString(res.ShardIterator)
	return
}

func (d *streams) GetShardIterator(ctx context.Context, iteratorPut ShardIterator) (shardIterator any, err error) {
	doneCh := make(chan struct{})
	d.pool.New(func() {
		shardIterator, err = d.getShardIterator(ctx, iteratorPut)
		doneCh <- struct{}{}
	})
	<-doneCh
	return
}

func (d *streams) getRecords(ctx context.Context, shardIterator any, limit int) (items []any, err error) {
	if shardIterator == nil {
		return nil, errors.New("shardIterator is nil")
	}
	for {
		shard, ok := shardIterator.(string)
		if !ok {
			return nil, errors.New("shardIterator is a invalid param")
		}
		recordInput := &dynamodbstreams.GetRecordsInput{
			ShardIterator: aws.String(shard),
			Limit:         aws.Int32(int32(limit)),
		}

		records, err := d.svc.GetRecords(ctx, recordInput)
		if err != nil {
			return nil, errors.Wrap(err, "get records fail")
		}
		for _, record := range records.Records {
			items = append(items, record)
		}

		if records.NextShardIterator == nil {
			// 后面没有数据, 表示遍历完成
			break
		}

		if len(items) >= limit {
			// 已经取到想要的数据量, 退出循环
			break
		}
		shardIterator = aws.ToString(records.NextShardIterator)
	}
	return
}

func (d *streams) GetRecords(ctx context.Context, shardIterator any, limit int) (items []any, err error) {
	doneCh := make(chan struct{})
	d.pool.New(func() {
		items, err = d.getRecords(ctx, shardIterator, limit)
		doneCh <- struct{}{}
	})
	<-doneCh
	return
}
