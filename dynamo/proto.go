package dynamo

const (
	NamePlaceholderUpdatePrefix         = "un"
	NamePlaceholderRemovePrefix         = "rn"
	NamePlaceholderAttrExistsPrefix     = "aesn"
	NamePlaceholderAttrNotExistsPrefix  = "anesn"
	NamePlaceholderAttrEqualPrefix      = "aeqn"
	NamePlaceholderAttrNotExistsOrEqual = "anesoeqn"
)

const (
	ValuePlaceholderUpdatePrefix         = "uv"
	ValuePlaceholderAttrEqualPrefix      = "aeqv"
	ValuePlaceholderAttrNotExistsOrEqual = "anesoeqv"
)

// const (
// 	NameTransactionID = "TRANSACTION_ID" // 事务id名称, 用于幂等接口
// )

type InsertInfo[T any] struct {
	Item T
}

type TxInsertInfo[T any] struct {
	Item       T
	Conditions Conditions
}

type UpdateInfo struct {
	Key, Sets, Removes map[string]any
	Conditions         Conditions
	TableName          string // 用于覆盖默认的表名的
}

type TxRawInsert struct {
	TableName  string
	Item       any
	Conditions Conditions
}

type TxRawUpdate struct {
	TableName string
	UpdateInfo
}

// Conditions 表示写入和更新需要判断的前置条件, 条件表达式介绍参考:
//
//	https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html
//	https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
type Conditions struct {
	AttributeExists           map[string]any // 属性存在, 用于更新的场景
	AttributeNotExists        map[string]any // 属性不存在, 用于写入的场景
	AttributeEqual            map[string]any // 属性等于某个值, 用于更新的场景
	AttributeNotExistsOrEqual map[string]any // 属性不存在或等于指定的值, 用于写入的场景
}

// 下面是跟streams有关的
type Stream struct {
	Arn       string
	Label     string
	TableName string
}

type Shard struct {
	ShardId                string
	ParentShardId          string
	EndingSequenceNumber   string
	StartingSequenceNumber string
}

type StreamDescription struct {
	LastShardId string
	Shards      []Shard
}

type ShardIteratorType string

// 模仿dynamodb的shard类型
const (
	// Start reading exactly from the position denoted by a specific sequence number.
	ShardIteratorTypeAt ShardIteratorType = "AT_SEQUENCE_NUMBER"
	// Start reading right after the position denoted by a specific sequence number.
	ShardIteratorTypeAfter ShardIteratorType = "AFTER_SEQUENCE_NUMBER"
	// Start reading at the last (untrimmed) stream record, which is the oldest record in the shard. In DynamoDB Streams, there is a 24 hour limit on data retention. Stream records whose age exceeds this limit are subject to removal (trimming) from the stream.
	ShardIteratorTypeTrim ShardIteratorType = "TRIM_HORIZON"
	// Start reading just after the most recent stream record in the shard, so that you always read the most recent data in the shard.
	ShardIteratorTypeLatest ShardIteratorType = "LATEST"
)

type ShardIterator struct {
	StreamArn         string
	ShardId           string
	ShardIteratorType ShardIteratorType
	SequenceNumber    string
}
