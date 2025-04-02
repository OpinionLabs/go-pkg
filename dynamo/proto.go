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
// 	NameTransactionID = "TRANSACTION_ID" // Transaction ID name, used for idempotent interfaces
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
	TableName          string // Used to override the default table name
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

// Conditions represents preconditions for write and update operations, see condition expressions documentation:
//
//  https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html
//  https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
type Conditions struct {
	AttributeExists           map[string]any // Attribute exists, used for update scenarios
	AttributeNotExists        map[string]any // Attribute does not exist, used for write scenarios
	AttributeEqual            map[string]any // Attribute equals a specific value, used for update scenarios
	AttributeNotExistsOrEqual map[string]any // Attribute does not exist or equals specified value, used for write scenarios
}

// Streams related types below
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

// Mimic DynamoDB shard types
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
