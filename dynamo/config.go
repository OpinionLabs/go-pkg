package dynamo

type Config struct {
	Region    string `mapstructure:"region" json:"region"`
	APIKey    string `mapstructure:"api_key" json:"api_key"`
	SecretKey string `mapstructure:"secret_key" json:"secret_key"`
	Endpoint  string `mapstructure:"endpoint" json:"endpoint"`     // 上面几个跟aws路由和鉴权相关, endpoint只有本地测试才需要配置
	PoolSize  int    `mapstructure:"pool_size" json:"pool_size"`   // 连接池大小, 影响 内部并发协程数 和 aws服务连接数
	TableName string `mapstructure:"table_name" json:"table_name"` // 表名称
	Session   string `mapstructure:"session" json:"session"`
	Debug     bool   `mapstructure:"debug" json:"mapstructure"` // 是否打印调试日志
}
