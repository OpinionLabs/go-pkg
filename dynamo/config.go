package dynamo

type Config struct {
	Region    string `mapstructure:"region" json:"region"`
	APIKey    string `mapstructure:"api_key" json:"api_key"`
	SecretKey string `mapstructure:"secret_key" json:"secret_key"`
	Endpoint  string `mapstructure:"endpoint" json:"endpoint"`
	PoolSize  int    `mapstructure:"pool_size" json:"pool_size"`
	TableName string `mapstructure:"table_name" json:"table_name"`
	Session   string `mapstructure:"session" json:"session"`
	Debug     bool   `mapstructure:"debug" json:"mapstructure"`
}
