package dynamo

type options struct {
	txID string // 用于作为幂等的id
}

type Option interface {
	apply(*options)
}

type txIDOption string

func (t txIDOption) apply(opts *options) {
	opts.txID = string(t)
}

func WithTxID(txID string) Option {
	return txIDOption(txID)
}
