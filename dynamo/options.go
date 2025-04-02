package dynamo

type options struct {
	txID string // Used as idempotency ID
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
