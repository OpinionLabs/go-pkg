package httpclient

type Option interface {
	apply(o *httpClientOptions)
}

type httpClientOptions struct {
	proxies []string // http使用的代理, 元素格式是http://host:port
}

type proxiesOption []string

func (p proxiesOption) apply(o *httpClientOptions) {
	o.proxies = append(o.proxies, p...)
}

func WithProxies(addrs []string) Option {
	return proxiesOption(addrs)
}
