package httpclient

import (
	"context"
	"log"
	"net/http"
	"net/url"
	"time"
)

const (
	TimeoutMS = int64(1000)
)

func handleHttpRespErr(reqUrl string, tp time.Time, response *http.Response, err error) {
	if err != nil {
		log.Println(err)
	}
	if response != nil && response.StatusCode != http.StatusOK {
		log.Println(err)
	}

	cost := time.Since(tp).Milliseconds()
	if cost > TimeoutMS {
		log.Printf("url: %s request cost: %d.", reqUrl, cost)
	}
}

// parseUrlResource Parse resource name from URL for circuit breaking
func parseUrlResource(urlStr string) string {
	urlInfo, err := url.ParseRequestURI(urlStr)
	if err != nil {
		return ""
	}
	return urlInfo.Path
}

func handleHttpOpt(cli *http.Client, opts ...Option) {
	opt := httpClientOptions{}
	for i := range opts {
		opts[i].apply(&opt)
	}

	if len(opt.proxies) > 0 {
		// Randomly select a proxy from the proxy list
		rndIdx := time.Now().UnixNano() % int64(len(opt.proxies))
		proxyUrl, err := url.Parse(opt.proxies[rndIdx])
		if err == nil {
			cli.Transport = &http.Transport{
				Proxy: http.ProxyURL(proxyUrl),
			}
		}
	}
}

// handleCtxDeadline Add default 3-second timeout if no deadline is set
func handleCtxDeadline(ctx context.Context) (context.Context, func()) {
	_, ok := ctx.Deadline()
	if ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, 3*time.Second)
}
