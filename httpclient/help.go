package httpclient

import (
	"context"
	"net/http"
	"net/url"
	"time"
	"log"
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
		log.Println(err)
	}
}

// parseUrlResource 根据url解析资源名称,用于后续熔断
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
		// 从代理列表里面随机选择一个
		rndIdx := time.Now().UnixNano() % int64(len(opt.proxies))
		proxyUrl, err := url.Parse(opt.proxies[rndIdx])
		if err == nil {
			cli.Transport = &http.Transport{
				Proxy: http.ProxyURL(proxyUrl),
			}
		}
	}
}

// handleCtxDeadline 没有设置结束时间时,添加一个默认的3秒
func handleCtxDeadline(ctx context.Context) (context.Context, func()) {
	_, ok := ctx.Deadline()
	if ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, 3*time.Second)
}
