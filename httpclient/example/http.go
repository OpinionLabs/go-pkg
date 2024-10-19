package main

import (
    "strconv"
    "fmt"
   	"context"
    "net/url"
	"github.com/shopspring/decimal"
	"github.com/ChewZ-life/go-pkg/httpclient"
)


type klineResp struct {
	Errno  int      `json:"errno"`
	Errmsg string   `json:"errmsg"`
	Result klineRet `json:"result,omitempty"` // Result解析为result,忽略空值
}

type klineRet struct {
	Symbol string      `json:"symbol"`
	Period string      `json:"period"`
	Ts     int64       `json:"ts"`
	Data   []klineData `json:"data"`
}

type klineData struct {
	Id     int64           `json:"id"`
	Amount decimal.Decimal `json:"amount"`
	Count  int             `json:"count"`
	Open   decimal.Decimal `json:"open"`
	Close  decimal.Decimal `json:"close"`
	Low    decimal.Decimal `json:"low"`
	High   decimal.Decimal `json:"high"`
	Vol    decimal.Decimal `json:"vol"`
}

func main() {
	api := ""

    params := url.Values{}
    params.Add("symbol", "")
    params.Add("project_id", strconv.Itoa(20))
    params.Add("period", "1hour")

	urlStr := fmt.Sprintf("http://%s%s?%s", "", api, params.Encode())

	resp, err := httpclient.Get[klineResp](context.TODO(), urlStr)
	if err != nil {
		return
	}
	fmt.Println("%v", resp)
}


