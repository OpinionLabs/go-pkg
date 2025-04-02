package main

import (
	"context"
	"fmt"
	"net/url"
	"strconv"

	"github.com/ChewZ-life/go-pkg/httpclient"
	"github.com/shopspring/decimal"
)


type klineResp struct {
	Errno  int      `json:"errno"`
	Errmsg string   `json:"errmsg"`
	Result klineRet `json:"result,omitempty"`
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

	_, err := httpclient.Get[klineResp](context.TODO(), urlStr)
	if err != nil {
		return
	}
	// fmt.Println(urlStr)
	// fmt.Println(resp.Result.Data)
}


