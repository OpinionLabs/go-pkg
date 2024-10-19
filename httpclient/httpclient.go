package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/alibaba/sentinel-golang/core/base"
	"github.com/pkg/errors"
)

func Get[T interface{}](ctx context.Context, urlStr string, opts ...Option) (responseInfo T, err error) {
	var response *http.Response
	tp := time.Now()
	defer func() {
		handleHttpRespErr(urlStr, tp, response, err)
	}()

	ctx, cancel := handleCtxDeadline(ctx)
	defer cancel()

	cli := http.Client{}
	handleHttpOpt(&cli, opts...)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		err = errors.Wrap(err, "Get new req")
		return
	}
	response, err = cli.Do(req)
	if err != nil {
		err = errors.Wrap(err, "Get send http")
		return
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		err = errors.Errorf("Get http expect, statusCode: %d", response.StatusCode)
		err = errno.New(errno.InternalServerError, err.Error())
		return
	}

	responseData, err := ioutil.ReadAll(response.Body)
	if err != nil {
		err = errors.Wrap(err, "Get read response")
		err = errno.New(errno.InternalServerError, err.Error())
		return
	}

	d := json.NewDecoder(bytes.NewReader(responseData))
	d.UseNumber()
	err = d.Decode(&responseInfo)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("Get unmarshal response %s", string(responseData)))
		err = errno.New(errno.InternalServerError, err.Error())
		return
	}

	return
}

func Post[T interface{}](ctx context.Context, urlStr string, header http.Header, body []byte, opts ...Option) (responseInfo T,
	err error) {
	var response *http.Response
	tp := time.Now()
	defer func() {
		handleHttpRespErr(urlStr, tp, response, err)
	}()

	ctx, cancel := handleCtxDeadline(ctx)
	defer cancel()

	cli := http.Client{}
	handleHttpOpt(&cli, opts...)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, urlStr, bytes.NewBuffer(body))
	if err != nil {
		err = errors.Wrap(err, "Post new req")
		return
	}
	req.Header = header

	response, err = cli.Do(req)
	if err != nil {
		err = errors.Wrap(err, "Post send http")
		return
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		err = errors.Errorf("Post http expect, statusCode: %d", response.StatusCode)
		err = errno.New(errno.InternalServerError, err.Error())
		return
	}

	responseData, err := ioutil.ReadAll(response.Body)
	if err != nil {
		err = errors.Wrap(err, "Post read response")
		err = errno.New(errno.InternalServerError, err.Error())
		return
	}

	err = json.Unmarshal(responseData, &responseInfo)
	if err != nil {
		err = errors.Wrap(err, "Post unmarshal response")
		err = errno.New(errno.InternalServerError, err.Error())
		return
	}
	return
}
