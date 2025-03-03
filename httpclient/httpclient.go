package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

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
		return
	}

	responseData, err := io.ReadAll(response.Body)
	if err != nil {
		err = errors.Wrap(err, "Get read response")
		return
	}

	d := json.NewDecoder(bytes.NewReader(responseData))
	d.UseNumber()
	err = d.Decode(&responseInfo)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("Get unmarshal response %s", string(responseData)))
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
		return
	}

	responseData, err := io.ReadAll(response.Body)
	if err != nil {
		err = errors.Wrap(err, "Post read response")
		return
	}

	err = json.Unmarshal(responseData, &responseInfo)
	if err != nil {
		err = errors.Wrap(err, "Post unmarshal response")
		return
	}
	return
}

func Put[T interface{}](ctx context.Context, urlStr string, header http.Header, body []byte, opts ...Option) (responseInfo T,
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
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, urlStr, bytes.NewBuffer(body))
	if err != nil {
		err = errors.Wrap(err, "Put new req")
		return
	}
	req.Header = header

	response, err = cli.Do(req)
	if err != nil {
		err = errors.Wrap(err, "Put send http")
		return
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		err = errors.Errorf("Put http expect, statusCode: %d", response.StatusCode)
		return
	}

	responseData, err := io.ReadAll(response.Body)
	if err != nil {
		err = errors.Wrap(err, "Put read response")
		return
	}

	err = json.Unmarshal(responseData, &responseInfo)
	if err != nil {
		err = errors.Wrap(err, "Put unmarshal response")
		return
	}
	return
}
