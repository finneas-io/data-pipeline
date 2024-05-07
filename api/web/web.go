package web

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

type webClient struct{}

func NewClient() *webClient {
	return &webClient{}
}

func (c *webClient) Request(urlStr string) ([]byte, error) {

	// build request
	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("User-Agent", "example.com info@example.com")
	req.Header.Add("Accept", "*/*")
	req.Header.Add("Connection", "keep-alive")

	// send request and respect rate limit
	time.Sleep(200 * time.Millisecond)
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode < 200 || res.StatusCode > 299 {
		return nil, errors.New(fmt.Sprintf("Returned status code %s", res.Status))
	}

	return io.ReadAll(res.Body)
}
