package httpclient

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/finneas-io/data-pipeline/domain/filing"
)

type httpClient struct {
	client *http.Client
}

func New() *httpClient {
	return &httpClient{client: &http.Client{}}
}

func (c *httpClient) GetCompany(cik string) (*filing.Company, error) {

	data, err := c.get(fmt.Sprintf("https://data.sec.gov/submissions/CIK%s.json", cik))
	if err != nil {
		return nil, err
	}

	res := &struct {
		Name    string   `json:"name"`
		Tickers []string `json:"tickers"`
		Exchs   []string `json:"exchanges"`
	}{}
	err = json.Unmarshal(data, res)
	if err != nil {
		return nil, err
	}

	cmp := &filing.Company{Cik: cik, Name: res.Name}
	for i := range res.Tickers {
		cmp.Tickers = append(cmp.Tickers, &filing.Ticker{Value: res.Tickers[i], Exchange: res.Exchs[i]})
	}

	return cmp, nil
}

func (c *httpClient) GetFilings(cik string) ([]*filing.Filing, error) {

	data, err := c.get(fmt.Sprintf("https://data.sec.gov/submissions/CIK%s.json", cik))
	if err != nil {
		return nil, err
	}

	// resolve received data into struct
	res := &filingResponse{}
	err = json.Unmarshal(data, res)
	if err != nil {
		return nil, err
	}

	// prepare result list and look up to avoid duplicates
	lookup := make(map[string]*filing.Filing)
	filings := res.Filings.Recent.transform()
	for _, v := range filings {
		lookup[v.Id] = v
	}

	// get filings from non recent pages and check for duplicates
	for _, old := range res.Filings.OldPages {
		data, err := c.get(fmt.Sprintf("https://data.sec.gov/submissions/%s", old.Name))
		if err != nil {
			return nil, err
		}
		filData := &filingData{}
		if err := json.Unmarshal(data, filData); err != nil {
			return nil, err
		}
		oldFils := filData.transform()
		for _, f := range oldFils {
			if lookup[f.Id] == nil {
				lookup[f.Id] = f
				filings = append(filings, f)
			}
		}
	}

	return filings, nil
}

type filingResponse struct {
	Name    string `json:"name"`
	Cik     string `json:"cik"`
	Filings struct {
		Recent   *filingData `json:"recent"`
		OldPages []struct {
			Name string `json:"name"`
		} `json:"files"`
	} `json:"filings"`
}

type filingData struct {
	Ids         []string `json:"accessionNumber"`
	FilingDates []string `json:"filingDate"`
	Forms       []string `json:"form"`
	PrimDocs    []string `json:"primaryDocument"`
}

func (d *filingData) transform() []*filing.Filing {

	filings := []*filing.Filing{}

	for i, v := range d.Forms {

		// we are only looking for quarterly and annual financial reports
		if v != "10-K" && v != "10-Q" {
			continue
		}
		ext, err := getExtension(d.PrimDocs[i])
		if err != nil {
			continue
		}
		if ext != ".htm" {
			continue
		}
		// TODO no error is expected but implement observability just to be sure
		fd, err := time.Parse("2006-01-02", d.FilingDates[i])
		if err != nil {
			fd = time.Time{}
		}

		f := &filing.Filing{
			Id:         strings.Replace(d.Ids[i], "-", "", -1),
			MainFile:   &filing.File{Key: d.PrimDocs[i]},
			Form:       v,
			FilingDate: fd,
		}
		filings = append(filings, f)
	}

	return filings
}

func getExtension(key string) (string, error) {
	if !strings.Contains(key, ".") {
		return "", errors.New("File extension could not be found")
	}
	result := ""
	for i := len(key) - 1; i >= 0; i-- {
		result = string(key[i]) + result
		if string(key[i]) == "." {
			break
		}
	}
	return result, nil
}

func (w *httpClient) GetFile(cik, id, key string) (*filing.File, error) {

	data, err := w.get(
		fmt.Sprintf("https://www.sec.gov/Archives/edgar/data/%s/%s/index.json", cik, id),
	)
	if err != nil {
		return nil, err
	}

	res := &fileResponse{}
	err = json.Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	files := res.transform()

	// find the main file from the fetched file list
	for _, v := range files {
		if v.Key == key {
			v.Data, err = w.get(
				fmt.Sprintf("https://www.sec.gov/Archives/edgar/data/%s/%s/%s", cik, id, key),
			)
			if err != nil {
				return nil, err
			}
			return v, nil
		}
	}

	return nil, errors.New("Filing main file not found in file list")
}

type fileResponse struct {
	Dir struct {
		Items []struct {
			Name         string `json:"name"`
			LastModified string `json:"last-modified"`
		} `json:"item"`
	} `json:"directory"`
}

func (r *fileResponse) transform() []*filing.File {
	files := []*filing.File{}
	for _, v := range r.Dir.Items {
		// TODO no error is expected but implement observability just to be sure
		lm, _ := time.Parse("2006-01-02 15:04:05", v.LastModified)
		files = append(files, &filing.File{
			Key:          v.Name,
			LastModified: lm,
		})
	}
	return files
}

func (w *httpClient) get(url string) ([]byte, error) {

	// build request
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("User-Agent", "example.com info@example.com")
	req.Header.Add("Accept", "*/*")
	req.Header.Add("Connection", "keep-alive")

	// send request and respect rate limit
	time.Sleep(200 * time.Millisecond)
	res, err := w.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode < 200 || res.StatusCode > 299 {
		return nil, errors.New(fmt.Sprintf("Got status code '%s'", res.Status))
	}

	return io.ReadAll(res.Body)
}
