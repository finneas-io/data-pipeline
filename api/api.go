package api

import (
	"encoding/json"
	"errors"
)

type Api struct {
	client client
}

type client interface {
	Request(urlStr string) ([]byte, error)
}

func New(c client) *Api {
	return &Api{client: c}
}

func (api *Api) GetFilings(cik string) ([]*Filing, error) {

	// fetch filings
	data, err := api.client.Request("https://data.sec.gov/submissions/CIK" + cik + ".json")
	if err != nil {
		return nil, err
	}
	res := &filingsResponse{}
	if err := json.Unmarshal(data, res); err != nil {
		return nil, err
	}

	// list to collect all filing lists (recent and old filings)
	fils, err := transformFilings(&res.Filings.FilingsData)
	if err != nil {
		return nil, err
	}
	filingsLists := [][]*Filing{fils}

	// fetch non recent filings
	for _, v := range res.Filings.OldFiles {
		data, err := api.client.Request("https://data.sec.gov/submissions/" + v.Name)
		if err != nil {
			return nil, err
		}
		filData := &filingsData{}
		if err := json.Unmarshal(data, filData); err != nil {
			return nil, err
		}
		fils, err := transformFilings(filData)
		if err != nil {
			return nil, err
		}
		filingsLists = append(filingsLists, fils)
	}

	// filter filing lists for duplicates and return
	return filterDuplicates(filingsLists), nil
}

func (api *Api) GetFile(cik, id, key string) (*File, error) {

	// get list of files in the filings
	data, err := api.client.Request(
		"https://www.sec.gov/Archives/edgar/data/" + cik + "/" + id + "/index.json",
	)
	if err != nil {
		return nil, err
	}
	res := &filesResponse{}
	if err := json.Unmarshal(data, res); err != nil {
		return nil, err
	}

	// find main file and fetch it's content
	file, err := getFile(transformFiles(res), key)
	if err != nil {
		return nil, err
	}
	file.Data, err = api.client.Request(
		"https://www.sec.gov/Archives/edgar/data/" + cik + "/" + id + "/" + key,
	)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func getFile(files []*File, key string) (*File, error) {
	for _, file := range files {
		if file.Key == key {
			return file, nil
		}
	}
	return nil, errors.New("File not in provided list")
}
