package api

import "time"

type Filing struct {
	Cik          string
	Id           string
	FilingDate   time.Time
	Form         string
	OriginalFile string
}

type File struct {
	Key          string
	Data         []byte
	LastModified *time.Time
}

type filingsResponse struct {
	Name    string  `json:"name"`
	CIK     string  `json:"cik"`
	Filings filings `json:"filings"`
}

type filings struct {
	FilingsData filingsData `json:"recent"`
	OldFiles    []oldFile   `json:"files"`
}

type filingsData struct {
	AccessNumber []string `json:"accessionNumber"`
	FilingDate   []string `json:"filingDate"`
	AcceptDate   []string `json:"acceptanceDateTime"`
	ReportDate   []string `json:"reportDate"`
	Form         []string `json:"form"`
	PrimDoc      []string `json:"primaryDocument"`
}

type oldFile struct {
	Name string `json:"name"`
}

type filesResponse struct {
	Dir directory `json:"directory"`
}

type directory struct {
	Items []item `json:"item"`
}

type item struct {
	Name         string `json:"name"`
	LastModified string `json:"last-modified"`
}
