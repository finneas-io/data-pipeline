package api

import (
	"errors"
	"strings"
	"time"
)

func transformFilings(data *filingsData) ([]*Filing, error) {
	var filings []*Filing
	for i, v := range data.Form {
		if v != "10-K" && v != "10-Q" {
			continue
		}
		ext, err := getExtension(data.PrimDoc[i])
		if err != nil {
			continue
		}
		if ext != ".htm" {
			continue
		}
		filDate, err := time.Parse("2006-01-02", data.FilingDate[i])
		if err != nil {
			return nil, err
		}
		fil := &Filing{
			Id:           strings.Replace(data.AccessNumber[i], "-", "", -1),
			OriginalFile: data.PrimDoc[i],
			Form:         v,
			FilingDate:   filDate,
		}
		filings = append(filings, fil)
	}
	return filings, nil
}

func transformFiles(data *filesResponse) []*File {
	files := []*File{}
	for _, v := range data.Dir.Items {
		files = append(files, &File{
			Key:          v.Name,
			LastModified: parseNullTime("2006-01-02 15:04:05", v.LastModified),
		})
	}
	return files
}

func filterDuplicates(lists [][]*Filing) []*Filing {
	result := []*Filing{}
	lookUp := make(map[string]*Filing)
	for _, v := range lists {
		for _, w := range v {
			if lookUp[w.Id] == nil {
				lookUp[w.Id] = w
				result = append(result, w)
			}
		}
	}
	return result
}

func parseNullTime(layout string, value string) *time.Time {
	t, err := time.Parse(layout, value)
	if err != nil {
		return nil
	}
	return &t
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
