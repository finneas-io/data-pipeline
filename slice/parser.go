package slice

import (
	"bytes"
	"errors"
	"io"
	"strconv"
	"strings"
	"unicode"

	"golang.org/x/net/html"
)

func stripExtension(key string) (string, error) {
	if len(key) < 4 {
		return "", errors.New("Key not long enough")
	}
	if key[len(key)-4:] != ".htm" {
		return "", errors.New("Key does not end with '.htm'")
	}
	return key[:len(key)-4], nil
}

func toHTML(data []byte) (*html.Node, error) {
	node, err := html.Parse(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	return node, nil
}

func stripStr(str string) string {
	str = strings.ToLower(str)
	result := ""
	for _, r := range str {
		if unicode.IsLetter(r) {
			result += string(r)
		}
	}
	return result
}

func toStr(node *html.Node) (string, error) {
	var buf bytes.Buffer
	w := io.Writer(&buf)
	err := html.Render(w, node)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

func getTableData(table *html.Node) [][]string {
	data := [][]string{}
	rows := getRows(table)
	for _, r := range rows {
		tmp := []string{}
		cols := getColumns(r)
		for _, c := range cols {
			str := getText(c)
			colspan := 1
			for _, a := range c.Attr {
				if a.Key == "colspan" {
					val, err := strconv.Atoi(a.Val)
					if err != nil {
						break
					}
					colspan = val
				}
			}
			for i := 0; i < colspan; i++ {
				tmp = append(tmp, str)
			}
		}
		data = append(data, tmp)
	}
	return data
}

func stripStrings(data [][]string) [][]string {
	result := [][]string{}
	for i, row := range data {
		result = append(result, []string{})
		for _, col := range row {
			tmp := myReplace(col)
			result[i] = append(result[i], stripStr(tmp))
		}
	}
	return result
}

func myReplace(str string) string {
	result := ""
	for _, char := range str {
		if char == 160 {
			result += " "
			continue
		}
		result += string(char)
	}
	return result
}

func dropRows(content [][]string) [][]string {
	result := [][]string{}
	for i := range content {
		for j := range content[i] {
			if len(content[i][j]) > 0 {
				result = append(result, content[i])
				break
			}
		}
	}
	return result
}

func dropCols(content [][]string) [][]string {
	if len(content) < 1 {
		return content
	}
	content = transpose(content)
	prev := content[0]
	result := [][]string{}
	result = append(result, content[0])
	for i := range content {
		for j := range content[i] {
			if content[i][j] != prev[j] {
				result = append(result, content[i])
				break
			}
		}
		prev = content[i]
	}
	return transpose(result)
}

func newMatrix(d2, d1 int) [][]string {
	a := make([]string, d2*d1)
	m := make([][]string, d2)
	lo, hi := 0, d1
	for i := range m {
		m[i] = a[lo:hi:hi]
		lo, hi = hi, hi+d1
	}
	return m
}

func transpose(a [][]string) [][]string {
	b := newMatrix(len(a[0]), len(a))
	for i := 0; i < len(b); i++ {
		c := b[i]
		for j := 0; j < len(c); j++ {
			c[j] = a[j][i]
		}
	}
	return b
}

func parseKey(key string) (string, error) {
	id := ""
	for i := 0; i < len(key); i++ {
		if string(key[i]) == "." {
			break
		}
		id += string(key[i])
	}
	if len(id) < 1 {
		return "", errors.New("ID in key is corrupted")
	}
	return id, nil
}
