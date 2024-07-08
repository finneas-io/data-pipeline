package filing

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	"golang.org/x/net/html"
)

type Company struct {
	Cik     string    `json:"cik"`
	Name    string    `json:"-"`
	Tickers []*Ticker `json:"-"`
	Filings []*Filing `json:"-"`
}

type Ticker struct {
	Value    string
	Exchange string
}

type Filing struct {
	Id         string    `json:"id"`
	Form       string    `json:"form"`
	FilingDate time.Time `json:"filing_date"`
	MainFile   *File     `json:"main_file"`
	Tables     []*Table  `json:"-"`
}

type File struct {
	Key          string    `json:"key"`
	LastModified time.Time `json:"last_modified"`
	Data         []byte    `json:"data"`
}

type Table struct {
	Id        uuid.UUID
	HeadIndex int
	Index     int
	Faktor    string
	Data      matrix
}

type matrix [][][]string

type Edge struct {
	From   *Table
	To     *Table
	Weight int
}

func (f *Filing) LoadTables() error {

	if f.MainFile == nil {
		return errors.New("Main file is nil")
	}

	document, err := toHtml(f.MainFile.Data)
	if err != nil {
		return err
	}

	// get nodes of HTML node type 'table'
	nodes := getNodes(document, "table")
	tables := []*Table{}
	for i, n := range nodes {
		mat, head := convert(n)
		tables = append(
			tables,
			&Table{
				Index:     i,
				Faktor:    searchStr(n, 8, 300, []string{"thousand", "million"}),
				HeadIndex: head,
				Data:      mat,
			},
		)
	}

	f.Tables = tables
	return nil
}

func (m matrix) Json() ([]byte, error) {
	return json.Marshal(m)
}

func toHtml(data []byte) (*html.Node, error) {
	return html.Parse(bytes.NewReader(data))
}

func getNodes(node *html.Node, nType string) []*html.Node {

	nodes := []*html.Node{}

	var crawler func(node *html.Node)
	crawler = func(node *html.Node) {
		if node.Type == html.ElementNode && node.Data == nType {
			nodes = append(nodes, node)
			return
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			crawler(child)
		}
	}
	crawler(node)

	return nodes
}

func convert(table *html.Node) (matrix, int) {
	matrix := matrix{}
	headIdx := 0
	counting := true

	// 'tr' usually the element type of rows in an HTML Table and
	// 'td' usually the element type of columns in an HTML Table row
	rows := getNodes(table, "tr")
	for i, r := range rows {

		for _, a := range r.Attr {
			// check if the row is colored to find out where the row header ends
			if a.Key == "bgcolor" || a.Key == "background-color" || a.Key == "background" {
				counting = false
				break
			}
			if a.Key == "style" {
				if strings.Contains(a.Val, "bgcolor:") ||
					strings.Contains(a.Val, "background-color:") ||
					strings.Contains(a.Val, "background:") {
					counting = false
					break
				}
			}
		}

		matrix = append(matrix, [][]string{})
		cols := getNodes(r, "td")
		for _, c := range cols {

			// find out the width of the column in this row
			cSpan := 1
			for _, a := range c.Attr {
				if a.Key == "style" {
					if strings.Contains(a.Val, "bgcolor:") ||
						strings.Contains(a.Val, "background-color:") ||
						strings.Contains(a.Val, "background:") {
						counting = false
					}
				}
				if a.Key == "colspan" {
					v, err := strconv.Atoi(a.Val)
					if err != nil {
						continue
					}
					cSpan = v
				}
			}

			for j := 0; j < cSpan; j++ {
				matrix[i] = append(matrix[i], getText(c))
			}
		}

		// increment if we are still counting and haven't found a colored row
		if counting {
			headIdx += 1
		}
	}

	return matrix, headIdx
}

func searchStr(node *html.Node, maxDist, maxLen int, queries []string) string {

	var crawler func(node *html.Node, except *html.Node) string
	crawler = func(node *html.Node, except *html.Node) string {
		if node == except {
			return ""
		}

		if node.Type == html.TextNode {
			return node.Data
		}

		result := ""
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			result += crawler(child, except)
		}
		return result
	}

	current := node
	for maxDist > 0 {
		maxDist--

		// determine next node
		if current.PrevSibling == nil {
			// check if we already have reached the root
			if current.Parent == nil {
				return ""
			}
			// no more siblings so we go one level up
			current = current.Parent
		} else {
			current = current.PrevSibling
		}

		str := crawler(current, node)
		if len(str) > maxLen {
			break
		}

		letts := getLetters(str)
		// check if the content contains one of the filter values
		for _, q := range queries {
			if strings.Contains(letts, q) {
				return str
			}
		}
	}

	return ""
}

func getLetters(str string) string {
	str = strings.ToLower(str)
	result := ""
	for _, r := range str {
		if unicode.IsLetter(r) {
			result += string(r)
		}
	}
	return result
}

// we return a string array here to make not assumption how to seperate
// the data of the text nodes
func getText(node *html.Node) []string {
	result := []string{}
	var crawler func(node *html.Node)
	crawler = func(node *html.Node) {
		if node.Type == html.TextNode {
			result = append(result, node.Data)
			return
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			crawler(child)
		}
	}
	crawler(node)
	return result
}
