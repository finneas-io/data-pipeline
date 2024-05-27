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
	Cik     string
	Name    string
	Tickers []*Ticker
	Filings []*Filing
}

type Ticker struct {
	Value    string
	Exchange string
}

type Filing struct {
	Id         string
	Form       string
	FilingDate time.Time
	MainFile   *File
	Tables     []*Table
}

type File struct {
	Key          string
	LastModified time.Time
	Data         []byte
}

type Table struct {
	Id     uuid.UUID
	Index  int
	Faktor string
	Data   matrix
}

type matrix [][]string

type Edge struct {
	From   *Table
	To     *Table
	Weight int
}

func (f *Filing) LoadTables() error {

	document, err := toHtml(f.MainFile.Data)
	if err != nil {
		return err
	}

	// get nodes of HTML node type 'table'
	nodes := getNodes(document, "table")
	tables := []*Table{}
	for i, n := range nodes {
		tables = append(
			tables,
			&Table{
				Index:  i,
				Faktor: searchStr(n, 8, 300, []string{"thousand", "million"}),
				Data:   toMatrix(n),
			},
		)
	}

	f.Tables = tables
	return nil
}

func (from *Filing) Connect(to *Filing) ([]*Edge, error) {

	edges := []*Edge{}
	for _, mainTbl := range from.Tables {
		for _, t := range to.Tables {
			weight := mainTbl.Data.getWeight(t.Data)
			if weight < 2 {
				continue
			}
			edges = append(
				edges,
				&Edge{From: mainTbl, To: t, Weight: weight},
			)
		}
	}

	return edges, nil
}

func (f *Filing) Json() ([]byte, error) {
	return json.Marshal(f)
}

func (m matrix) Compress() (matrix, error) {
	result := m.stripCells().dropEmptyRows()
	result, err := result.dropDuplCols()
	if err != nil {
		return nil, err
	}
	return result.mergeCols()
}

func (m matrix) Json() ([]byte, error) {
	return json.Marshal(m)
}

/*
Helper functions
*/

func (m matrix) getWeight(mat matrix) int {

	lookup := make(map[string]bool)
	for _, row := range m {
		if len(row) < 1 {
			continue
		}
		lookup[row[0]] = true
	}

	weight := 0
	for _, row := range mat {
		if len(row) < 1 {
			continue
		}
		if lookup[row[0]] {
			weight++
			lookup[row[0]] = false
		}
	}

	return weight
}

func (m matrix) stripCells() matrix {
	newMtrx := matrix{}
	for i, r := range m {
		newMtrx = append(newMtrx, []string{})
		for _, c := range r {
			newCell := ""
			for j, char := range c {
				// 160: no break space in ASCII
				//   9: horizontal tab in ASCII
				//  10: line feed '\n' in ASCII
				//  11: vertical tab in ASCII
				//  13: carriage return '\r' in ASCII
				//  32: space in ASCII
				if char == 160 || char == 9 || char == 10 || char == 11 || char == 13 || char == 32 {
					if len(c)-1 == j {
						break
					}
					if j < 1 {
						continue
					}
					newCell += " "
					continue
				}
				newCell += string(char)
			}
			newMtrx[i] = append(newMtrx[i], newCell)
		}
	}
	return newMtrx
}

func (m matrix) dropEmptyRows() matrix {
	newMtrx := matrix{}
	for i := range m {
		for j := range m[i] {
			if len(m[i][j]) > 0 {
				newMtrx = append(newMtrx, m[i])
				break
			}
		}
	}
	return newMtrx
}

func (m matrix) mergeCols() (matrix, error) {

	headIdx := 0
	for _, r := range m {
		if isHeader(r) {
			headIdx++
			continue
		}
		break
	}

	if headIdx < 1 {
		return m, nil
	}

	// initialize new matrix
	newMtrx := matrix{}
	for _, r := range m {
		if len(r) < 1 {
			return nil, errors.New("Matrix is ragged")
		}
		newMtrx = append(newMtrx, []string{r[0]})
	}

	for i := 1; i < len(m[0]); i++ {

		// check if column and previous column can be merged
		merge := true
		for j := 0; j < headIdx; j++ {
			if len(m[j]) <= i {
				return nil, errors.New("Matrix is ragged")
			}
			if m[j][i] != m[j][i-1] && len(m[j][i]) > 0 {
				merge = false
				break
			}
		}

		if merge {
			for j := 0; j < len(m); j++ {
				if len(m[j]) <= i {
					return nil, errors.New("Matrix is ragged")
				}
				if m[j][i] == m[j][i-1] {
					continue
				}
				newMtrx[j][len(newMtrx[j])-1] += m[j][i]
			}
		} else {
			// columns can't be merged and we just append the new column
			for j := 0; j < len(m); j++ {
				newMtrx[j] = append(newMtrx[j], m[j][i])
			}
		}
	}

	return newMtrx, nil
}

func isHeader(row []string) bool {
	if len(row) < 1 {
		return false
	}
	if len(row[0]) < 1 {
		return true
	}
	return false
}

func (m matrix) dropDuplCols() (matrix, error) {
	tMtrx, err := m.transpose()
	if err != nil {
		return nil, err
	}

	if len(tMtrx) < 1 {
		return m, nil
	}
	prev := tMtrx[0]
	newMtrx := matrix{prev}
	for i := range tMtrx {
		for j := range tMtrx[i] {
			if tMtrx[i][j] != prev[j] {
				newMtrx = append(newMtrx, tMtrx[i])
				break
			}
		}
		prev = tMtrx[i]
	}

	return newMtrx.transpose()
}

func (m matrix) transpose() (matrix, error) {
	if len(m) < 1 {
		return m, nil
	}

	newMtrx := matrix{}
	for i := 0; i < len(m[0]); i++ {
		newMtrx = append(newMtrx, []string{})
		for j := 0; j < len(m); j++ {
			if len(m[j]) != len(m[0]) {
				return nil, errors.New("Matrix is ragged")
			}
			newMtrx[i] = append(newMtrx[i], m[j][i])
		}
	}

	return newMtrx, nil
}

func toMatrix(Table *html.Node) matrix {
	matrix := matrix{}

	// 'tr' usually the element type of rows in an HTML Table and
	// 'td' usually the element type of columns in an HTML Table row
	rows := getNodes(Table, "tr")
	for i, r := range rows {
		matrix = append(matrix, []string{})
		cols := getNodes(r, "td")
		for _, c := range cols {

			// find out the width of the column in this row
			cSpan := 1
			for _, a := range c.Attr {
				if a.Key == "colspan" {
					v, err := strconv.Atoi(a.Val)
					if err != nil {
						break
					}
					cSpan = v
				}
			}

			for j := 0; j < cSpan; j++ {
				matrix[i] = append(matrix[i], getText(c))
			}
		}
	}

	return matrix
}

func getText(node *html.Node) string {
	str := ""
	var crawler func(node *html.Node)
	crawler = func(node *html.Node) {
		if node.Type == html.TextNode {
			str += node.Data
			return
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			crawler(child)
		}
	}
	crawler(node)
	return str
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

func searchStr(node *html.Node, maxDist, maxLen int, queries []string) string {

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

		if current.Type == html.TextNode && len(current.Data) <= maxLen {
			str := getLetters(current.Data)
			// check if the content contains one of the filter values
			for _, q := range queries {
				if strings.Contains(str, q) {
					return current.Data
				}
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

func toHtml(data []byte) (*html.Node, error) {
	return html.Parse(bytes.NewReader(data))
}
