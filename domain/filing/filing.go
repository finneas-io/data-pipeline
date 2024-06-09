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
	Id      uuid.UUID
	HeadIdx int
	Index   int
	Faktor  string
	Data    matrix
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
		mat, head := convert(n)
		tables = append(
			tables,
			&Table{
				Index:   i,
				Faktor:  searchStr(n, 8, 300, []string{"thousand", "million"}),
				HeadIdx: head,
				Data:    mat,
			},
		)
	}

	f.Tables = tables
	return nil
}

func Connect(from *Filing, to *Filing) ([]*Edge, error) {

	edges := []*Edge{}
	for _, mainTbl := range from.Tables {
		for _, t := range to.Tables {
			weight := getWeight(mainTbl, t)
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

func Join(first *Table, second *Table) (*Table, error) {
	if first == nil || second == nil {
		return nil, errors.New("Tables must be not nil")
	}

	table := &Table{}

	return table, nil
}

func (f *Filing) Json() ([]byte, error) {
	return json.Marshal(f)
}

func (t *Table) Compress() error {
	mat := t.Data.stripCells()
	mat, headIdx := mat.dropEmptyRows(t.HeadIdx)
	mat, err := mat.dropDuplCols()
	if err != nil {
		return err
	}
	mat, err = mat.mergeCols(headIdx)
	if err != nil {
		return err
	}
	t.Data = mat
	t.HeadIdx = headIdx
	return nil
}

func (m matrix) Json() ([]byte, error) {
	return json.Marshal(m)
}

/*
Helper functions
*/

func getWeight(first, second *Table) int {

	rowLookup := make(map[string]bool)
	for _, row := range first.Data {
		if len(row) < 1 {
			continue
		}
		rowLookup[row[0]] = true
	}

	weight := 0
	for _, row := range second.Data {
		if len(row) < 1 {
			continue
		}
		if rowLookup[row[0]] {
			weight++
			rowLookup[row[0]] = false
		}
	}

	if len(first.Data) < 1 || len(first.Data) >= first.HeadIdx || len(second.Data) >= second.HeadIdx {
		return weight
	}

	columnLookup := []string{}
	for i := 0; i < first.HeadIdx; i++ {
		for j, column := range first.Data[i] {
			if j == 0 {
				continue
			}
			columnLookup = append(columnLookup, column)
		}
	}

	for i := 0; i < second.HeadIdx; i++ {
		for j, column := range second.Data[i] {
			if j == 0 {
				continue
			}
			for k := 0; k < len(columnLookup); k++ {
				if columnLookup[k] == column {
					columnLookup = append(columnLookup[:k], columnLookup[k+1:]...)
					weight++
					break
				}
			}
		}
		if len(columnLookup) < 1 {
			break
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
				// check ASCII table to understand this and good luck
				if char < 33 || char > 126 {
					if len(c)-1 == j {
						break
					}
					if len(newCell) < 1 {
						continue
					}
					if newCell[len(newCell)-1:] == " " {
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

func (m matrix) dropEmptyRows(head int) (matrix, int) {
	newMtrx := matrix{}
	newHead := head
	for i := range m {
		empty := true
		for j := range m[i] {
			if len(m[i][j]) > 0 {
				newMtrx = append(newMtrx, m[i])
				empty = false
				break
			}
		}
		if empty && i <= head {
			newHead--
		}
	}
	return newMtrx, newHead
}

func (m matrix) mergeCols(head int) (matrix, error) {

	if head < 1 {
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
		for j := 0; j < head; j++ {
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

		matrix = append(matrix, []string{})
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
