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
	Tables     []*Table  `json:"tables"`
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
	Factor    string
	Data      matrix
	CompData  compMatrix
}

type matrix [][][]string
type compMatrix [][]string

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
				Factor:    searchStr(n, 8, 300, []string{"thousand", "million"}),
				HeadIndex: head,
				Data:      mat,
			},
		)
	}

	f.Tables = tables
	return nil
}

func (t *Table) Compress() error {

	mat := t.Data.sumCells()
	mat = mat.stripCells()
	mat, headIdx := mat.dropEmptyRows(t.HeadIndex)
	mat, err := mat.dropDuplCols()
	if err != nil {
		return err
	}
	mat, err = mat.mergeCols(headIdx)
	if err != nil {
		return err
	}
	t.Factor = stripFactor(t.Factor)
	t.CompData = mat
	t.HeadIndex = headIdx
	return nil
}

func (m matrix) Json() ([]byte, error) {
	return json.Marshal(m)
}

func (m compMatrix) Json() ([]byte, error) {
	return json.Marshal(m)
}

func stripFactor(factor string) string {
	if strings.Contains(factor, "thousand") {
		return "thousand"
	} else if strings.Contains(factor, "million") {
		return "million"
	}
	return ""
}

func (m matrix) sumCells() compMatrix {

	c := compMatrix{}
	for i, row := range m {
		c = append(c, []string{})
		for _, col := range row {
			concatCell := ""
			for _, cell := range col {
				concatCell += cell
				concatCell += " "
			}
			c[i] = append(c[i], concatCell)
		}
	}

	return c
}

func (m compMatrix) stripCells() compMatrix {
	newMtrx := compMatrix{}
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

			// maybe not?
			if len(newCell) > 0 && newCell[len(newCell)-1:] == " " {
				newCell = newCell[:len(newCell)-1]
			}

			newMtrx[i] = append(newMtrx[i], newCell)
		}
	}
	return newMtrx
}

func (m compMatrix) dropEmptyRows(head int) (compMatrix, int) {
	newMtrx := compMatrix{}
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

func (m compMatrix) mergeCols(head int) (compMatrix, error) {

	if head < 1 {
		return m, nil
	}

	// initialize new matrix
	newMtrx := compMatrix{}
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

func (m compMatrix) dropDuplCols() (compMatrix, error) {
	tMtrx, err := m.transpose()
	if err != nil {
		return nil, err
	}

	if len(tMtrx) < 1 {
		return m, nil
	}
	prev := tMtrx[0]
	newMtrx := compMatrix{prev}
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

func (m compMatrix) transpose() (compMatrix, error) {
	if len(m) < 1 {
		return m, nil
	}

	newMtrx := compMatrix{}
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
