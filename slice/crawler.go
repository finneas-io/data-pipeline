package slice

import (
	"strings"

	"golang.org/x/net/html"
)

type table struct {
	faktor string
	node   *html.Node
}

func getTables(document *html.Node) []*table {
	tables := []*table{}
	current := &table{}
	var crawler func(node *html.Node)
	crawler = func(node *html.Node) {
		if node.Type == html.ElementNode && node.Data == "table" {
			current.node = node
			tables = append(tables, current)
			current = &table{}
			return
		}
		if node.Type == html.TextNode && len(node.Data) <= 500 {
			content := stripStr(node.Data)
			if strings.Contains(content, "thousand") || strings.Contains(content, "million") {
				current.faktor = node.Data
			}
			return
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			crawler(child)
		}
	}
	crawler(document)
	return tables
}

func getRows(table *html.Node) []*html.Node {
	rows := []*html.Node{}
	var crawler func(node *html.Node)
	crawler = func(node *html.Node) {
		if node.Type == html.ElementNode && node.Data == "tr" {
			rows = append(rows, node)
			return
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			crawler(child)
		}
	}
	crawler(table)
	return rows
}

func getColumns(row *html.Node) []*html.Node {
	cols := []*html.Node{}
	var crawler func(node *html.Node)
	crawler = func(node *html.Node) {
		if node.Type == html.ElementNode && node.Data == "td" {
			cols = append(cols, node)
			return
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			crawler(child)
		}
	}
	crawler(row)
	return cols
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
