package main

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gocolly/colly"
	_ "github.com/gocolly/colly"
)

func dbConn() (db *sql.DB) {
	dbDriver := "mysql"
	dbUser := "manager"
	dbPass := "manager"
	dbName := "crawler"
	db, err := sql.Open(dbDriver, dbUser+":"+dbPass+"@/"+dbName)
	if err != nil {
		panic(err.Error())
	}
	return db
}

/*CountData */
type CountData struct {
	COUNT int
}

//parseUrl function
func parseURL(url string) []string {

	fmt.Println("url from parseUrl")
	fmt.Println(url)
	prefix := "https://medium.com"

	var strArray []string

	if !strings.Contains(url, prefix) && !strings.HasPrefix(url, "/") {
		return nil
	}
	var Url string

	if strings.HasPrefix(url, "/") {
		Url = prefix + url
	} else {
		Url = url
	}
	strArray = strings.Split(Url, "?")
	fmt.Printf("\nstrArray: %s", strArray)
	return strArray
}

func addRecords(conn *sql.DB, url string, params string) *sql.Rows {

	fmt.Println("inside the addRecords")

	query := "INSERT INTO crawled(url,params) VALUES (?,?)"

	result, err := conn.Query(query, url, params)

	if err != nil {
		panic(err.Error())
	}

	return result
}

func getURLInfo(conn *sql.DB) *sql.Rows {

	query := "SELECT url, COUNT(url) COUNT, params FROM crawled GROUP BY url, params"

	result, err := conn.Query(query)

	if err != nil {
		panic(err.Error())
	}

	return result
}

func isURLExists(conn *sql.DB, url string, params string) *sql.Rows {

	query := "SELECT count(id) COUNT FROM crawled WHERE url=? AND params=?"

	result, err := conn.Query(query, url, params)

	if err != nil {
		panic(err.Error())
	}

	return result
}

func crawl(url string, c *colly.Collector, conn *sql.DB, visited map[string]int, msg chan string) {

	fmt.Printf("######## visiting %s\n", url)

	val, exists := visited[url]

	fmt.Println(val)

	urlParts := parseURL(url)

	if urlParts == nil {
		return
	}

	params := ""

	if len(urlParts) == 2 {
		params = urlParts[1]
	}

	fmt.Printf("\n url: %s \n params: %d", urlParts[0], len(urlParts))

	addRecords(conn, urlParts[0], params)

	c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		newURL := e.Attr("href")
		fmt.Printf("newUrl: %s: ", newURL)
		crawl(newURL, c, conn, visited, msg)
	})

	c.OnRequest(func(r *colly.Request) {
		fmt.Println("Visiting", r.URL)
	})

	c.OnError(func(r *colly.Response, err error) {
		fmt.Println("Request URL:", r.Request.URL, "failed with response:", r, "\nError:", err)
	})

	if !exists {
		visited[url] = 1
		c.Visit(urlParts[0])
		messages := <-msg
		fmt.Println(messages)
	} else {
		visited[url] = visited[url] + 1
	}

}

func main() {
	// conn := dbConn()
	c := colly.NewCollector(colly.MaxDepth(10))
	url := "https://medium.com"
	messages := make(chan string, 5)
	var visited map[string]int
	visited = make(map[string]int)
	// msg := <-messages
	// fmt.Println(msg)
	// sleep
	defer conn.Close()

	go crawl(url, c, conn, visited, messages)

	<-messages

	c := colly.NewCollector()

	// Find and visit all links
	// c.OnHTML("a[href]", func(e *colly.HTMLElement) {
	// 	e.Request.Visit(e.Attr("href"))
	// })

	// c.OnRequest(func(r *colly.Request) {
	// 	fmt.Println("Visiting", r.URL)
	// })

	// c.Visit(url)

}
