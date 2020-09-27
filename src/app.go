package main

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
	"net/http"
	_ "github.com/go-sql-driver/mysql"
	"github.com/PuerkitoBio/goquery"
)

func dbConn( i *int) (db *sql.DB) {
	dbDriver := "mysql"
	dbUser := "manager"
	dbPass := "manager"
	dbName := "crawler"

	fmt.Printf("### current connection count %d\n",i)

	for *i>5{
		fmt.Printf("connection count greater than 5..sleeping for now\n")
		time.Sleep(time.Second * 3)
		*i--;
		}

	db, err := sql.Open(dbDriver, dbUser+":"+dbPass+"@/"+dbName)
	if err != nil {
		panic(err.Error())
	}
	*i++
	fmt.Printf("### current connection count %d\n",*i)
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

func addRecords(url string, params string, i *int) *sql.Rows {

	fmt.Printf("inside the addRecords %d \n ", i)

		conn := dbConn(i)

	fmt.Printf("### current connection count value froma addRecords %d\n",*i)
	query := "INSERT INTO crawled(url,params) VALUES (?,?)"

	result, err := conn.Query(query, url, params)

	if err != nil {
		panic(err.Error())
		conn.Close()
	}

	conn.Close()
	// *i--;
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

// func crawl(url string, conn *sql.DB, visited map[string]int, msg chan string) {
	func crawl(url string,visited map[string]int, connCount *int) {

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

	addRecords(urlParts[0], params, connCount)


	// If url already crawled increment count & exit. otherwise register & crawl
	if exists {
		visited[url] = visited[url] + 1
		// messages := <-msg
		// fmt.Println(messages)
		return
	} else {
		visited[url] = 1
	}



	// Get the HTML
	resp, err := http.Get(urlParts[0])
	if err != nil {
		fmt.Printf("error while visiting: %s\n",urlParts[0])
	}


	// Convert HTML into goquery document
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		fmt.Printf("unable to read documents %s\n",err.Error())
		return
	}

	// Save each .post-title as a list
	doc.Find("a[href]").Each(func(i int, s *goquery.Selection) {
		title := s.Text()
		link, _ := s.Attr("href")
		fmt.Printf("Post #%d: %s - %s\n", i, title, link)	
		fmt.Printf("### recursion passing connCount: %d\n", connCount)
		crawl(link,visited,connCount)
	})
	// return titles, nil
}

func main() {
	// conn := dbConn()
	// c := colly.NewCollector(colly.MaxDepth(10))
	url := "https://medium.com"
	// messages := make(chan string, 5)
	var visited map[string]int
	visited = make(map[string]int)
	// msg := <-messages
	// fmt.Println(msg)
	// sleep
	// defer conn.Close()

	// crawl(url,conn, visited, messages)
	i:=0
	b:=&i
	*b++;
	fmt.Printf("b %d \n",*b);
	crawl(url,visited,&i)
	time.Sleep(time.Second * 5)

	fmt.Printf("$$$$$$$$$$$$b %d \n",*b);

	// <-messages

}
