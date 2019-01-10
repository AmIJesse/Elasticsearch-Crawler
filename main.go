package main

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/levigross/grequests"
)

func main() {
	// https://www.elastic.co/guide/en/elasticsearch/reference/6.3/search-request-scroll.html
	s := grequests.NewSession(nil)
	ipAddr := "172.104.246.109"
	port := 9200

	resp, err := s.Get(fmt.Sprintf("http://%s:%d/_all/_search?size=100000000", ipAddr, port), nil)
	if err != nil {
		panic(err)
	}

	rs := strings.Split(resp.String(), "r equal to: [")
	if len(rs) < 2 {
		panic("Max sort not found")
	}
	rs = strings.Split(rs[1], "]")
	if len(rs) < 2 {
		panic("Max sort not found")
	}
	maxSize := rs[0]
	fmt.Printf("Max Size: %s\n", maxSize)

	f, err := os.OpenFile("ErrorLog.txt", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	ro := &grequests.RequestOptions{
		Params: map[string]string{
			"size": "1000",
		},
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
	}

	resp, err = s.Post(fmt.Sprintf("http://%s:%d/_all/_search?scroll=1m", ipAddr, port), ro)
	if err != nil {
		panic(err)
	}

	r := regexp.MustCompile(`scroll_id":".*?"`)
	matches := r.FindAllString(resp.String(), -1)
	fmt.Printf("%d\n", len(matches))
	match := matches[0]
	scrollID := strings.Split(match, `"`)[2]

	n := 1
	for {

		fmt.Printf("Grabbing request number %d\n", n)
		n++

		ro := &grequests.RequestOptions{
			Params: map[string]string{
				"scroll":    "10m",
				"scroll_id": scrollID,
			},
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
		}

		resp, err := s.Post(fmt.Sprintf("http://%s:%d/_search/scroll", ipAddr, port), ro)
		if err != nil {
			panic(err)
		}

		f.WriteString(resp.String() + "\n")

		if strings.Contains(resp.String(), `"hits":[]}`) {
			fmt.Printf("Scraped all results.\n")
			os.Exit(0)
		}

		time.Sleep(1 * time.Second)
	}
}
