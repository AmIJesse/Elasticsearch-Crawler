import requests
import re
import sys
import time

ipAdr = "172.104.246.109"  # Ip address to crawl
port = "9200"                # Port of elastic service

s = requests.session()
r = s.post("http://" + ipAdr + ":" + port + "/_all/_search?scroll=1m&size=1000", headers={'Content-Type': 'application/json'})

matches = re.findall('scroll_id":".*?"', r.text)

if len(matches) < 1:
    print("Unable to find scroll_id")
    exit(1)
scrollID = matches[0].split('"')[2]

f = open("CrawlResults.txt", "ab")

i = 1
while True:
    print("Getting page ", i)
    i = i + 1

    r = s.post("http://" + ipAdr + ":" + str(port) + "/_search/scroll?scroll=1m&scroll_id=" + scrollID, headers={'Content-Type': 'application/json'})

    if '"hits":[]}' in r.text:
        print("Done crawling")
        sys.exit(0)

    f.write(r.text.encode("utf-8") + "\n".encode(("utf-8")))
    time.sleep(1)
