import requests
import sys
import time
import json

ipAdr = "172.104.246.109"   # Ip address to crawl
index = "_all"              # Index to scan
port = "9200"               # Port of elastic service

# To list all indices go to <IP>:<port>/_cat/indices?v

s = requests.session()
r = s.post("http://" + ipAdr + ":" + port + "/" + index + "/_search?scroll=1m&size=1000", headers={'Content-Type': 'application/json'})
if not r.ok:
    print("Response not okay, exiting")
    sys.exit(1)

rJson = json.loads(r.text)

scrollID = rJson["_scroll_id"]
f = open("CrawlResults.txt", "a")
totalRequests = (rJson["hits"]["total"])/1000

i = 1
while True:
    print("Getting page ", i, "/", int(totalRequests) + 1)
    i = i + 1

    r = s.post("http://" + ipAdr + ":" + str(port) + "/_search/scroll?scroll=1m&scroll_id=" + scrollID, headers={'Content-Type': 'application/json'})
    if not r.ok:
        print("Response not okay, sleeping 10 seconds")
        time.sleep(10)
        continue

    rJson = json.loads(r.text)
    scrollID = rJson["_scroll_id"]

    if len(rJson["hits"]["hits"]) == 0:
        print("Got all data")
        sys.exit(0)

    json.dump(rJson["hits"]["hits"], f)
    f.write("\n")
    time.sleep(1)
