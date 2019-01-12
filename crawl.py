import requests
from nested_lookup import nested_lookup

import sys
import time
import json

ipAdr = "18.218.159.17"   # Ip address to crawl
index = "scraping"              # Index to scan
port = "9200"               # Port of elastic service
size = 1000
pagesPerFile = 1000
scrollTimer = "1440"
# To list all indices go to <IP>:<port>/_cat/indices?v


def parse_single(data):
    save = [
        "tf_full_name",
        "city_name",
        "state_name",
        "tf_all_job_titles",
        "tf_all_education",
        "linkedin_profile_url",
        "job_title"
        "tf_all_companies",
        ['certification', "title"],
        "certificate_authority"
    ]

    save_data = ""

    for i in save:
        if isinstance(i, (list,)):
            results = nested_lookup(i[0], data)
            results = nested_lookup(i[1], results)
            key = i[0] + "-" + i[1]
        else:
            results = nested_lookup(i, data)

        if len(results) == 1:
            if results[0] != "":
                save_data = save_data + str(results[0]) + ', '
        else:
            for n in results:
                if n != "":
                    save_data = save_data + n + ', '

    save_data = save_data.replace(", \n", "")
    save_data = save_data.replace("\n", "")

    return save_data


scrollFile = open(ipAdr + "-scrollID.txt", "w+")
print(len(scrollFile.readlines()))
scrollContents = scrollFile.readlines()

if len(scrollContents) == 0:

    s = requests.session()
    r = s.post("http://" + ipAdr + ":" + port + "/" + index + "/_search?scroll=" + scrollTimer + "m&size=" + str(size), headers={'Content-Type': 'application/json'})
    if not r.ok:
        print("Response not okay, exiting")
        print(r.text)
        sys.exit(1)

    rJson = json.loads(r.text)

    if 'error' in rJson:
        print(rJson)
        sys.exit(1)

    scrollID = rJson["_scroll_id"]
    totalRequests = str(int((rJson["hits"]["total"])/size))

    scrollContents.append(scrollID)
    scrollContents.append(totalRequests)
    scrollContents.append("1")
scrollFile.close()

fileName = ipAdr + "-" + index + "-" + str(int(int(scrollContents[2]) / pagesPerFile)) + ".txt"
f = open(fileName, "a", encoding='utf-16')

while True:
    print("Getting page ", scrollContents[2], "/", scrollContents[1])
    scrollContents[2] = str(int(scrollContents[2]) + 1)

    if int(scrollContents[1]) % pagesPerFile == 0:
        f.close()

        fileName = ipAdr + "-" + index + "-" + str(int(int(scrollContents[2]) % pagesPerFile)) + ".txt"
        f = open(fileName, "a", encoding='utf-16')

    r = s.post("http://" + ipAdr + ":" + str(port) + "/_search/scroll?scroll=1m&scroll_id=" + scrollID, headers={'Content-Type': 'application/json'})
    if not r.ok:
        print("Response not okay, sleeping 10 seconds")
        time.sleep(10)
        continue

    rJson = json.loads(r.text)
    scrollID = rJson["_scroll_id"]
    if scrollID != scrollContents[0]:
        scrollContents[0] = scrollID

    scrollFile = open(ipAdr + "-scrollID.txt", "w")
    for i in scrollContents:
        scrollFile.write("%s\n" %i)
    scrollFile.close()

    if len(rJson["hits"]["hits"]) == 0:
        print("Got all data")
        f.close()
        sys.exit(0)

    for hit in rJson["hits"]["hits"]:
        cwd = hit["_source"]
        csv = parse_single(cwd)

        f.write(csv + "\n")

    time.sleep(1)

