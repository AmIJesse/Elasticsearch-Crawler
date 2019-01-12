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
# To list all indices go to <IP>:<port>/_cat/indices?v


def parse_single(data):
    save = [
        "tf_full_name",
        "city_name",
        "canonical_str",
        "state_name",
        "education_org",
        "tf_all_job_titles",
        "tf_all_education",
        "linkedin_profile_url",
        "tf_current_company",
        "tf_current_job_title",
        "company_name",
        "job_title"
        "tf_all_companies",
        "tf_all_education",
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


s = requests.session()
r = s.post("http://" + ipAdr + ":" + port + "/" + index + "/_search?scroll=1m&size=" + str(size), headers={'Content-Type': 'application/json'})
if not r.ok:
    print("Response not okay, exiting")
    sys.exit(1)

rJson = json.loads(r.text)

scrollID = rJson["_scroll_id"]
totalRequests = (rJson["hits"]["total"])/size

fileName = ipAdr + "-" + index + "-0.txt"

f = open(fileName, "a", encoding='utf-16')

i = 1
while True:
    print("Getting page ", i, "/", int(totalRequests) + 1)
    i = i + 1

    if i % pagesPerFile == 0:
        f.close()
        fileName = ipAdr + "-" + index + "-" + str(int(i/pagesPerFile)) + ".txt"
        f = open(fileName, "a", encoding='utf-16')

    r = s.post("http://" + ipAdr + ":" + str(port) + "/_search/scroll?scroll=1m&scroll_id=" + scrollID, headers={'Content-Type': 'application/json'})
    if not r.ok:
        print("Response not okay, sleeping 10 seconds")
        time.sleep(10)
        continue

    rJson = json.loads(r.text)
    scrollID = rJson["_scroll_id"]

    if len(rJson["hits"]["hits"]) == 0:
        print("Got all data")
        f.close()
        sys.exit(0)

    for hit in rJson["hits"]["hits"]:
        cwd = hit["_source"]
        csv = parse_single(cwd)

        f.write(csv + "\n")

    time.sleep(1)

