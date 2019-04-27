#!/usr/bin/python3

import sys
if sys.version_info[0] < 3:
    pVer = 2
else:
    pVer = 3

import requests
try:
    from nested_lookup import nested_lookup
except ImportError:
    print("Nested_lookup import not found.")
    if pVer == 3:
        print("please execute the command: pip3 install nested_lookup")
    else:
        print("please execute the command: pip install nested_lookup")

    sys.exit(1)

import time
import json
import sys
import os.path
import socket
import ast
from io import open

if pVer == 3:
    inpFunc = input
else:
    inpFunc = raw_input

size = 1000
pagesPerFile = 1000
scrollTimer = "1440"

# Take input for IP address, port, index, and values to save
ipAdr = inpFunc("IP address: ")
try:
    socket.inet_aton(ipAdr)
except socket.error:
    print("Invalid IP.")
    sys.exit()

print("To list all indices go to <IP>:<port>/_cat/indices?v")
index = inpFunc("Index: ")

port = inpFunc("Port (Default is 9200): ")
if port == "":
    port = 9200

save = []
print("Values within the index you want to save. When you're done submit an empty string")
print("If you need a value that's within an object, that shares a name with an outer object such as")
print("{'name': 'Jesse', 'job': {'name': 'Electrician', 'position': 'worker', 'company': 'securelectric'}")
print("And you only want the name of the job, not the actual persons name, you can do the following as input")
print("['job', 'name']")

inp = inpFunc("Value: ")
while inp != "":
    if '[' in inp and ']' in inp:
        try:
            save.append(ast.literal_eval(inp))
        except SyntaxError:
            print("Invalid input.")
    else:
        save.append(inp)
    inp = inpFunc("Value: ")

def parse_single(data):
    # Set our save string to nothing
    save_data = ""

    # For each value you want to save, if it's a list, do two nested lookips
    for i in save:
        # If you passed a list, loop through it to get the innermost value
        if isinstance(i, (list,)):
            print('list')
            results = data
            for n in range(len(i)):
                results = nested_lookup(i[n], results)
        else:
            # Else just lookup the value
            results = nested_lookup(i, data)

        # If we have a single result that isn't empty add it to the string
        if len(results) == 1:
            if results[0] != "":
                save_data = save_data + str(results[0]) + ', '
        else:
            # If we have list of results, if each of them isn't empty all them to the string
            for n in results:
                if n != "":
                    save_data = save_data + n + ', '

    # Clean up the string
    save_data = save_data.replace(", \n", "")
    save_data = save_data.replace("\n", "")

    return save_data

# Create session to keep track of cookies/headers
s = requests.session()

# If there is a scrollID.txt file parse it to figure out where in the search we are
if os.path.isfile("./" + ipAdr + "-scrollID.txt"):
    scrollFile = open(ipAdr + "-scrollID.txt", "r+")
    scrollContents = scrollFile.read().split("\n")
    scrollFile.close()
    scrollID = scrollContents[0]

else:
    # If there is no scrollID.txt file
    # Send initial request to get a scrollID to start pulling all the data, and not just the 5000 results that you can get from a search

    # scrollContents contains the values we need to "scoll" through all the pages of results
    scrollContents = []
    r = s.post("http://" + ipAdr + ":" + port + "/" + index + "/_search?scroll=" + scrollTimer + "m&size=" + str(size), headers={'Content-Type': 'application/json'})
    if not r.ok:
        print("Response not okay, exiting")
        print(r.text)
        sys.exit(1)

    rJson = json.loads(r.text)

    if 'error' in rJson:
        print("The server returned an error")
        print(rJson)
        sys.exit(1)

    scrollID = rJson["_scroll_id"]
    totalRequests = str(int((rJson["hits"]["total"])/size))

    scrollContents.append(scrollID)
    scrollContents.append(totalRequests)
    scrollContents.append("1")


# Strip all whitespace from the scrollContents
print(str(scrollContents))
for i in range(len(scrollContents)-1):
    scrollContents[i] = scrollContents[i].strip()

# Create scroll files. We save 1000 "pages" of results per file
fileName = ipAdr + "-" + index + "-" + str(int(int(scrollContents[2]) / pagesPerFile)) + ".txt"
f = open(fileName, "a", encoding='utf-16')

# Loop through every request, get the results, parse them, and save them to their respective files
while True:
    print("Getting page ", scrollContents[2], "/", scrollContents[1])
    scrollContents[2] = str(int(scrollContents[2]) + 1)

    if int(scrollContents[1]) % pagesPerFile == 0:
        # If we've hit the 1000 pages per file for our scolling, save the file and open the next
        f.close()

        fileName = ipAdr + "-" + index + "-" + str(int(int(scrollContents[2]) % pagesPerFile)) + ".txt"
        f = open(fileName, "a", encoding='utf-16')

    # Get next "page" storia_moments
    r = s.post("http://" + ipAdr + ":" + str(port) + "/_search/scroll?scroll=" + scrollTimer + "m&scroll_id=" + scrollID, headers={'Content-Type': 'application/json'})
    if not r.ok:
        # This shouldn't happen often unless we're being ratelimited
        print("Response not okay, sleeping 10 seconds")
        print(r.text)
        print("http://" + ipAdr + ":" + str(port) + "/_search/scroll?scroll=" + scrollTimer + "m&scroll_id=" + scrollID)
        time.sleep(10)
        continue

    # Update scrollID
    rJson = json.loads(r.text)
    scrollID = rJson["_scroll_id"]
    if scrollID != scrollContents[0]:
        scrollContents[0] = scrollID

    # Update scrollID.txt file if anything's changed
    scrollFile = open(ipAdr + "-scrollID.txt", "w")
    for i in scrollContents:
        scrollFile.write("%s\n" %i)
    scrollFile.close()

    # If we're out of results, we've scraped everything
    if len(rJson["hits"]["hits"]) == 0:
        print("Got all data")
        f.close()
        sys.exit(0)

    # Run each result through the parsing function
    for hit in rJson["hits"]["hits"]:
        cwd = hit["_source"]
        csv = parse_single(cwd)
        # and write them to the current file
        f.write(csv + "\n")

    time.sleep(1)
