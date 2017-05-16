import os
import os.path
import requests
import json
from time import sleep

print("Fetching page #%s" % 1)

r = requests.get("http://declarations.com.ua/search?format=json").json()

for page in range(1, r["results"]["paginator"]["num_pages"] + 1):
    pth = os.path.join("jsons", str(page))
    os.makedirs(pth, exist_ok=True)

    sleep(0.5)
    print("Fetching page #%s" % page)

    subr = requests.get(
        "http://declarations.com.ua/search?format=json&page=%s" % page).json()
    for i, d in enumerate(subr["results"]["object_list"]):
        with open(os.path.join(pth, "%s.json" % i), "w") as fp:
            json.dump(d, fp, indent=4, sort_keys=True)
