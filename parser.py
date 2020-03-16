#!/usr/bin/env python3

import csv
import io
import json
import os
from pathlib import Path

import elasticsearch.helpers
import requests
from elasticsearch import Elasticsearch

INDEX = "covid"
DATA_DIR = "data"
KIBANA_EXPORT = Path("export.ndjson")

API = "https://api.github.com"
_OWNER = "CSSEGISandData"
_REPO = "COVID-19"
_DIR = "csse_covid_19_data/csse_covid_19_daily_reports"

# API content URL /repos/:owner/:repo/contents/:path
BASE_CONTENT_URL = '/'.join((API, "repos", _OWNER, _REPO, "contents"))
# Province/State,Country/Region,Last Update,Confirmed,Deaths,Recovered,Latitude,Longitude
CONTENT_URL = '/'.join((BASE_CONTENT_URL, "csse_covid_19_data/csse_covid_19_daily_reports"))
ARCHIVE_CONTENT_URL = '/'.join((BASE_CONTENT_URL, "archived_data/archived_daily_case_updates"))

if 'ESURL' not in os.environ:
    es_url = "http://localhost:9200"
else:
    es_url = os.environ['ESURL']

if 'KIBANAURL' not in os.environ:
    kibana_url = "http://localhost:5601/api"
else:
    kibana_url = os.environ["KIBANAURL"]


def prep_es(es):
    """Cleanup existing indices and apply mapping"""
    if es.indices.exists(INDEX) is True:
        es.indices.delete(index=INDEX, ignore=[400, 404])

    mapping = Path("mapping.json")
    with mapping.open() as m:
        es.indices.create(INDEX, body=json.load(m))


def prep_kibana(api_url):
    """Apply saved objects"""
    r = requests.post(
        '/'.join([api_url, "saved_objects/_import"]),
        json=KIBANA_EXPORT.read_text(),
        headers={"kbn-xsrf": "kibana"},
        params={"overwrite": True},
    )
    r.raise_for_status()


def data_getter(dir_url):
    """Iterator for corvid data, returns (<csv_data>, <year month day: str>"""
    # not saving files for now
    # Path(DATA_DIR).mkdir(exist_ok=True)
    # for x in Path(DATA_DIR).iterdir():
    #     x.unlink()

    r = requests.get(dir_url)
    r.raise_for_status()
    files = r.json()
    for file in files:
        if file.get('type') != "file":
            continue
        if file.get('name') in [".gitignore", "README.md"]:
            continue
        dl_url = file.get('download_url')
        if dl_url:
            # name example: 02-04-2020.csv
            month, day, year = file.get("name").split('.')[0].split('-')
            date = f"{year}{month}{day}"
            # TODO: consider dropping date yield and trusting "Last Update" as true across file
            yield io.StringIO(requests.get(dl_url).content.decode('utf-8-sig')), date


def normalizer(csv_date_iter):
    for data, date in csv_date_iter:
        # dialect = csv.Sniffer().sniff(data[0:1024])
        csvdata = csv.DictReader(data)

        for line in csvdata:
            base = dict()
            base["province"] = line["Province/State"]
            base["country"] = line["Country/Region"]

            for key in ["Confirmed", "Deaths", "Recovered"]:
                if line[key] == "":
                    line[key] = 0
                    continue
                line[key] = int(line[key])

            base["confirmed"] = line["Confirmed"]
            base["deaths"] = line["Deaths"]
            base["recovered"] = line["Recovered"]

            lat = line.get("Latitude")
            lon = line.get("Longitude")
            if lat and lon:
                base["location"] = {"lat": lat, "lon": lon}

            base['day'] = date

            bulk = {
                "_op_type": "index",
                "_index": INDEX,
            }

            bulk.update(base.copy())

            yield bulk


def main():
    es = Elasticsearch([es_url])
    prep_es(es)
    # prep_kibana(kibana_url)
    bulks = normalizer(data_getter(CONTENT_URL))

    for ok, item in elasticsearch.helpers.streaming_bulk(es, bulks, max_retries=2):
        if not ok:
            print(f"ERROR:\n{item}")


if __name__ == "__main__":
    main()
