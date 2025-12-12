import os
import re
import sys
import subprocess
from dataclasses import dataclass
from collections import defaultdict
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

TRAIN_ARCHIVE_URL = "https://www.rijdendetreinen.nl/en/open-data/train-archive"
DISRUPTIONS_URL = "https://www.rijdendetreinen.nl/en/open-data/disruptions"
STATIONS_URL = "https://www.rijdendetreinen.nl/en/open-data/stations"
STATION_DISTANCES_URL = "https://www.rijdendetreinen.nl/en/open-data/station-distances"

RAW_DIR = "data/raw"
HDFS_DIR = "" #final_project/data/raw

TRAIN_YEAR_MIN = 2019

TARGET_STATIONS_FILE = "stations-2023-09.csv"
TARGET_TARIFF_DIST_FILE = "tariff-distances-2022-01.csv"

SERVICES_YEARLY_RE = re.compile(r"services-(\d{4})\.csv\.gz$")
SERVICES_MONTHLY_RE = re.compile(r"services-(\d{4})-(\d{2})\.csv\.gz$")
DISRUPTIONS_RE = re.compile(r"disruptions-(\d{4})\.csv$")

@dataclass(frozen=True)
class DownloadItem:
    dataset: str
    url: str
    filename: str

def scrape_links(page_url):
    resp = requests.get(page_url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    out = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not urlparse(href).netloc:
            href = urljoin(page_url, href)
        out.append(href)

    return out

def download(item: DownloadItem, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, item.filename)

    if os.path.exists(path) and os.path.getsize(path) > 0:
        return path

    resp = requests.get(item.url)
    resp.raise_for_status()
    with open(path, "wb") as f:
        f.write(resp.content)

def select_train_archive_items():
    links = scrape_links(TRAIN_ARCHIVE_URL)
    by_year = defaultdict(lambda: {"yearly": [], "monthly": []})

    for url in links:
        filename = os.path.basename(urlparse(url).path)

        m_yearly = SERVICES_YEARLY_RE.match(filename)
        if m_yearly:
            year = int(m_yearly.group(1))
            if year >= TRAIN_YEAR_MIN:
                by_year[year]["yearly"].append((filename, url))
            continue

        m_monthly = SERVICES_MONTHLY_RE.match(filename)
        if m_monthly:
            year = int(m_monthly.group(1))
            if year >= TRAIN_YEAR_MIN:
                by_year[year]["monthly"].append((filename, url))

    files = []
    for year in sorted(by_year):
        group = by_year[year]
        chosen = group["yearly"] if group["yearly"] else group["monthly"]
        for filename, url in sorted(chosen):
            files.append(DownloadItem("services", url, filename))

    if not files:
        raise RuntimeError(f"No train-archive files matched year>={TRAIN_YEAR_MIN}")
    return files


def select_disruptions_items():
    links = scrape_links(DISRUPTIONS_URL)
    out = []
    for url in links:
        filename = os.path.basename(urlparse(url).path)
        m = DISRUPTIONS_RE.match(filename)
        if not m:
            continue
        year = int(m.group(1))
        if year >= TRAIN_YEAR_MIN:
            out.append(DownloadItem("disruptions", url, filename))
    if not out:
        raise RuntimeError("No disruptions files matched year>=2019")
    return sorted(out, key=lambda x: x.filename)


def select_stations_item():
    links = scrape_links(STATIONS_URL)
    for url in links:
        filename = os.path.basename(urlparse(url).path)
        if filename == TARGET_STATIONS_FILE:
            return DownloadItem("stations", url, filename)
    raise RuntimeError(f"Could not find {TARGET_STATIONS_FILE} on stations page")


def select_tariff_distances_item():
    links = scrape_links(STATION_DISTANCES_URL)
    for url in links:
        filename = os.path.basename(urlparse(url).path)
        if filename == TARGET_TARIFF_DIST_FILE:
            return DownloadItem("tariff_distances", url, filename)
    raise RuntimeError(f"Could not find {TARGET_TARIFF_DIST_FILE} on station-distances page")

def hdfs_put(local_dir, hdfs_target):
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_target], check=True)
    subprocess.run(
        ["hdfs", "dfs", "-put", "-f", os.path.join(local_dir, "*"), hdfs_target],
        check=True,
    )

def scrape_train_data():
    os.makedirs(RAW_DIR, exist_ok=True)

    items = []
    items.extend(select_train_archive_items())
    items.extend(select_disruptions_items())
    items.append(select_stations_item())
    items.append(select_tariff_distances_item())

    for item in items:
        out_dir = os.path.join(RAW_DIR, item.dataset)
        download(item, out_dir)

    if HDFS_DIR:
        hdfs_put(RAW_DIR, HDFS_DIR)

    print("OK")
    print(f"Raw local: {os.path.abspath(RAW_DIR)}")
    if HDFS_DIR:
        print(f"HDFS: {HDFS_DIR}")

if __name__ == "__main__":
    try:
        scrape_train_data()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
