import urllib.request
import re
import datetime
import logging
import os
import sys
import csv

HOLIDAYS_ICS_URL = "https://raw.github.com/PanderMusubi/dutch-holidays/master/DutchHolidays.ics"


def fetch_holidays_from_ics(url):
    """Fetch events from an iCalendar (.ics) URL and return list of (date_iso, summary).

    Returns empty list on any error.
    """
    try:
        resp = urllib.request.urlopen(url, timeout=20)
        txt = resp.read().decode("utf-8")
    except Exception:
        logging.exception("Failed fetching holidays from %s", url)
        return []

    events = []
    in_event = False
    dt = None
    summary = None
    for raw_line in txt.splitlines():
        line = raw_line.strip()
        if line.startswith("BEGIN:VEVENT"):
            in_event = True
            dt = None
            summary = None
            continue
        if line.startswith("END:VEVENT"):
            if in_event and dt:
                m = re.search(r"(\d{8})", dt)  # extract YYYYMMDD
                if m:
                    try:
                        d = datetime.datetime.strptime(m.group(1), "%Y%m%d").date().isoformat()
                    except Exception:
                        d = None
                    if d:
                        events.append((d, summary or ""))
            in_event = False
            continue
        if not in_event:
            continue
        if line.startswith("DTSTART"):
            parts = line.split(":", 1)
            if len(parts) > 1:
                dt = parts[1]
            else:
                dt = line
        elif line.startswith("SUMMARY"):
            parts = line.split(":", 1)
            if len(parts) > 1:
                summary = parts[1]

    return events


def expand_singleton_holidays_to_year(events, max_year=2025):
    """For any holiday summary that appears only once in `events`,
    create entries for the same month/day for each year up to `max_year`.

    - `events` is a list of (date_iso, summary).
    - Returns an extended list (original events + generated ones),
      avoiding duplicates and skipping invalid dates (e.g., Feb 29 on non-leap years).
    """
    from collections import defaultdict

    by_summary = defaultdict(set)
    for d_iso, summary in events:
        by_summary[summary].add(d_iso)

    out = list(events)
    existing = set(events)

    for summary, dates in by_summary.items():
        if not summary:
            continue
        if len(dates) != 1:
            continue
        # single date -> expand
        only_date = next(iter(dates))
        try:
            dt = datetime.datetime.strptime(only_date, "%Y-%m-%d").date()
        except Exception:
            continue
        month = dt.month
        day = dt.day
        start_year = dt.year
        for y in range(start_year + 1, max_year + 1):
            try:
                new_dt = datetime.date(y, month, day)
            except ValueError:
                # invalid date (e.g., Feb 29 on non-leap year)
                continue
            new_iso = new_dt.isoformat()
            tup = (new_iso, summary)
            if tup not in existing:
                out.append(tup)
                existing.add(tup)

    return out


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    out_path = None
    # optional first arg: output CSV path
    if len(sys.argv) > 1:
        out_path = sys.argv[1]

    logging.info("Fetching holidays from %s", HOLIDAYS_ICS_URL)
    events = fetch_holidays_from_ics(HOLIDAYS_ICS_URL)
    logging.info("Fetched %d holiday events", len(events))

    # expand single-occurrence holidays through 2025
    orig_count = len(events)
    events = expand_singleton_holidays_to_year(events, max_year=2025)
    logging.info("Expanded singleton holidays: %d -> %d", orig_count, len(events))

    # Ensure New Year's Eve is present for each year in the range (avoid duplicates)
    existing = set(events)
    if events:
        min_year = min(int(d.split("-")[0]) for d, _ in events)
    else:
        min_year = 2019
    added = 0
    for y in range(min_year, 2026):
        tup = (f"{y}-12-31", "New Year's Eve")
        if tup not in existing:
            events.append(tup)
            existing.add(tup)
            added += 1
    if added:
        logging.info("Added %d New Year's Eve entries", added)

    def save_holidays_to_csv(path, holidays=None):
        """Save holidays (list of (date_iso, summary)) to CSV at path.

        Creates parent directories as needed. Overwrites existing file.
        """
        holidays = holidays or []
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["event_date", "holiday"])
            for d, s in holidays:
                writer.writerow([d, s])

    # default path if not provided
    if not out_path:
        out_path = os.path.join(os.getcwd(), "holidays.csv")

    try:
        save_holidays_to_csv(out_path, events)
        logging.info("Wrote holidays CSV to %s", out_path)
    except Exception:
        logging.exception("Failed writing holidays CSV to %s", out_path)
