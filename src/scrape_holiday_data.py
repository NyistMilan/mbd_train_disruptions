import urllib.request
import re
import datetime
import logging
import os
import sys
import csv

HOLIDAYS_ICS_URL = "https://raw.github.com/PanderMusubi/dutch-holidays/master/DutchHolidays.ics"
CSV_OUTPUT_PATH = os.path.join(os.getcwd(), "holidays.csv")


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
    
    def _english_summary(text):
        if not text:
            return ""
        m = re.search(r"\(([^)]+)\)\s*$", text)
        return m.group(1).strip() if m else text.strip()
    
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
                        summ = _english_summary(summary)
                        cat = "Holiday"
                        if summ.lower() == "summer time":
                            cat = "Time change"
                        elif summ.lower() == "winter time":
                            cat = "Time change"

                        events.append((d, summ, cat))
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
    summary_category = {}
    for item in events:
        # support both 2- and 3-tuple input (backwards compatibility)
        if len(item) == 3:
            d_iso, summary, cat = item
        else:
            d_iso, summary = item
            cat = "Holiday"
        by_summary[summary].add(d_iso)
        # remember a category for this summary (prefer first seen)
        if summary and summary not in summary_category:
            summary_category[summary] = cat

    out = list(events)
    existing = set()
    for item in out:
        existing.add(tuple(item))

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
        cat = summary_category.get(summary, "Holiday")
        for y in range(start_year + 1, max_year + 1):
            try:
                new_dt = datetime.date(y, month, day)
            except ValueError:
                # invalid date (e.g., Feb 29 on non-leap year)
                continue
            new_iso = new_dt.isoformat()
            tup = (new_iso, summary, cat)
            if tup not in existing:
                out.append(tup)
                existing.add(tup)

    return out


def generate_historical_school_vacations(start_year=2015, end_year=2025):
    """Generation of Dutch school vacations per-day for years.

    This uses heuristic rules (typical week ranges) to create per-day entries
    for Summer, Christmas, Spring/Voorjaars (incl. Krokus), Autumn and May
    vacations. These are approximations of when the vacations took place.
    """
    out = []
    seen = set()

    def add_range(start_dt, end_dt, name):
        d = start_dt
        while d <= end_dt:
            tup = (d.isoformat(), name, "Vacation")
            if tup not in seen:
                out.append(tup)
                seen.add(tup)
            d += datetime.timedelta(days=1)

    for y in range(start_year, end_year + 1):
        # Summer: approx July 15 -> Aug 25 (6 weeks)
        try:
            s_start = datetime.date(y, 7, 15)
            s_end = datetime.date(y, 8, 25)
            add_range(s_start, s_end, "Summer vacation")
        except Exception:
            pass

        # Christmas: Dec 24 (year) -> Jan 4 (next year)
        try:
            c_start = datetime.date(y, 12, 24)
            c_end = datetime.date(y + 1, 1, 4)
            add_range(c_start, c_end, "Christmas vacation")
        except Exception:
            pass

        # Spring/Voorjaars (krokus): one-week around Feb 15 (Monday-Sunday)
        try:
            ref = datetime.date(y, 2, 15)
            mon = ref - datetime.timedelta(days=ref.weekday())
            add_range(mon, mon + datetime.timedelta(days=6), "Spring vacation")
        except Exception:
            pass

        # Autumn: one-week around Oct 15
        try:
            ref = datetime.date(y, 10, 15)
            mon = ref - datetime.timedelta(days=ref.weekday())
            add_range(mon, mon + datetime.timedelta(days=6), "Autumn vacation")
        except Exception:
            pass

        # May vacation (around King's Day Apr 27). Use week containing Apr 27.
        try:
            ref = datetime.date(y, 4, 27)
            mon = ref - datetime.timedelta(days=ref.weekday())
            add_range(mon, mon + datetime.timedelta(days=6), "May vacation")
        except Exception:
            pass

    return out

events = fetch_holidays_from_ics(HOLIDAYS_ICS_URL)
events = expand_singleton_holidays_to_year(events, max_year=2025)
backfill = generate_historical_school_vacations(2015, 2025)
existing = set(events)
added = 0
for tup in backfill:
    if tup not in existing:
        events.append(tup)
        existing.add(tup)
        added += 1

events = events or []
parent = os.path.dirname(CSV_OUTPUT_PATH)
if parent:
    os.makedirs(parent, exist_ok=True)

with open(CSV_OUTPUT_PATH, "w", newline="", encoding="utf-8") as fh:
    writer = csv.writer(fh)
    writer.writerow(["event_date", "name", "category"])
    for d, s, c in events:
        writer.writerow([d, s, c])