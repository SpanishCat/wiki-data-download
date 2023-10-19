#!/usr/bin/env python
# -*- coding: hebrew -*-

import os
import json
import time
import schedule
import urllib.parse
import urllib.request

from datetime import date, timedelta
from typing import Final as Const

# region Config
# Output
OUT_FORMAT: Const = "json"
OUT_DIR: Const = "out/"
START_DATE: Const = "20231001"

# Misc
UPDATE_TIME: Const = "12:00"
articles = (
    ("Hamas", "en"),
    ("Palestinian_Islamic_Jihad", "en"),
    ("חמאס", "he"),
    ("הג'יהאד_האסלאמי_הפלסטיני", "he")
)
# endregion


def to_timestamp(date: date):
    return f"{date.year}{date.month}{date.day}"


def get_page_data(article_name_: str, end_date: str, start_date: str = START_DATE, lang: str = "en") -> ():
    """
    Extract information and statistics about a certain page of the Wikipedia, from a certain date(s).

    :param article_name_: Title of Wiki entry page.
    :param end_date: Date after which to stop providing data; Format: "yyyymmdd", e.g "20230225" (25.2.2023).
    :param start_date: Date from which to start providing data; Format: "yyyymmdd", e.g "20230225" (25.2.2023).
    :param lang: Language of wiki page (e.g "en", "he", "ru").
    :return: Tuple of data dictionaries for each day checked.
    """

    url = u"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/" \
          u"{}.wikipedia.org/all-access/all-agents/{}/daily/{}/{}" \
        .format(lang.lower(), urllib.parse.quote(article_name_), start_date, end_date)

    try:
        page = urllib.request.urlopen(url).read()
    except Exception as exception:
        print(f"Error reading {url}: {exception}")
        return

    page = page.decode("UTF-8")
    items = tuple(json.loads(page)["items"])
    return items


def update_daily_data():
    print(f"The time is {time.asctime()}, Updating data...")
    yesterday: str = to_timestamp(date.today() - timedelta(1))

    for article in articles:
        article_name, article_lang = article
        article_page_data = get_page_data(article_name, yesterday, lang=article_lang)

        if not os.path.exists(OUT_DIR + article_name):
            os.makedirs(OUT_DIR + article_name)

        for daily_stats in article_page_data:
            with open(f"{OUT_DIR + article_name}/{daily_stats['timestamp']}.{OUT_FORMAT}", 'w') as out_file:
                json.dump(daily_stats, out_file)

        print(f"Written JSON for '{article_name} (data for {len(article_page_data)} dates)'")


if __name__ == "__main__":
    schedule.every().day.at(UPDATE_TIME).do(update_daily_data)
    update_daily_data()

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        print("Shutdown")