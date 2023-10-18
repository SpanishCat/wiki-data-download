#!/usr/bin/env python
# -*- coding: hebrew -*-

import os
import json
import urllib.parse
import urllib.request

# Config
pages = (
    ("Hamas", "en"),
    ("Palestinian_Islamic_Jihad", "en"),
    ("חמאס", "he"),
    ("הג'יהאד_האסלאמי_הפלסטיני", "he")
)
OUT_FORMAT = "json"
OUT_DIR = "out/"


def get_wiki_stats(page_title: str, end_date: str, start_date: str = "20231001", lang: str="en"):
    """

    :param page_title:
    :param end_date: Timestamp format: "<year><month><day>", e.g "20230225" (25.2.2023)
    :param start_date: Timestamp format: "<year><month><day>", e.g "20230225" (25.2.2023)
    :param lang: Language of wiki page (e.g "en", "he", "ru")
    :return:
    """

    url = u"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/" \
          u"{}.wikipedia.org/all-access/all-agents/{}/daily/{}/{}"\
        .format(lang.lower(), urllib.parse.quote(page_title), start_date, end_date)

    try:
        page = urllib.request.urlopen(url).read()
    except Exception as exception:
        print(f"Error reading {url}: {exception}")
        return

    page = page.decode("UTF-8")
    items = json.loads(page)["items"]
    return items


if __name__ == "__main__":
    for page in pages:
        page_name = page[0]
        page_lang = page[1]

        stats = get_wiki_stats(page_name, "20231017", lang=page_lang)

        if not os.path.exists(OUT_DIR + page_name):
            os.makedirs(OUT_DIR + page_name)

        for item in stats:
            with open(f"{OUT_DIR + page_name}/{item['timestamp']}.{OUT_FORMAT}", 'w') as out_file:
                json.dump(item, out_file)

        print(f"Written JSON for '{page_name}'")
