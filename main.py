#!/usr/bin/env python
# -*- coding: hebrew -*-

import os
import json
import time
import requests
import schedule
import wikipedia as wiki
import urllib.parse
import urllib.request
import urllib.error

from datetime import date, timedelta
from typing import Final as Const, Any

# region Config
# Output
OUT_FORMAT: Const = "json"
OUT_DIR: Const = "out_old/"
START_DAYS_AGO: Const = 4  # Update info for every day since x days ago
RESULTS_PER_KEYWORD: Const = 3
# START_DATE: Const = "20231001"

# Misc
UPDATE_TIME: Const = "12:00"
KEYWORDS_FILENAME: Const = "keywords.txt"


# endregion


class Article:
    def __init__(self, id: int, title: str, key: str, url: str = ""):
        self.id = id
        self.title = title
        self.key = key
        self.url = url
        self.language: str  # todo
        self.matched_title: Any  # todo

    def __repr__(self):
        return self.title


def to_timestamp(date: date):
    return f"{date.year}{date.month}{date.day}"


def get_keywords(filename: str) -> set:
    with open(filename, 'r', encoding="utf-8") as file:
        keywords = file.read().splitlines()
    keywords = set(keywords)

    return keywords


def get_pageid_dict_for(titles: iter, titles_per_request: int = 40) -> dict[str: str]:
    """

    :param titles: Titles of articles to translate in dict.
    :param titles_per_request: Because there's a limit of about 50 titles per request.
    :return:
    """

    request_num = titles_per_request
    while request_num <= len(titles):
        # Json from URL
        url = (
                'https://en.wikipedia.org/w/api.php'
                '?action=query'
                '&prop=info'
                '&inprop=subjectid'
                '&titles=' + '|'.join(tuple(titles)[request_num - titles_per_request:request_num]) +
                '&format=json')

        request_num += titles_per_request
        json_response = requests.get(url).json()

        # Create dictionary (according to JSON)
        out_dict = {
            page_info['title']: page_id
            for page_id, page_info in json_response['query']['pages'].items()}

        return out_dict


def pageid_to_wikipedia_page(id_: str) -> wiki.WikipediaPage:
    try:
        return wiki.page(pageid=id_, auto_suggest=False)
    except (wiki.PageError, wiki.DisambiguationError) as err:
        print(f"Article was not found, skipping")


def find_articles_by_keywords(keywords_: iter) -> tuple[wiki.WikipediaPage]:
    # Search keywords in Wikipedia
    article_titles = set()
    x = 0

    base_title_url = "https://api.wikimedia.org/core/v1/wikipedia/en/search/title"
    base_content_url = "https://api.wikimedia.org/core/v1/wikipedia/en/search/page"
    articles = []

    for word_num, word in enumerate(keywords_):
        article_titles.add(tuple(wiki.search(word, results=RESULTS_PER_KEYWORD)))

        # title_search_results = (
        #     requests.get(
        #         base_title_url,
        #         params={"q": word, "limit": RESULTS_PER_KEYWORD})
        #         .json()
        # )
        # title_search_results = tuple(title_search_results["pages"]) \
        #     if "pages" in title_search_results \
        #     else tuple()
        #
        # content_search_results = (
        #     requests.get(
        #         base_content_url,
        #         params={"q": word, "limit": RESULTS_PER_KEYWORD})
        #         .json()
        # )
        # content_search_results = tuple(content_search_results["pages"]) \
        #     if "pages" in content_search_results \
        #     else tuple()
        #
        # if len(title_search_results) == 0 and len(content_search_results) == 0:
        #     print("skipping")
        #     continue
        #
        # print(f"Title: {tuple(x['title'] for x in title_search_results)}")
        # print(f"Content: {tuple(x['title'] for x in content_search_results)}")
        # new_pages = tuple(title_search_results) + tuple(content_search_results)
        # print(f"{new_pages=}")
        # new_pages = tuple(
        #     page for page in new_pages
        #     if page["key"] not in article_titles)
        # print(f"{new_pages=}")
        #
        # if len(new_pages) == 0:
        #     print("skipping")
        #     continue
        #
        # new_pages = tuple(
        #     Article(page["id"], page["title"], page["key"])
        #     for page in new_pages)
        # print(f"{new_pages=}")
        #
        # article_titles.add(page.key for page in new_pages)
        # articles += [page.key for page in new_pages]


        # print(f"new articles: {tuple(x['title'] for x in temp_new_articles)}")

        # for page in article_request:
        #     if page["key"] not in article_titles:
        #         articles.add(page["key"])
        #         article_titles.add(page["key"])

        # tuple(
        #     (articles.add(page["key"]), article_titles.add(page["key"]))
        #     for page in title_search_results
        #     if page["key"] not in article_titles
        # )

        if x / len(keywords_) >= 0.01:
            x = 0
            print(f"\r- Searching keywords in Wikipedia ({word_num}/{len(keywords_)}; "
                  f"{int(word_num / len(keywords_) * 100)}%)",
                  end="")
        x += 1

    article_titles = set(article for tup in article_titles for article in tup)
    print(f"\nArticle titles: {article_titles}\n\n")

    # Convert Titles to 'WikipediaPage's (Title -> Page ID -> WikipediaPage):
    print("- Converting to 'wikipedia.WikipediaPage' class")

    # Titles -> Page IDs
    title_dict = get_pageid_dict_for(article_titles)
    page_ids = tuple(title_dict.values())
    print(f"{page_ids=}\n")

    # Page IDs -> WikipediaPages
    out_articles = tuple(pageid_to_wikipedia_page(id_) for id_ in page_ids)
    print(f"{out_articles=}\n")

    return out_articles


def get_page_data(article_name_: str, end_date: str, start_date: str, lang: str = "en") -> ():
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
    except urllib.error.URLError as error:
        print(f"Update failed, No internet connection ({error})")
        return

    page = page.decode("UTF-8")
    stats_per_date = tuple(json.loads(page)["items"])
    return stats_per_date


def update_daily_data():
    print(f"The time is {time.asctime()}, Updating data...")
    start_date = to_timestamp(date.today() - timedelta(START_DAYS_AGO))
    yesterday: str = to_timestamp(date.today() - timedelta(1))
    invalid_name_index = 0

    print("Building a collection of relevant articles:")
    articles = find_articles_by_keywords(get_keywords(KEYWORDS_FILENAME))
    articles = (article for article in articles if article)
    print("Collection is ready. Extracting stats...")

    for article in articles:
        # Output folder
        # NOTE: It's up here to save time, in case article is being skipped
        if not os.path.exists(OUT_DIR + article.title):
            try:
                os.makedirs(OUT_DIR + article.title)

            except OSError as err:
                try:
                    os.makedirs(OUT_DIR + "INVALID_NAME#" + str(invalid_name_index))
                    invalid_name_index += 1
                except Exception as err:
                    print(f"Folder for article {article.title} could not be created. Skipping. ({err})")

            except Exception as err:
                print(f"Folder for article {article.title} could not be created. Skipping. ({err})")

        # Extract data from article
        article_lang = article.url.split("//")[1][0:2]
        article_page_data = get_page_data(article.title, yesterday, lang=article_lang, start_date=start_date)

        # Data exists
        if article_page_data is None:
            continue

        # Edit output statistics (json data)
        out_stats = article_page_data
        for i, date_stats in enumerate(out_stats, start=1):
            # Add categories
            date_stats["language"] = article_lang
            date_stats["url"] = article.url
            date_stats["date"] = date_stats["timestamp"]

            # Remove categories
            del date_stats["granularity"]
            del date_stats["access"]
            del date_stats["agent"]
            del date_stats["project"]
            del date_stats["timestamp"]

            print(f"\rRearranging statistics. Date: {i}/{len(out_stats)}, Article: '{article.title}'", end="")

        # Output as JSON
        for daily_stats in article_page_data:
            with open(f"{OUT_DIR + article.title}/{daily_stats['date']}.{OUT_FORMAT}", 'w') as out_file:
                json.dump(daily_stats, out_file)

        print(f"\nDone: '{article.title}' (data for {len(article_page_data)} dates)")

    print("Data update is DONE!")


if __name__ == "__main__":
    schedule.every().day.at(UPDATE_TIME).do(update_daily_data)
    update_daily_data()

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        print("Shutdown")
