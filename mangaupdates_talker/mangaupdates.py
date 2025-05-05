"""
MangaUpdates information source
"""
# Copyright comictagger team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

import argparse
import json
import logging
import pathlib
import re
import time
from typing import Any, Callable, TypedDict, cast
from urllib.parse import urljoin

import requests
import settngs
from comicapi import utils
from comicapi.genericmetadata import ComicSeries, GenericMetadata, MetadataOrigin
from comictalker import talker_utils
from comictalker.comiccacher import ComicCacher
from comictalker.comiccacher import Series as CCSeries
from comictalker.comictalker import ComicTalker, TalkerDataError, TalkerNetworkError
from pyrate_limiter import Duration, Limiter, RequestRate

logger = logging.getLogger(f"comictalker.{__name__}")


class MUGenre(TypedDict, total=False):
    genre: str
    color: str


class MUImageURL(TypedDict):
    original: str
    thumb: str


class MUImage(TypedDict):
    url: MUImageURL
    height: int
    width: int


class MULastUpdated(TypedDict):
    timestamp: int
    as_rfc3339: str
    as_string: str


class MURecord(TypedDict, total=False):
    series_id: int
    title: str
    url: str
    description: str
    image: MUImage
    type: str
    year: str
    bayesian_rating: float
    rating_votes: int
    genres: list[MUGenre]
    last_updated: MULastUpdated


class MUStatus(TypedDict):
    volume: int
    chapter: int


class MUUserList(TypedDict):
    list_type: str
    list_icon: str
    status: MUStatus


class MUMetadata(TypedDict):
    user_list: MUUserList


class MUResult(TypedDict):
    record: MURecord
    hit_title: str
    metadata: MUMetadata
    user_genre_highlights: list[MUGenre]


class MUResponse(TypedDict):
    total_hits: int
    page: int
    per_page: int
    results: list[MUResult]


class MUVolumeReply(TypedDict):
    reason: str
    status: str
    context: dict[Any, Any]
    total_hits: int
    page: int
    per_page: int
    results: list[MUResult] | MUResult


class MUAssTitle(TypedDict):
    title: str


class MUCategories(TypedDict):
    series_id: int
    category: str
    votes: int
    votes_plus: int
    votes_minus: int
    added_by: int


class MUAnime(TypedDict):
    start: str
    end: str


class MURelatedSeries(TypedDict):
    relation_id: int
    relation_type: str
    related_series_id: int
    related_series_name: str
    triggered_by_relation_id: int


class MUAuthor(TypedDict):
    name: str
    author_id: int
    type: str


class MUPublisher(TypedDict):
    publisher_name: str
    publisher_id: int
    type: str
    notes: str


class MUPublication(TypedDict):
    publication_name: str
    publisher_name: str
    publisher_id: int


class MURecommendations(TypedDict):
    series_name: str
    series_id: int
    weight: int


class MUPosition(TypedDict):
    week: int
    month: int
    three_months: int
    six_months: int
    year: int


class MULists(TypedDict):
    reading: int
    wish: int
    complete: int
    unfinished: int
    custom: int


class MURank(TypedDict):
    position: MUPosition
    old_position: MUPosition


class MUSeries(TypedDict, total=False):
    series_id: int
    title: str
    url: str
    associated: list[MUAssTitle]
    description: str
    image: MUImage
    type: str
    year: str
    bayesian_rating: float
    rating_votes: int
    genres: list[MUGenre]
    categories: list[MUCategories]
    latest_chapter: int
    forum_id: int
    status: str
    licensed: bool
    completed: bool
    anime: MUAnime
    related_series: list[MURelatedSeries]
    authors: list[MUAuthor]
    publishers: list[MUPublisher]
    publications: list[MUPublication]
    recommendations: list[MURecommendations]
    category_recommendations: list[MURecommendations]
    rank: MURank
    last_updated: MULastUpdated


# MangaUpdates states: You will use reasonable spacing between requests so as not to overwhelm the MangaUpdates servers
limiter = Limiter(RequestRate(5, Duration.SECOND))


class MangaUpdatesTalker(ComicTalker):
    name: str = "MangaUpdates"
    id: str = "mangaupdates"
    logo_url: str = "https://www.mangaupdates.com/images/mascot.gif"
    website: str = "https://mangaupdates.com/"
    attribution: str = f"Metadata provided by <a href='{website}'>{name}</a>"
    about: str = (
        f"<a href='{website}'>{name}</a> (also known as Baka-Updates Manga) is a site dedicated to bringing the manga "
        f"community (and by extension the manhwa, manhua, etc, communities) the latest scanlation and series "
        f"information. We were founded in July 2004 and are the sister site of Baka-Updates."
    )

    def __init__(self, version: str, cache_folder: pathlib.Path):
        super().__init__(version, cache_folder)
        # Settings
        self.default_api_url = self.api_url = "https://api.mangaupdates.com/v1/"
        self.use_series_start_as_volume: bool = False
        self.use_search_title: bool = False
        self.use_original_publisher: bool = False
        self.use_ongoing_issue_count: bool = False
        self.filter_nsfw: bool = False
        self.add_nsfw_rating: bool = False
        self.filter_dojin: bool = True

    def register_settings(self, parser: settngs.Manager) -> None:
        parser.add_setting(
            "--mu_use-series-start-as-volume",
            default=False,
            action=argparse.BooleanOptionalAction,
            display_name="Use series start as volume",
        )
        parser.add_setting(
            "--mu-use-search-title",
            default=False,
            action=argparse.BooleanOptionalAction,
            display_name="Use search title",
            help="Use search title result instead of the English title",
        )
        parser.add_setting(
            "--mu-use-ongoing",
            default=False,
            action=argparse.BooleanOptionalAction,
            display_name="Use the ongoing issue count",
            help='If a series is labelled as "ongoing", use the current issue count (otherwise empty)',
        )
        parser.add_setting(
            "--mu-use-original-publisher",
            default=False,
            action=argparse.BooleanOptionalAction,
            display_name="Use the original publisher",
            help="Use the original publisher instead of English language publisher",
        )
        parser.add_setting(
            "--mu-filter-nsfw",
            default=False,
            action=argparse.BooleanOptionalAction,
            display_name="Filter out NSFW results",
            help="Filter out NSFW from the search results (Genre: Adult and Hentai)",
        )
        parser.add_setting(
            "--mu-add-nsfw-rating",
            default=False,
            action=argparse.BooleanOptionalAction,
            display_name="Add 'Adult' maturity rating if 'Adult' or 'Hentai' genre",
            help="Add a maturity rating of 'Adult' if the genre is 'Adult' or 'Hentai'",
        )
        parser.add_setting(
            "--mu-filter-dojin",
            default=True,
            action=argparse.BooleanOptionalAction,
            display_name="Filter out dojin results",
            help="Filter out dojin from the search results (Genre: Doujinshi)",
        )
        parser.add_setting(
            f"--{self.id}-url",
            display_name="API URL",
            help=f"Use the given Manga Updates URL. (default: {self.default_api_url})",
        )
        parser.add_setting(f"--{self.id}-key", file=False, cmdline=False)

    def parse_settings(self, settings: dict[str, Any]) -> dict[str, Any]:
        settings = super().parse_settings(settings)

        self.use_series_start_as_volume = settings["mu_use_series_start_as_volume"]
        self.use_search_title = settings["mu_use_search_title"]
        self.use_ongoing_issue_count = settings["mu_use_ongoing"]
        self.use_original_publisher = settings["mu_use_original_publisher"]
        self.filter_nsfw = settings["mu_filter_nsfw"]
        self.add_nsfw_rating = settings["mu_add_nsfw_rating"]
        self.filter_dojin = settings["mu_filter_dojin"]

        return settings

    def check_status(self, settings: dict[str, Any]) -> tuple[str, bool]:
        url = talker_utils.fix_url(settings[f"{self.id}_url"])
        if not url:
            url = self.default_api_url
        try:
            mu_response = requests.get(
                url,
                headers={"user-agent": "comictagger/" + self.version},
            ).json()

            if mu_response["status"] == "success":
                return "The URL is valid", True
            else:
                return "The URL is INVALID!", False
        except Exception:
            return "Failed to connect to the URL!", False

    def search_for_series(
        self,
        series_name: str,
        callback: Callable[[int, int], None] | None = None,
        refresh_cache: bool = False,
        literal: bool = False,
        series_match_thresh: int = 90,
    ) -> list[ComicSeries]:
        search_series_name = utils.sanitize_title(series_name, literal)
        logger.info(f"{self.name} searching: {search_series_name}")

        # Before we search online, look in our cache, since we might have done this same search recently
        # For literal searches always retrieve from online
        cvc = ComicCacher(self.cache_folder, self.version)
        if not refresh_cache and not literal:
            cached_search_results = cvc.get_search_results(self.id, series_name)
            if len(cached_search_results) > 0:
                # Unpack to apply any filters
                json_cache: list[MUSeries] = [json.loads(x[0].data) for x in cached_search_results]
                if self.filter_nsfw:
                    json_cache = self._filter_nsfw(json_cache)
                if self.filter_dojin:
                    json_cache = self._filter_dojin(json_cache)

                return self._format_search_results(json_cache)

        params: dict[str, Any] = {
            "search": search_series_name,
            "page": 1,
            "perpage": 100,
        }

        mu_response = self._get_mu_content(urljoin(self.api_url, "series/search"), params)

        search_results: list[MUSeries] = []

        total_result_count = mu_response["total_hits"]

        # 1. Don't fetch more than some sane amount of pages.
        # 2. Halt when any result on the current page is less than or equal to a set ratio using thefuzz
        max_results = 500  # 5 pages

        current_result_count = mu_response["per_page"] * mu_response["page"]
        total_result_count = min(total_result_count, max_results)

        if callback is None:
            logger.debug(
                f"Found {mu_response['per_page'] * mu_response['page']} of {mu_response['total_hits']} results"
            )
        search_results.extend(s["record"] for s in mu_response["results"])
        page = 1

        if callback is not None:
            callback(current_result_count, total_result_count)

        # see if we need to keep asking for more pages...
        while current_result_count < total_result_count:
            if not literal:
                # Stop searching once any entry falls below the threshold
                stop_searching = any(
                    not utils.titles_match(search_series_name, volume["record"]["title"], series_match_thresh)
                    for volume in cast(list[MUResult], mu_response["results"])
                )

                if stop_searching:
                    break

            if callback is None:
                logger.debug(f"getting another page of results {current_result_count} of {total_result_count}...")
            page += 1

            params["page"] = page
            mu_response = self._get_mu_content(urljoin(self.api_url, "series/search"), params)

            search_results.extend(s["record"] for s in mu_response["results"])
            # search_results.extend(cast(list[MUResult], mu_response["results"]))
            # current_result_count += mu_response["number_of_page_results"]

            if callback is not None:
                callback(current_result_count, total_result_count)

        # Cache raw data
        cvc.add_search_results(
            self.id,
            series_name,
            [CCSeries(id=x["series_id"], data=json.dumps(x).encode("utf-8")) for x in search_results],
            False,
        )

        # Filter any tags AFTER adding to cache
        if self.filter_nsfw:
            search_results = self._filter_nsfw(search_results)
        if self.filter_dojin:
            search_results = self._filter_dojin(search_results)

        formatted_search_results = self._format_search_results(search_results)

        return formatted_search_results

    def fetch_comic_data(
        self, issue_id: str | None = None, series_id: str | None = None, issue_number: str = ""
    ) -> GenericMetadata:
        comic_data = GenericMetadata()
        # Could be sent "issue_id" only which is actually series_id
        if issue_id and series_id is None:
            series_id = issue_id

        if series_id is not None:
            return self._map_comic_issue_to_metadata(self._fetch_series(int(series_id)))

        return comic_data

    def fetch_issues_in_series(self, series_id: str) -> list[GenericMetadata]:
        # Manga Updates has no issue level data
        return [GenericMetadata()]

    @limiter.ratelimit("default", delay=True)
    def _get_mu_content(self, url: str, params: dict[str, Any]) -> MUResponse | MUSeries | MUPublisher:
        while True:
            mu_response = self._get_url_content(url, params)
            if mu_response.get("status") == "exception":
                logger.debug(f"{self.name} query failed with error {mu_response['reason']}.")
                raise TalkerNetworkError(self.name, 0, f"{mu_response['reason']}")

            break
        return mu_response

    def _get_url_content(self, url: str, params: dict[str, Any]) -> Any:
        for tries in range(3):
            try:
                if not params:
                    resp = requests.get(url, headers={"user-agent": "comictagger/" + self.version})
                else:
                    resp = requests.post(url, json=params, headers={"user-agent": "comictagger/" + self.version})

                if resp.status_code == requests.status_codes.codes.ok:
                    return resp.json()
                if resp.status_code == requests.status_codes.codes.server_error:
                    logger.debug(f"Try #{tries + 1}: ")
                    time.sleep(1)
                    logger.debug(str(resp.status_code))
                if resp.status_code == requests.status_codes.codes.bad_request:
                    logger.debug(f"Bad request: {resp.json()}")
                    raise TalkerNetworkError(self.name, 2, f"Bad request: {resp.json()}")
                if resp.status_code == requests.status_codes.codes.forbidden:
                    logger.debug(f"Forbidden: {resp.json()}")
                    raise TalkerNetworkError(self.name, 2, f"Forbidden: {resp.json()}")
                if resp.status_code == requests.status_codes.codes.not_found:
                    logger.debug(f"Manga not found: {resp.json()}")
                    raise TalkerNetworkError(self.name, 2, f"Manga not found: {resp.json()}")
                if resp.status_code == requests.status_codes.codes.too_many_requests:
                    logger.debug(f"Rate limit reached: {resp.json()}")
                    # If given a time to wait before re-trying, use that time + 1 sec
                    if resp.headers.get("x-ratelimit-retry-after", None):
                        wait_time = int(resp.headers["x-ratelimit-retry-after"]) - int(time.time())
                        if wait_time > 0:
                            time.sleep(wait_time + 1)
                    else:
                        time.sleep(5)
                else:
                    break

            except requests.exceptions.Timeout:
                logger.debug(f"Connection to {self.name} timed out.")
                raise TalkerNetworkError(self.name, 4)
            except requests.exceptions.RequestException as e:
                logger.debug(f"Request exception: {e}")
                raise TalkerNetworkError(self.name, 0, str(e)) from e
            except json.JSONDecodeError as e:
                logger.debug(f"JSON decode error: {e}")
                raise TalkerDataError(self.name, 2, f"{self.name} did not provide json")

        raise TalkerNetworkError(self.name, 5)

    def _format_search_results(self, search_results: list[MUSeries]) -> list[ComicSeries]:
        formatted_results = []
        for record in search_results:
            formatted_results.append(self._format_series(record))

        return formatted_results

    def _format_series(self, series: MUSeries) -> ComicSeries:
        aliases = set()
        for alias in series.get("associated", []):
            aliases.add(alias["title"])

        start_year: int | None = None
        if series.get("year"):
            start_year = utils.xlate_int(series["year"])

        publisher = None
        if series.get("publishers"):
            publisher_list = []
            for pub in series["publishers"]:
                if self.use_original_publisher and pub["type"] == "Original":
                    publisher_list.append(pub["publisher_name"])
                elif pub["type"] == "English":
                    publisher_list.append(pub["publisher_name"])
            publisher = ", ".join(publisher_list)

        count_of_issues = None
        if series.get("completed"):
            count_of_issues = series["latest_chapter"]

        return ComicSeries(
            aliases=aliases,
            count_of_issues=count_of_issues,
            description=series.get("description", ""),
            id=str(series["series_id"]),
            image_url=series["image"]["url"].get("original", ""),
            name=series.get("title", ""),
            publisher=publisher,
            start_year=start_year,
            count_of_volumes=None,
            format=None,
        )

    def _fetch_publisher(self, pub_id: int) -> MUPublisher:
        mu_response = self._get_mu_content(urljoin(self.api_url, f"publishers/{pub_id}"), {})

        return cast(MUPublisher, mu_response)

    def _filter_nsfw(self, search_results: list[MUSeries]) -> list[MUSeries]:
        filtered_list = []
        for series in search_results:
            if not any(genre in ["Adult", "Hentai"] for genre in series.get("genres", [])):
                filtered_list.append(series)

        return filtered_list

    def _filter_dojin(self, search_results: list[MUSeries]) -> list[MUSeries]:
        filtered_list = []
        for series in search_results:
            if "Doujinshi" not in series.get("genres", []):
                filtered_list.append(series)

        return filtered_list

    def fetch_series(self, series_id: str) -> ComicSeries:
        return self._format_series(self._fetch_series(int(series_id)))

    def _fetch_series(self, series_id: int) -> MUSeries:
        cvc = ComicCacher(self.cache_folder, self.version)
        cached_series = cvc.get_series_info(str(series_id), self.id)

        if cached_series is not None and cached_series[1]:
            return json.loads(cached_series[0].data)

        issue_url = urljoin(self.api_url, f"series/{series_id}")
        mu_response: MUSeries = self._get_mu_content(issue_url, {})

        # Series will now have publisher so update cache record
        # Cache raw data
        cvc.add_series_info(
            self.id,
            CCSeries(id=str(series_id), data=json.dumps(mu_response).encode("utf-8")),
            True,
        )

        return mu_response

    def fetch_issues_by_series_issue_num_and_year(
        self, series_id_list: list[str], issue_number: str, year: str | int | None
    ) -> list[GenericMetadata]:
        series_list = []
        for series_id in series_id_list:
            series_list.append(self._map_comic_issue_to_metadata(self._fetch_series(int(series_id))))

        return series_list

    def _map_comic_issue_to_metadata(self, series: MUSeries) -> GenericMetadata:
        md = GenericMetadata(
            data_origin=MetadataOrigin(self.id, self.name),
            series_id=utils.xlate(series["series_id"]),
            issue_id=utils.xlate(series["series_id"]),
        )
        md.cover_image = series["image"]["url"].get("original", "")

        for alias in series["associated"]:
            md.series_aliases.add(alias["title"])

        publisher_list = []
        for pub in series["publishers"]:
            if not self.use_original_publisher and pub["type"] == "English":
                publisher_list.append(pub["publisher_name"])
            else:
                publisher_list.append(pub["publisher_name"])

        md.publisher = ", ".join(publisher_list)

        for person in series["authors"]:
            md.add_credit(person["name"], person["type"])

        # Types: Artbook, Doujinshi, Drama CD, Filipino, Indonesian, Manga, Manhwa, Manhua, Novel, OEL, Thai,
        # Vietnamese, Malaysian, Nordic, French, Spanish
        if series["type"] in ["Manga", "Doujinshi"]:
            md.manga = "Yes"

        for genre in series["genres"]:
            md.genres.add(genre["genre"])
            if genre in ["Adult", "Hentai"]:
                md.mature_rating = "Adult"

        for cat in series["categories"]:
            md.tags.add(cat["category"])

        count_of_volumes: int | None = None  # TODO parse from publisher notes depending on lang?
        reg = re.compile(r"((\d+).*volume.).*(complete)(.*)", re.IGNORECASE)
        reg_match = reg.search(series["status"])
        if reg_match is not None:
            count_of_volumes = utils.xlate_int(reg_match.group(2))

        # Marked as complete so latest_chapter can be confirmed as number of chapters
        if series["completed"] or self.use_ongoing_issue_count:
            md.count_of_issues = series["latest_chapter"]
        md.count_of_volumes = count_of_volumes

        md.year = utils.xlate_int(series.get("year"))

        md.description = series.get("description")

        md.web_link = series["url"]

        if self.use_series_start_as_volume and md.year:
            md.volume = md.year

        return md
