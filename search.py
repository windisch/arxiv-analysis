import requests
import logging
import feedparser
import pickle
from urllib.parse import urlencode

logger = logging.getLogger(__name__)


class Search(object):
    """

    Args:
        keyword (string): The thing after which should be searched
        category (string): The category of arXiv that should be searched. Defaults to :code:`all`
    """

    url = "http://export.arxiv.org/api/query"

    def __init__(self, keyword=None, category="all", max_results_per_call=10, date_from=None,
                 date_to=None):

        self.query = self._get_query(
            keyword=keyword,
            category=category,
            date_from=date_from.replace('-', ''),
            date_to=date_to.replace('-', ''))

        self.max_results = max_results_per_call
        self.sort_by = 'submittedDate'
        self.sort_order = 'descending'

    def _get_query(self, keyword=None, category=None, date_from=None, date_to=None):
        search_query = "{}:{}".format(category, keyword) if keyword else category

        if date_from and date_to:
            search_query = "{}+AND+submittedDate:[{}+TO+{}]".format(
                search_query, date_from, date_to)

        logger.info('Use query {}'.format(search_query))

        return search_query

    def _get_url(self, start):

        url_args = urlencode({
            "start": start,
            "max_results": self.max_results,
            "sortBy": self.sort_by,
            "sortOrder": self.sort_order})

        url = '{}?search_query={}&{}'.format(self.url, self.query, url_args)
        return url

    def _fetch_entries(self, url):
        logging.info('Fetch entries from arXiv')
        r = requests.get(url)

        return feedparser.parse(r.text)

    def _normalize_text(self, text):

        text = text.replace('\n', '')
        text = text.replace('\r', '')
        text = text.replace('  ', ' ')
        text = text.strip()
        return text

    def _extract_id_from_url(self, url):
        """
        Args:
            url (string): The arXiv url that points to the paper

        """
        return url.replace("http://arxiv.org/abs/", "")

    def _process_entry(self, entry):
        """

        Args:
            entry (dict): Dict received by the arXiv
        """

        result = {
            'summary': self._normalize_text(entry['summary']),
            'id': self._extract_id_from_url(entry['id']),
            'title': self._normalize_text(entry['title']),
            'published': entry['published'],
            'authors': [a['name'] for a in entry['authors']]
            }

        return result

    def get_results(self):
        """

        """

        start = 0
        while True:

            url = self._get_url(start)
            start = start + self.max_results

            data = self._fetch_entries(url)

            # If no entries have been returned, break
            if len(data["entries"]) == 0:
                break

            for entry in data["entries"]:
                yield self._process_entry(entry)

    def save(self, filepath=None):
        """
        Persist the result of the search query
        """
        data = dict()
        for entry in self.get_results():
            data[entry['id']] = entry

        logger.info('Finish data loading')
        with open(filepath, 'wb') as f:
            logger.info('Persisting data')
            pickle.dump(data, f)
