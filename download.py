from search import Search
from luigi.date_interval import Month
import pickle
import luigi
import os
import logging

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO)

logger = logging.getLogger('arxiv')


class Download(luigi.WrapperTask):

    download_dir = luigi.Parameter(default='./data/download')
    year = luigi.IntParameter(default=2018)

    def requires(self):

        os.makedirs(self.download_dir, exist_ok=True)
        dates = [Month(self.year, m) for m in range(1, 12)]

        for interval in dates:
            yield DownloadInterval(
                interval=interval,
                download_dir=self.download_dir)


class DownloadInterval(luigi.Task):

    interval = luigi.DateIntervalParameter()

    download_dir = luigi.Parameter(default='./data/download')

    def requires(self):
        return []

    def run(self):
        search = Search(
            category="stat.ML",
            date_from=self.interval.date_a.isoformat(),
            date_to=self.interval.date_b.isoformat())

        logger.info(
            'Start downloading for range {}'.format(self.interval))

        data = dict()
        for entry in search.get_results():
                data[entry['id']] = entry

        logger.info('Finish data loading')

        logger.info('Persisting data')
        with open(self.output().path, 'wb') as f:
            pickle.dump(data, f)

    def output(self):

        filename = 'arxiv_{}.pkl'.format(self.interval)
        outfile = os.path.join(self.download_dir, filename)

        return luigi.LocalTarget(outfile)


if __name__ == "__main__":

    luigi.build([Download(year=2017)], workers=8)
