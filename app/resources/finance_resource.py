from flask_restx import Namespace, Resource
# from ..finance_most_active.finance_most_active.spiders.most_active import MostActiveSpider
from twisted.internet import reactor
from scrapydo import setup
from scrapy.crawler import CrawlerRunner
from concurrent.futures import ThreadPoolExecutor

# Call setup() to configure Scrapy to run in a separate thread
setup()
ns_f = Namespace("finance")


@ns_f.route('/finance_scrape')
class ScrapeResource(Resource):
    def post(self):
        try:
            # Create a CrawlerRunner with project settings
            runner = CrawlerRunner()

            # Add your Scrapy spider to the runner
            runner.crawl(FinanceDataSpider)

            # Create a thread pool to run the reactor in
            def run_reactor():
                reactor.run(installSignalHandlers=False)

            thread_pool = ThreadPoolExecutor(max_workers=1)
            thread_pool.submit(run_reactor)

            # Return a response immediately
            return {'message': 'Scraping started'}, 200
        except Exception as e:
            return {'error': str(e)}, 500


@ns_f.route('/finance_most_active')
class MostActive(Resource):
    def post(self):
        try:
            # Create a CrawlerRunner with project settings
            runner = CrawlerRunner()

            # Add your Scrapy spider to the runner
            runner.crawl(MostActiveSpider)

            # Create a thread pool to run the reactor in
            def run_reactor():
                reactor.run(installSignalHandlers=False)

            thread_pool = ThreadPoolExecutor(max_workers=1)
            thread_pool.submit(run_reactor)

            # Return a response immediately
            return {'message': 'Scraping started'}, 200
        except Exception as e:
            return {'error': str(e)}, 500


if __name__ == '__main__':
    ns_f.run()
