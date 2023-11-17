from flask import request

from ..scrape.scrape.spiders.stock_csx_current_index import StockCSXCurrentIndex
from ..scrape.scrape.spiders.stock_trade_summary_all import StockTradeSummaryAll
from ..scrape.scrape.spiders.stock_detail_data import StockDetailDataSpider
from ..scrape.scrape.spiders.stock_historical_data import StockHistoricalDataSpider
from ..scrape.scrape.spiders.finance_data import FinanceDataSpider
from ..scrape.scrape.spiders.most_active import MostActiveSpider
from ..scrape.scrape.spiders.stock_trade_summary import StockTradeSummarySpider
from ..scrape.scrape.spiders.stock_csx_index import StockCSXIndex
from flask_restx import Namespace, Resource
from twisted.internet import reactor
from scrapydo import setup
from scrapy.crawler import CrawlerRunner
from concurrent.futures import ThreadPoolExecutor
from ..database.connect_db import conn
from flask_jwt_extended import jwt_required
from datetime import datetime
import pytz

phnom_penh = pytz.timezone('Asia/Phnom_Penh')
current_datetime = datetime.now(phnom_penh)
setup()

ns_stock = Namespace("api/v1/stock")


# _________________________________________  100% all route _________________________________________
@ns_stock.route('/local_stock_historical_data')
class ScrapeResource(Resource):

    @jwt_required(refresh=False)
    @ns_stock.doc(security="Bearer")
    def post(self):

        try:
            # Create a CrawlerRunner with project settings
            runner = CrawlerRunner()

            # Add your Scrapy spider to the runner
            runner.crawl(StockHistoricalDataSpider)

            # Create a thread pool to run the reactor in
            def run_reactor():
                reactor.run(installSignalHandlers=False)

            thread_pool = ThreadPoolExecutor(max_workers=1)
            thread_pool.submit(run_reactor)

            # Return a response immediately
            return {
                'date': datetime.now().isoformat(),
                'success': True,
                'message': 'scraped successfully'
            }, 200
        except Exception as e:
            return {'success': False,
                    'error': str(e)}, 404

    @jwt_required(refresh=False)
    @ns_stock.doc(security="Bearer")
    @ns_stock.doc(params={'scraped_date': 'The date timestamp for the update without millisecond'})
    def put(self):
        scraped_date = request.args.get('scraped_date')

        try:
            datetime.strptime(scraped_date, '%Y-%m-%d %H:%M:%S')
        except ValueError:

            return {'success': False,
                    'message': 'Invalid scraped_date format, valid format is YYYY-MM-DD HH:MM:SS'}, 400

        # Validate the scraped_date parameter (you can add more validation)
        if scraped_date is None:
            return {'success': False, 'message': 'Missing scraped_date parameter'}, 400

        # Update the resource in the database using the scraped_date parameter

        cursor = conn.cursor()
        check_date_query = "SELECT status FROM local_stock_historical_data WHERE TO_CHAR" \
                           "(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = %s"
        cursor.execute(check_date_query, (scraped_date,))
        date_exists = cursor.fetchone()

        if not date_exists:
            return {'success': False, 'message': 'No data scraped in ' + scraped_date}, 404

        if date_exists and date_exists[0] == 'published':
            return {
                'success': False,
                'message': 'data is already updated'
            }, 400
        update_historical_query = """
                                UPDATE local_stock_historical_data
                                SET status = %s
                                WHERE TO_CHAR(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = %s;
                                """
        try:
            cursor.execute(update_historical_query, ('published', scraped_date))
            conn.commit()
            return {
                'success': True,
                'message': 'Updated successfully'
            }
        except Exception as e:
            conn.rollback()
            return {'success': False, 'message': 'Error updating resource: {}'.format(str(e))}, 400

    def get(self):
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM local_stock_historical_data")
            historical_stock = cursor.fetchall()
            if not historical_stock:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []

            # Iterate through the records and format them
            for record in historical_stock:
                response_data.append({
                    'date': record[0],
                    'close_price': record[1],
                    'change': record[2],
                    'change_status': record[3],
                    'trading_volume_shr': record[4],
                    'trading_volume_khr': record[5],
                    'opening': record[6],
                    'high': record[7],
                    'low': record[8],
                    'market_cap': record[9],
                    'full_market_cap': record[10],
                    'category': record[11],
                    'status': record[12]
                })

            return {
                'date': datetime.now().isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@ns_stock.route('/local_stock_historical_data/published')
class local_historical(Resource):

    def get(self):
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM local_stock_historical_data where status = 'published'")
            historical_stock_published = cursor.fetchall()
            if not historical_stock_published:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []

            # Iterate through the records and format them
            for record in historical_stock_published:
                response_data.append({
                    'date': record[0],
                    'close_price': record[1],
                    'change': record[2],
                    'change_status': record[3],
                    'trading_volume_shr': record[4],
                    'trading_volume_khr': record[5],
                    'opening': record[6],
                    'high': record[7],
                    'low': record[8],
                    'market_cap': record[9],
                    'full_market_cap': record[10],
                    'category': record[11],
                    'status': record[12]
                })

            return {
                'date': datetime.now().isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@ns_stock.route('/local_stock')
class ScrapeResource(Resource):
    @jwt_required(refresh=False)
    @ns_stock.doc(security="Bearer")
    def post(self):

        try:
            runner = CrawlerRunner()

            # Add your Scrapy spider to the runner
            runner.crawl(StockTradeSummarySpider)

            # Create a thread pool to run the reactor in
            def run_reactor():
                reactor.run(installSignalHandlers=False)

            thread_pool = ThreadPoolExecutor(max_workers=1)
            thread_pool.submit(run_reactor)

            # Return a response immediately
            return {
                'date': datetime.now().isoformat(),
                'success': True,
                'message': 'scraped successfully'
            }, 200
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }, 404

    @jwt_required(refresh=False)
    @ns_stock.doc(security="Bearer")
    @ns_stock.doc(params={'scraped_date': 'The date timestamp for the update without millisecond'})
    def put(self):
        scraped_date = request.args.get('scraped_date')
        try:
            datetime.strptime(scraped_date, '%Y-%m-%d %H:%M:%S')
        except ValueError:

            return {'success': False,
                    'message': 'Invalid scraped_date format, valid format is YYYY-MM-DD HH:MM:SS'}, 400
        # Validate the scraped_date parameter (you can add more validation)
        if scraped_date is None:
            return {'success': False, 'message': 'Invalid or missing scraped_date parameter'}, 400

        # Update the resource in the database using the scraped_date parameter

        cursor = conn.cursor()
        check_date_query = "SELECT status FROM local_stock_summary WHERE TO_CHAR" \
                           "(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = %s"
        cursor.execute(check_date_query, (scraped_date,))
        date_exists = cursor.fetchone()

        if not date_exists:
            return {'success': False, 'message': 'No data scraped in ' + scraped_date}, 404
        if date_exists and date_exists[0] == 'published':
            return {
                'success': False,
                'message': 'data is already updated'
            }, 400
        update_query = """
                                    UPDATE local_stock_summary
                                    SET status = %s
                                    WHERE TO_CHAR(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = %s;
                                    """
        try:
            cursor.execute(update_query, ('published', scraped_date))
            conn.commit()
            return {
                'success': True,
                'message': 'Updated successfully'
            }
        except Exception as e:
            conn.rollback()
            return {'success': False, 'message': 'Error updating resource: {}'.format(str(e))}, 400

    def get(self):
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM local_stock_summary")
            historical_stock_published = cursor.fetchall()
            if not historical_stock_published:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []

            # Iterate through the records and format them
            for record in historical_stock_published:
                response_data.append({
                    'stock': record[0],
                    'close': record[1],
                    'change': record[2],
                    'open': record[3],
                    'high': record[4],
                    'low': record[5],
                    'volume_share': record[6],
                    'value_KHR': record[7],
                    'p_e': record[8],
                    'p_b': record[9],
                    'change_status': record[10],
                    'date': record[11].isoformat(),
                    'status': record[12]
                })

            return {
                'date': datetime.now().isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@ns_stock.route('/local_stock/published')
class local_stock_symbol(Resource):

    def get(self):
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM local_stock_summary where status = 'published'")
            local_stock_published = cursor.fetchall()
            if not local_stock_published:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in local_stock_published:
                response_data.append({
                    'stock': record[0],
                    'close': record[1],
                    'change': record[2],
                    'open': record[3],
                    'high': record[4],
                    'low': record[5],
                    'volume_share': record[6],
                    'value_KHR': record[7],
                    'p_e': record[8],
                    'p_b': record[9],
                    'change_status': record[10],
                    'date': record[11].isoformat(),
                    'status': record[12]
                })

            return {
                'date': datetime.now().isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@ns_stock.route('/local_stock_symbol')
class ScrapeResource(Resource):

    @jwt_required(refresh=False)
    @ns_stock.doc(security="Bearer")
    def post(self):
        try:
            # Create a CrawlerRunner with project settings
            runner = CrawlerRunner()

            # Add your Scrapy spider to the runner
            runner.crawl(StockDetailDataSpider)

            # Create a thread pool to run the reactor in
            def run_reactor():
                reactor.run(installSignalHandlers=False)

            thread_pool = ThreadPoolExecutor(max_workers=1)
            thread_pool.submit(run_reactor)

            # Return a response immediately
            return {
                'success': True,
                'message': 'scraped successfully'
            }, 200
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }, 404

    @ns_stock.route('/finance_scrape')
    class ScrapeResource(Resource):
        method_decorators = [jwt_required()]

    @jwt_required(refresh=False)
    @ns_stock.doc(security="Bearer")
    @ns_stock.doc(params={'scraped_date': 'The date timestamp for the update without millisecond'})
    def put(self):
        scraped_date = request.args.get('scraped_date')

        try:
            datetime.strptime(scraped_date, '%Y-%m-%d %H:%M:%S')
        except ValueError:

            return {'success': False,
                    'message': 'Invalid scraped_date format, valid format is YYYY-MM-DD HH:MM:SS'}, 400
        # Validate the scraped_date parameter (you can add more validation)
        if scraped_date is None:
            return {'success': False, 'message': 'Invalid or missing scraped_date parameter'}, 400

        # Update the resource in the database using the scraped_date parameter

        cursor = conn.cursor()
        check_date_query = "SELECT status FROM stock_detail WHERE TO_CHAR" \
                           "(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = %s"
        cursor.execute(check_date_query, (scraped_date,))
        date_exists = cursor.fetchone()

        if not date_exists:
            return {'success': False, 'message': 'No data scraped in ' + scraped_date}, 404
        if date_exists and date_exists[0] == 'published':
            return {
                'success': False,
                'message': 'data is already updated'
            }, 400
        update_query = """
                                        UPDATE stock_detail
                                        SET status = %s
                                        WHERE TO_CHAR(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = %s;
                                        """
        try:
            cursor.execute(update_query, ('published', scraped_date))
            conn.commit()
            return {
                'success': True,
                'message': 'Updated successfully'
            }
        except Exception as e:
            conn.rollback()
            return {'success': False, 'message': 'Error updating resource: {}'.format(str(e))}, 404

    def get(self):
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM stock_detail")
                stock = cursor.fetchall()
                if not stock:
                    return {
                        'success': True,
                        "message: ": "data is empty"}, 200
                response_data = []

                for record in stock:
                    response_data.append({
                        'symbol': record[0],
                        'company_name': record[1],
                        'date': record[2],
                        'status': record[3]
                    })

                return {
                    'date': datetime.now().isoformat(),
                    'success': True,
                    'message': 'get data successfully',
                    'payload': response_data
                }, 200
        except Exception:
            return {'success': False, "error": "An error occurred while processing your request."}, 404

    @ns_stock.route('/local_stock_symbol/published')
    class LocalStockSymbol(Resource):

        def get(self):
            try:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT * FROM stock_detail where status = 'published'")
                    stock = cursor.fetchall()
                    if not stock:
                        return {
                            'success': True,
                            "message: ": "data is empty"}, 200
                    response_data = []

                    for record in stock:
                        response_data.append({
                            'symbol': record[0],
                            'company_name': record[1],
                            'date': record[2],
                            'status': record[3]
                        })

                    return {
                        'date': datetime.now().isoformat(),
                        'success': True,
                        'message': 'get data successfully',
                        'payload': response_data
                    }, 200
            except Exception:
                return {
                    'success': False,
                    "error": "An error occurred while processing your request."}, 400


@ns_stock.route('/global_stock')
class ScrapeGlobalStock(Resource):

    @jwt_required(refresh=False)
    @ns_stock.doc(security="Bearer")
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
            return {
                'success': True,
                'message': 'scraped successfully'
            }, 200
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }, 404

    @jwt_required(refresh=False)
    @ns_stock.doc(security="Bearer")
    @ns_stock.doc(params={'scraped_date': 'The date timestamp for the update without millisecond'})
    def put(self):
        scraped_date = request.args.get('scraped_date')
        try:
            datetime.strptime(scraped_date, '%Y-%m-%d %H:%M:%S')
        except ValueError:

            return {'success': False,
                    'message': 'Invalid scraped_date format, valid format is YYYY-MM-DD HH:MM:SS'}, 400
        # Validate the scraped_date parameter (you can add more validation)
        if scraped_date is None:
            return {'success': False, 'message': 'Invalid or missing scraped_date parameter'}, 400

        # Update the resource in the database using the scraped_date parameter

        cursor = conn.cursor()
        check_date_query = "SELECT status FROM global_stock WHERE TO_CHAR" \
                           "(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = %s"
        cursor.execute(check_date_query, (scraped_date,))
        date_exists = cursor.fetchone()

        if not date_exists:
            return {'success': False, 'message': 'No data scraped in ' + scraped_date}, 404
        if date_exists and date_exists[0] == 'published':
            return {
                'success': False,
                'message': 'data is already updated'
            }, 400
        update_query = """
                            UPDATE global_stock
                            SET status = %s
                            WHERE TO_CHAR(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = %s;
                            """
        try:
            cursor.execute(update_query, ('published', scraped_date))
            conn.commit()
            return {
                'success': True,
                'message': 'Updated successfully'
            }
        except Exception as e:
            conn.rollback()
            return {'success': False, 'message': 'Error updating resource: {}'.format(str(e))}, 400

    def get(self):
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM global_stock")
            global_stock = cursor.fetchall()
            if not global_stock:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in global_stock:
                response_data.append({
                    'stock': record[0],
                    'close_price': record[1],
                    'change': record[2],
                    'percentage_change': record[3],
                    'category': record[4],
                    'date': record[5].isoformat(),
                    'status': record[6]
                })

            return {
                'date': datetime.now().isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@ns_stock.route('/global_stock/published')
class ScrapeResource(Resource):

    def get(self):
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM global_stock where status = 'published'")
            global_stock = cursor.fetchall()
            if not global_stock:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            print("______________global_stock______________ : ", global_stock)
            response_data = []

            # Iterate through the records and format them
            for record in global_stock:
                response_data.append({
                    'stock': record[0],
                    'close_price': record[1],
                    'change': record[2],
                    'percentage_change': record[3],
                    'category': record[4],
                    'date': record[5].isoformat(),
                    'status': record[6]
                })

            return {
                'date': datetime.now().isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200

    @ns_stock.route('/global_stock_most_active')
    class MostActive(Resource):

        @jwt_required(refresh=False)
        @ns_stock.doc(security="Bearer")
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
                return {
                    'success': True,
                    'message': 'scraped successfully'
                }, 200
            except Exception as e:
                return {
                    'success': False,
                    'error': str(e)
                }, 404

        @jwt_required(refresh=False)
        @ns_stock.doc(security="Bearer")
        @ns_stock.doc(params={'scraped_date': 'The date timestamp for the update without millisecond'})
        def put(self):
            scraped_date = request.args.get('scraped_date')
            try:
                datetime.strptime(scraped_date, '%Y-%m-%d %H:%M:%S')
            except ValueError:

                return {'success': False,
                        'message': 'Invalid scraped_date format, valid format is YYYY-MM-DD HH:MM:SS'}, 400
            # Validate the scraped_date parameter (you can add more validation)
            if scraped_date is None:
                return {'success': False, 'message': 'Invalid or missing scraped_date parameter'}, 400

            # Update the resource in the database using the scraped_date parameter

            cursor = conn.cursor()
            check_date_query = "SELECT status FROM global_stock_most_active WHERE TO_CHAR" \
                               "(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = %s"
            cursor.execute(check_date_query, (scraped_date,))
            date_exists = cursor.fetchone()

            if not date_exists:
                return {'success': False, 'message': 'No data scraped in ' + scraped_date}, 404
            if date_exists and date_exists[0] == 'published':
                return {
                    'success': False,
                    'message': 'data is already updated'
                }, 400
            update_query = """
                                    UPDATE global_stock_most_active
                                    SET status = %s
                                    WHERE TO_CHAR(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = %s;
                                    """
            try:
                cursor.execute(update_query, ('published', scraped_date))
                conn.commit()
                return {
                    'success': True,
                    'message': 'Updated successfully'
                }
            except Exception as e:
                conn.rollback()
                return {'success': False, 'message': 'Error updating resource: {}'.format(str(e))}, 400

        def get(self):
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM global_stock_most_active")
                most_active_finance = cursor.fetchall()
                if not most_active_finance:
                    return {
                        'success': True,
                        "message: ": "data is empty"}, 200
                # print("______________historical_stock______________ : ", historical_stock)
                response_data = []

                # Iterate through the records and format them
                for record in most_active_finance:
                    response_data.append({
                        'symbol': record[6],
                        'stock': record[0],
                        'close_price': record[1],
                        'change': record[2],
                        'percentage_change': record[3],
                        'date': record[4].isoformat(),
                        'status': record[5]
                    })

                return {
                    'date': datetime.now().isoformat(),
                    'success': True,
                    'message': 'get data successfully',
                    'payload': response_data
                }, 200


@ns_stock.route('/global_stock_most_active/published')
class MostActive(Resource):

    def get(self):
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM global_stock_most_active where status = 'published'")
            most_active_finance = cursor.fetchall()
            if not most_active_finance:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []

            # Iterate through the records and format them
            for record in most_active_finance:
                response_data.append({
                    'symbol': record[6],
                    'stock': record[0],
                    'close_price': record[1],
                    'change': record[2],
                    'percentage_change': record[3],
                    'date': record[4].isoformat(),
                    'status': record[5]
                })

            return {
                'date': datetime.now().isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@ns_stock.route('/local_stock/stock_name_detail')
class ScrapeResource(Resource):

    def get(self):
        with conn.cursor() as cursor:
            cursor.execute("select * from local_stock_summary inner join stock_detail sd on "
                           "local_stock_summary.stock = sd.symbol where sd.status = 'published' "
                           "and local_stock_summary.status = 'published'")
            local_stock_detail = cursor.fetchall()
            if not local_stock_detail:
                return {'success': True,
                        "message": "data is empty"}, 200
            response_data = []

            # Iterate through the records and format them
            for record in local_stock_detail:
                response_data.append({
                    'symbol': record[13],
                    'company_name': record[14],
                    'registered_date': record[15],
                    'close_price': record[1],
                    'opening_price': record[3],
                    'high_price': record[4],
                    'low_price': record[5],
                    'change': record[2],
                    'change_status': record[10],
                    'volume_share': record[6],
                    'value_KHMER': record[7],
                    'p_e': record[8],
                    'p_b': record[9],
                    'scraped_date': record[11].isoformat(),
                    'status': record[12],

                })

            return {
                'date': datetime.now().isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@ns_stock.route('/local/both')
class ScrapeResource(Resource):
    # method_decorators = [jwt_required()]

    @jwt_required(refresh=False)
    @ns_stock.doc(security="Bearer")
    def post(self):
        scrape_number = 0
        try:
            # Create a CrawlerRunner with project settings
            runner = CrawlerRunner()
            historical = runner.crawl(StockHistoricalDataSpider)
            normal = runner.crawl(StockTradeSummarySpider)
            index = runner.crawl(StockCSXIndex)
            current_index = runner.crawl(StockCSXCurrentIndex)
            summary_all = runner.crawl(StockTradeSummaryAll)

            # Create a thread pool to run the reactor in
            def run_reactor():
                reactor.run(installSignalHandlers=False)

            thread_pool = ThreadPoolExecutor(max_workers=1)
            thread_pool.submit(run_reactor)

            cursor = conn.cursor()
            scrape_number_query = "SELECT * FROM count_scrape_stock_local(1)"
            cursor.execute(scrape_number_query)

            if cursor.rowcount > 0:
                scrape_number = cursor.fetchone()[0]

            conn.commit()
            return {
                'success': True,
                'message': 'scraped successfully',
                'scraped_number': scrape_number
            }, 200
        except Exception as e:
            return {'success': False, 'error': str(e)}, 404


@ns_stock.route('/global/both', doc={'description': 'Scrape most active and current stock'})
class ScrapeResource(Resource):

    @jwt_required(refresh=False)
    @ns_stock.doc(security="Bearer")
    def post(self):
        scrape_number = 0
        try:
            runner = CrawlerRunner()
            global_stock = runner.crawl(FinanceDataSpider)
            local_stock = runner.crawl(MostActiveSpider)

            def run_reactor():
                reactor.run(installSignalHandlers=False)

            thread_pool = ThreadPoolExecutor(max_workers=1)
            thread_pool.submit(run_reactor)

            cursor = conn.cursor()
            scrape_number_query = "SELECT * FROM count_scrape_stock_global(1)"
            cursor.execute(scrape_number_query)

            if cursor.rowcount > 0:
                scrape_number = cursor.fetchone()[0]

            conn.commit()
            return {
                'success': True,
                'message': 'scraped successfully',
                'scraped_number': scrape_number
            }, 200
        except Exception as e:
            return {'success': False, 'error': str(e)}, 404


# unpublished local

@ns_stock.route('/local_stock/unpublished')
class unpublished(Resource):
    @jwt_required(refresh=False)
    @ns_stock.doc(security="Bearer")
    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                """
                    SELECT scraped_date FROM local_stock_summary where status = 'unpublished'
                    GROUP BY scraped_date
                    ORDER BY scraped_date DESC;
                """
            )
            stock_response = cursor.fetchall()
            if not stock_response:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in stock_response:
                response_data.append({
                    'category': 'Stocks',
                    'part': 'Local',
                    'description': 'Current Data',
                    'scraped_date': record[0].isoformat()
                })
            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


# unpublished global stock
@ns_stock.route('/global_stock/unpublished')
class unpublished(Resource):
    @jwt_required(refresh=False)
    @ns_stock.doc(security="Bearer")
    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                """
                    SELECT scraped_date FROM global_stock where status = 'unpublished'
                    GROUP BY scraped_date
                    ORDER BY scraped_date DESC;
                """
            )
            stock_response = cursor.fetchall()
            if not stock_response:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in stock_response:
                response_data.append({
                    'category': 'Stocks',
                    'part': 'Global Stock',
                    'description': 'Current Data',
                    'scraped_date': record[0].isoformat()
                })
            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


# unpublished global stock most active
@jwt_required(refresh=False)
@ns_stock.doc(security="Bearer")
@ns_stock.route('/global_stock_most_active/unpublished')
class unpublished(Resource):
    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                """
                    SELECT scraped_date FROM global_stock_most_active where status = 'unpublished'
                    GROUP BY scraped_date
                    ORDER BY scraped_date DESC;
                """
            )
            stock_response = cursor.fetchall()
            if not stock_response:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in stock_response:
                response_data.append({
                    'category': 'Stocks',
                    'part': 'Global Stock Most Active',
                    'description': 'Current Data',
                    'scraped_date': record[0].isoformat()
                })
            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


# unpublished
@jwt_required(refresh=False)
@ns_stock.doc(security="Bearer")
@ns_stock.route('/local_stock_historical_data/unpublished')
class unpublished(Resource):
    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                """
                    SELECT scraped_date FROM local_stock_historical_data where status = 'unpublished'
                    GROUP BY scraped_date
                    ORDER BY scraped_date DESC;
                """
            )
            stock_response = cursor.fetchall()
            if not stock_response:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in stock_response:
                response_data.append({
                    'category': 'Stocks',
                    'part': 'Local',
                    'description': 'Historical Data',
                    'scraped_date': record[0].isoformat()
                })
            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


# unpublished
@jwt_required(refresh=False)
@ns_stock.doc(security="Bearer")
@ns_stock.route('/local_stock_symbol/unpublished')
class unpublished(Resource):
    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                """
                    SELECT scraped_date FROM stock_detail where status = 'unpublished'
                    GROUP BY scraped_date
                    ORDER BY scraped_date DESC;
                """
            )
            stock_response = cursor.fetchall()
            if not stock_response:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in stock_response:
                response_data.append({
                    'category': 'Stocks',
                    'part': 'Local Stock Symbol',
                    'description': 'Historical Data',
                    'scraped_date': record[0].isoformat()
                })
            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@ns_stock.route('/csx_index')
class ScrapeResource(Resource):

    @jwt_required(refresh=False)
    @ns_stock.doc(security="Bearer")
    def post(self):
        try:
            # Create a CrawlerRunner with project settings
            runner = CrawlerRunner()

            # Add your Scrapy spider to the runner
            runner.crawl(StockCSXIndex)

            # Create a thread pool to run the reactor in
            def run_reactor():
                reactor.run(installSignalHandlers=False)

            thread_pool = ThreadPoolExecutor(max_workers=1)
            thread_pool.submit(run_reactor)

            # Return a response immediately
            return {
                'success': True,
                'message': 'scraped successfully'
            }, 200
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }, 404

    def get(self):
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM stock_csx_index where status = 'published'")
            stock_csx_index = cursor.fetchall()
            if not stock_csx_index:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []

            # Iterate through the records and format them
            for record in stock_csx_index:
                response_data.append({
                    'id': record[0],
                    'date': record[1],
                    'current_index': record[2],
                    'change_percentage': record[3],
                    'change_status': record[12],
                    'opening': record[4],
                    'high_price': record[5],
                    'low_price': record[6],
                    'trading_volume_share': record[7],
                    'trading_value_khr': record[8],
                    'market_cap': record[9],
                    'status': record[10],
                    'scraped_date': record[11].isoformat(),
                })

            return {
                'date': datetime.now().isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200

    @jwt_required(refresh=False)
    @ns_stock.doc(security="Bearer")
    @ns_stock.doc(params={'scraped_date': 'The date timestamp for the update without millisecond'})
    def put(self):
        scraped_date = request.args.get('scraped_date')

        try:
            datetime.strptime(scraped_date, '%Y-%m-%d %H:%M:%S')
        except ValueError:

            return {'success': False,
                    'message': 'Invalid scraped_date format, valid format is YYYY-MM-DD HH:MM:SS'}, 400

        # Validate the scraped_date parameter (you can add more validation)
        if scraped_date is None:
            return {'success': False, 'message': 'Missing scraped_date parameter'}, 400

        # Update the resource in the database using the scraped_date parameter

        cursor = conn.cursor()
        check_date_query = "SELECT status FROM stock_csx_index WHERE TO_CHAR" \
                           "(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = %s"
        cursor.execute(check_date_query, (scraped_date,))
        date_exists = cursor.fetchone()

        if not date_exists:
            return {'success': False, 'message': 'No data scraped in ' + scraped_date}, 404

        if date_exists and date_exists[0] == 'published':
            return {
                'success': False,
                'message': 'data is already updated'
            }, 400
        update_historical_query = """
                                    UPDATE stock_csx_index
                                    SET status = %s
                                    WHERE TO_CHAR(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = %s;
                                    """
        try:
            cursor.execute(update_historical_query, ('published', scraped_date))
            conn.commit()
            return {
                'success': True,
                'message': 'Updated successfully'
            }
        except Exception as e:
            conn.rollback()
            return {'success': False, 'message': 'Error updating resource: {}'.format(str(e))}, 400


@ns_stock.route('/csx_index/unpublished')
class unpublished(Resource):
    @jwt_required(refresh=False)
    @ns_stock.doc(security="Bearer")
    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                """
                    SELECT scraped_date FROM stock_csx_index where status = 'unpublished'
                    GROUP BY scraped_date
                    ORDER BY scraped_date DESC;
                """
            )
            stock_response = cursor.fetchall()
            if not stock_response:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in stock_response:
                response_data.append({
                    'category': 'Stocks',
                    'part': 'CSX Index',
                    'description': 'Historical Data',
                    'scraped_date': record[0].isoformat()
                })
            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@ns_stock.route('/csx_index/latest_date')
class ScrapeResource(Resource):

    def get(self):
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM stock_csx_index WHERE status = 'published' "
                           "ORDER BY TO_DATE(date, 'DD/MM/YYYY') DESC LIMIT 1")
            stock_csx_index = cursor.fetchall()
            if not stock_csx_index:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []

            # Iterate through the records and format them
            for record in stock_csx_index:
                response_data.append({
                    'id': record[0],
                    'date': record[1],
                    'current_index': record[2],
                    'change_percentage': record[3],
                    'change_status': record[12],
                    'opening': record[4],
                    'high_price': record[5],
                    'low_price': record[6],
                    'trading_volume_share': record[7],
                    'trading_value_khr': record[8],
                    'market_cap': record[9],
                    'status': record[10],
                    'scraped_date': record[11].isoformat(),
                })

            return {
                'date': datetime.now().isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@ns_stock.route('/local_stock/historical_data/category')
class FilterResource(Resource):

    @ns_stock.doc(params={'category': 'The category to filter local stock'})
    def get(self):
        category = request.args.get('category')
        if category is None:
            return {
                'success': False,
                'message': 'Invalid or missing category'
            }, 400

        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM local_stock_historical_data where status = 'published' and category = %s",
                (category,))

            global_exchange = cursor.fetchall()
            if not global_exchange:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in global_exchange:
                response_data.append({
                    'date': record[0],
                    'close_price': record[1],
                    'change': record[2],
                    'change_status': record[3],
                    'trading_volume_shr': record[4],
                    'trading_volume_khr': record[5],
                    'opening': record[6],
                    'high': record[7],
                    'low': record[8],
                    'market_cap': record[9],
                    'full_market_cap': record[10],
                    'category': record[11],
                    'status': record[12]
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@ns_stock.route('/csx_current_index')
class ScrapeResource(Resource):

    def get(self):
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM stock_csx_current_index where status = 'published'")
            stock_csx_index = cursor.fetchall()
            if not stock_csx_index:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []

            # Iterate through the records and format them
            for record in stock_csx_index:
                response_data.append({
                    'id': record[0],
                    'current_index': record[1],
                    'change': record[2],
                    'change_status': record[3],
                    'change_per': record[4],
                    'change_per_status': record[5],
                    'opening_price': record[6],
                    'high_price': record[7],
                    'low_price': record[8],
                    'trading_volume_share': record[9],
                    'trading_value_khr': record[10],
                    'status': record[12],
                    'scraped_date': record[11].isoformat(),
                })

            return {
                'date': datetime.now().isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200

    @jwt_required(refresh=False)
    @ns_stock.doc(security="Bearer")
    @ns_stock.doc(params={'scraped_date': 'The date timestamp for the update without millisecond'})
    def put(self):
        scraped_date = request.args.get('scraped_date')

        try:
            datetime.strptime(scraped_date, '%Y-%m-%d %H:%M:%S')
        except ValueError:

            return {'success': False,
                    'message': 'Invalid scraped_date format, valid format is YYYY-MM-DD HH:MM:SS'}, 400

        # Validate the scraped_date parameter (you can add more validation)
        if scraped_date is None:
            return {'success': False, 'message': 'Missing scraped_date parameter'}, 400

        # Update the resource in the database using the scraped_date parameter

        cursor = conn.cursor()
        check_date_query = "SELECT status FROM stock_csx_current_index WHERE TO_CHAR" \
                           "(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = %s"
        cursor.execute(check_date_query, (scraped_date,))
        date_exists = cursor.fetchone()

        if not date_exists:
            return {'success': False, 'message': 'No data scraped in ' + scraped_date}, 404

        if date_exists and date_exists[0] == 'published':
            return {
                'success': False,
                'message': 'data is already updated'
            }, 400
        update_historical_query = """
                                    UPDATE stock_csx_current_index
                                    SET status = %s
                                    WHERE TO_CHAR(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = %s;
                                    """
        try:
            cursor.execute(update_historical_query, ('published', scraped_date))
            conn.commit()
            return {
                'success': True,
                'message': 'Updated successfully'
            }
        except Exception as e:
            conn.rollback()
            return {'success': False, 'message': 'Error updating resource: {}'.format(str(e))}, 400


@ns_stock.route('/csx_current_index/unpublished')
class unpublished(Resource):
    @jwt_required(refresh=False)
    @ns_stock.doc(security="Bearer")
    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                """
                    SELECT scraped_date FROM stock_csx_current_index where status = 'unpublished'
                    GROUP BY scraped_date
                    ORDER BY scraped_date DESC;
                """
            )
            stock_response = cursor.fetchall()
            if not stock_response:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in stock_response:
                response_data.append({
                    'category': 'Stocks',
                    'part': 'csx current index',
                    'description': 'Current Data',
                    'scraped_date': record[0].isoformat()
                })
            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200




@ns_stock.route('/trade_summary_all')
class ScrapeResource(Resource):

    def get(self):
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM stock_trade_summary_all where status = 'published'")
            stock_csx_index = cursor.fetchall()
            if not stock_csx_index:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []

            # Iterate through the records and format them
            for record in stock_csx_index:
                response_data.append({
                    'id': record[0],
                    'board': record[1],
                    'volume_share': record[2],
                    'value_khr': record[3],
                    'market_cap': record[4],
                    'full_market_cap': record[5],
                    'status': record[6],
                    'scraped_date': record[7].isoformat()
                })

            return {
                'date': datetime.now().isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200

    @jwt_required(refresh=False)
    @ns_stock.doc(security="Bearer")
    @ns_stock.doc(params={'scraped_date': 'The date timestamp for the update without millisecond'})
    def put(self):
        scraped_date = request.args.get('scraped_date')

        try:
            datetime.strptime(scraped_date, '%Y-%m-%d %H:%M:%S')
        except ValueError:

            return {'success': False,
                    'message': 'Invalid scraped_date format, valid format is YYYY-MM-DD HH:MM:SS'}, 400

        # Validate the scraped_date parameter (you can add more validation)
        if scraped_date is None:
            return {'success': False, 'message': 'Missing scraped_date parameter'}, 400

        # Update the resource in the database using the scraped_date parameter

        cursor = conn.cursor()
        check_date_query = "SELECT status FROM stock_trade_summary_all WHERE TO_CHAR" \
                           "(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = %s"
        cursor.execute(check_date_query, (scraped_date,))
        date_exists = cursor.fetchone()

        if not date_exists:
            return {'success': False, 'message': 'No data scraped in ' + scraped_date}, 404

        if date_exists and date_exists[0] == 'published':
            return {
                'success': False,
                'message': 'data is already updated'
            }, 400
        update_historical_query = """
                                    UPDATE stock_trade_summary_all
                                    SET status = %s
                                    WHERE TO_CHAR(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = %s;
                                    """
        try:
            cursor.execute(update_historical_query, ('published', scraped_date))
            conn.commit()
            return {
                'success': True,
                'message': 'Updated successfully'
            }
        except Exception as e:
            conn.rollback()
            return {'success': False, 'message': 'Error updating resource: {}'.format(str(e))}, 400


@ns_stock.route('/trade_summary_all/unpublished')
class unpublished(Resource):
    @jwt_required(refresh=False)
    @ns_stock.doc(security="Bearer")
    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                """
                    SELECT scraped_date FROM stock_trade_summary_all where status = 'unpublished'
                    GROUP BY scraped_date
                    ORDER BY scraped_date DESC;
                """
            )
            stock_response = cursor.fetchall()
            if not stock_response:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in stock_response:
                response_data.append({
                    'category': 'Stocks',
                    'part': 'local stock trade summary all',
                    'description': 'Current Data',
                    'scraped_date': record[0].isoformat()
                })
            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@ns_stock.route('/local_stock/historical_data/latest')
class FilterResource(Resource):

    @ns_stock.doc(params={'category': 'The category to filter local stock'})
    def get(self):
        category = request.args.get('category')
        if category is None:
            return {
                'success': False,
                'message': 'Invalid or missing category'
            }, 400

        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT * FROM local_stock_historical_data "
                "WHERE status = 'published' AND category = %s "
                "ORDER BY TO_DATE(date, 'DD/MM/YYYY') DESC "
                "LIMIT 1",
                (category,))

            global_exchange = cursor.fetchone()

            if global_exchange is None:
                return {
                    'success': True,
                    'message': "Data is empty"
                }, 200

            columns = [
                'date', 'close_price', 'change', 'change_status',
                'trading_volume_shr', 'trading_volume_khr', 'opening',
                'high', 'low', 'market_cap', 'full_market_cap',
                'category', 'status'
            ]

            response_data = {columns[i]: global_exchange[i] for i in range(len(columns))}

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'Get data successfully',
                'payload': response_data
            }, 200



