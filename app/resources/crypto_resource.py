from flask import request
from flask_jwt_extended import jwt_required
from flask_restx import Namespace, Resource
from scrapy.crawler import CrawlerRunner
from ..scrape.scrape.spiders.crypto_historical_date import CryptoHistoricalSpider
from ..scrape.scrape.spiders.crypto_data import CryptoSpider
from twisted.internet import reactor
from concurrent.futures import ThreadPoolExecutor
from ..database.connect_db import conn
import datetime
import pytz
from datetime import datetime as time

crypto_ns = Namespace("api/v1/crypto")

phnom_penh = pytz.timezone('Asia/Phnom_Penh')
current_datetime = datetime.datetime.now(phnom_penh)


@crypto_ns.route('')
class gdp(Resource):

    @jwt_required(refresh=False)
    @crypto_ns.doc(security="Bearer")
    def post(self):
        try:
            # Create a CrawlerRunner with project settings
            runner = CrawlerRunner()

            # Add your Scrapy spider to the runner
            runner.crawl(CryptoSpider)

            # Create a thread pool to run the reactor in
            def run_reactor():
                reactor.run(installSignalHandlers=False)

            thread_pool = ThreadPoolExecutor(max_workers=1)
            thread_pool.submit(run_reactor)

            # Return a response immediately
            cursor = conn.cursor()
            scrape_number_query = "SELECT * FROM count_scrape_exchange_local(1)"
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
            return {'error': str(e)}, 404

    @jwt_required(refresh=False)
    @crypto_ns.doc(security="Bearer")
    @crypto_ns.doc(params={'scraped_date': 'The date timestamp for the update without millisecond'})
    def put(self):
        scraped_date = request.args.get('scraped_date')
        try:
            time.strptime(scraped_date, '%Y-%m-%d %H:%M:%S')
        except ValueError:

            return {'message': 'Invalid scraped_date format, valid format is YYYY-MM-DD HH:MM:SS'}, 400

        # Validate the scraped_date parameter (you can add more validation)
        if scraped_date is None:
            return {'message': 'Invalid or missing scraped_date parameter'}, 400

        # Update the resource in the database using the scraped_date parameter

        cursor = conn.cursor()
        check_date_query = "SELECT status FROM crypto WHERE TO_CHAR" \
                           "(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = %s"
        cursor.execute(check_date_query, (scraped_date,))
        date_exists = cursor.fetchone()

        if not date_exists:
            return {'message': 'No data scraped in ' + scraped_date}, 404

        if date_exists and date_exists[0] == 'published':
            return {
                'success': False,
                'message': 'data is already updated'
            }, 400

        update_query = """
                            UPDATE crypto
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
            return {'message': 'Error updating resource: {}'.format(str(e))}, 400

    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM crypto")
            crypto_historical_response = cursor.fetchall()
            if not crypto_historical_response:
                return {
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in crypto_historical_response:
                response_data.append({
                    'id': record[0],
                    'no': record[1],
                    'name': record[2],
                    'symbol': record[3],
                    'price': record[4],
                    'market_cap': record[5],
                    'volume': record[6],
                    'total_volume': record[7],
                    'change_24h': record[8],
                    'change_7d': record[9],
                    'scraped_date': record[10].isoformat(),
                    'status': record[11]
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@crypto_ns.route('/published')
class gdp(Resource):

    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM crypto where status = 'published'")
            crypto_historical_response = cursor.fetchall()
            if not crypto_historical_response:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in crypto_historical_response:
                response_data.append({
                    'id': record[0],
                    'no': record[1],
                    'name': record[2],
                    'symbol': record[3],
                    'price': record[4],
                    'market_cap': record[5],
                    'volume': record[6],
                    'total_volume': record[7],
                    'change_24h': record[8],
                    'change_7d': record[9],
                    'scraped_date': record[10].isoformat(),
                    'status': record[11]
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@crypto_ns.route('/published/getById/<int:crypto_id>')
class CryptoByID(Resource):

    def get(self, crypto_id):
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM crypto WHERE status = 'published' and id = %s", (crypto_id,))
            crypto_data = cursor.fetchone()
            if not crypto_data:
                return {
                    'success': False,
                    "message": f"No data found for crypto ID {crypto_id}"
                }, 404

            response_data = {
                'id': crypto_data[0],
                'no': crypto_data[1],
                'name': crypto_data[2],
                'symbol': crypto_data[3],
                'price': crypto_data[4],
                'market_cap': crypto_data[5],
                'volume': crypto_data[6],
                'total_volume': crypto_data[7],
                'change_24h': crypto_data[8],
                'change_7d': crypto_data[9],
                'scraped_date': crypto_data[10].isoformat(),
                'status': crypto_data[11]
            }

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'payload': response_data
            }, 200


@crypto_ns.route('/historical_data')
class gdp(Resource):

    @jwt_required(refresh=False)
    @crypto_ns.doc(security="Bearer")
    def post(self):
        try:
            # Create a CrawlerRunner with project settings
            runner = CrawlerRunner()

            # Add your Scrapy spider to the runner
            runner.crawl(CryptoHistoricalSpider)

            # Create a thread pool to run the reactor in
            def run_reactor():
                reactor.run(installSignalHandlers=False)

            thread_pool = ThreadPoolExecutor(max_workers=1)
            thread_pool.submit(run_reactor)

            # Return a response immediately
            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'scraped successfully'
            }, 200
        except Exception as e:
            return {'error': str(e)}, 404

    @jwt_required(refresh=False)
    @crypto_ns.doc(security="Bearer")
    @crypto_ns.doc(params={'scraped_date': 'The date timestamp for the update without millisecond'})
    def put(self):
        scraped_date = request.args.get('scraped_date')

        try:
            time.strptime(scraped_date, '%Y-%m-%d %H:%M:%S')
        except ValueError:

            return {'message': 'Invalid scraped_date format, valid format is YYYY-MM-DD HH:MM:SS'}, 400

        # Validate the scraped_date parameter (you can add more validation)
        if scraped_date is None:
            return {'message': 'Invalid or missing scraped_date parameter'}, 400

        # Update the resource in the database using the scraped_date parameter

        cursor = conn.cursor()
        check_date_query = "SELECT status FROM crypto_historical WHERE TO_CHAR" \
                           "(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = %s"
        cursor.execute(check_date_query, (scraped_date,))
        date_exists = cursor.fetchone()

        if not date_exists:
            return {
                'success': False,
                'message': 'No data scraped in ' + scraped_date
            }, 404

        if date_exists and date_exists[0] == 'published':
            return {
                'success': False,
                'message': 'data is already updated'
            }, 400

        update_query = """
                        UPDATE crypto_historical
                        SET status = %s
                        WHERE TO_CHAR(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = %s;
                        """
        try:
            cursor.execute(update_query, ('published', scraped_date))
            conn.commit()
            return {
                'success': True,
                'message': 'Updated successfully'
            }, 200
        except Exception as e:
            conn.rollback()
            return {
                'success': False,
                'message': 'Error updating resource: {}'.format(str(e))
            }, 400

    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM crypto_historical")
            crypto_historical_response = cursor.fetchall()
            if not crypto_historical_response:
                return {
                    'success': True,
                    "message: ": "data is empty"
                }, 200
            response_data = []
            for record in crypto_historical_response:
                response_data.append({
                    'id': record[0],
                    'date': record[1],
                    'price': record[2],
                    'open_price': record[3],
                    'high_price': record[4],
                    'low_price': record[5],
                    'volume': record[6],
                    'change': record[7],
                    'category': record[8],
                    'status': record[9],
                    'scraped_date': record[10].isoformat()
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@crypto_ns.route('/historical_data/published')
class gdp(Resource):

    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM crypto_historical where status = 'published'")
            crypto_historical_response = cursor.fetchall()
            if not crypto_historical_response:
                return {
                    'success': True,
                    "message: ": "data is empty"
                }, 200
            response_data = []
            for record in crypto_historical_response:
                response_data.append({
                    'id': record[0],
                    'date': record[1],
                    'price': record[2],
                    'open_price': record[3],
                    'high_price': record[4],
                    'low_price': record[5],
                    'volume': record[6],
                    'change': record[7],
                    'category': record[8],
                    'status': record[9],
                    'scraped_date': record[10].isoformat()
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@crypto_ns.route('/both')
class gdp(Resource):

    @jwt_required(refresh=False)
    @crypto_ns.doc(security="Bearer")
    def post(self):
        scrape_number = 0
        try:
            # Create a CrawlerRunner with project settings
            runner = CrawlerRunner()

            # Add your Scrapy spider to the runner
            runner.crawl(CryptoHistoricalSpider)
            runner.crawl(CryptoSpider)

            # Create a thread pool to run the reactor in
            def run_reactor():
                reactor.run(installSignalHandlers=False)

            thread_pool = ThreadPoolExecutor(max_workers=1)
            thread_pool.submit(run_reactor)

            # Return a response immediately
            cursor = conn.cursor()
            scrape_number_query = "SELECT * FROM count_scrape_crypto(1)"
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
            return {
                'success': False,
                'error': str(e)
            }, 404


# unpublished global

@crypto_ns.route('/unpublished')
class unpublished(Resource):
    @jwt_required(refresh=False)
    @crypto_ns.doc(security="Bearer")
    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                """
                    SELECT scraped_date FROM crypto where status = 'unpublished'
                    GROUP BY scraped_date
                    ORDER BY scraped_date DESC;
                """
            )
            crypto_response = cursor.fetchall()
            if not crypto_response:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in crypto_response:
                response_data.append({
                    'category': 'Cryptocurrency',
                    'part': 'Global',
                    'description': 'Current Data',
                    'scraped_date': record[0].isoformat()
                })
            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


# unpublished global historical_data

@crypto_ns.route('/historical_data/unpublished')
class unpublished(Resource):
    @jwt_required(refresh=False)
    @crypto_ns.doc(security="Bearer")
    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                """
                    SELECT scraped_date FROM crypto_historical where status = 'unpublished'
                    GROUP BY scraped_date
                    ORDER BY scraped_date DESC;
                """
            )
            crypto_response = cursor.fetchall()
            if not crypto_response:
                return {
                    'success': True,
                    "message: ": "data is empty"
                }, 200
            response_data = []
            for record in crypto_response:
                response_data.append({
                    'category': 'Cryptocurrency',
                    'part': 'Global',
                    'description': 'Historical Data',
                    'scraped_date': record[0].isoformat()
                })
            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@crypto_ns.route('/crypto/historical_data/<category>')
class Resource(Resource):
    @crypto_ns.doc(params={'category': 'category to filter crypto'})
    def get(self, category):

        if category is None:
            return {
                'success': False,
                'message': 'Invalid or missing category'}, 400

        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM crypto_historical where status = 'published' and category = %s", (category,))

            local_exchange = cursor.fetchall()
            if not local_exchange:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in local_exchange:
                response_data.append({
                    'id': record[0],
                    'date': record[1],
                    'price': record[2],
                    'open_price': record[3],
                    'high_price': record[4],
                    'low_price': record[5],
                    'volume': record[6],
                    'change': record[7],
                    'category': record[8],
                    'status': record[9],
                    'scraped_date': record[10].isoformat()
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200
