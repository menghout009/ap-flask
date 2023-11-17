from flask import Flask, request

from ..extensions import api
from flask_jwt_extended import jwt_required
from flask_restx import Namespace, Resource, reqparse
from scrapy.crawler import CrawlerRunner
from ..scrape.scrape.spiders.global_ExchangeRate import GlobalExchangeRate
from ..scrape.scrape.spiders.global_ExchangeRate_historical_data import GlobalExchangeRateHistorical
from twisted.internet import reactor
from concurrent.futures import ThreadPoolExecutor
from ..database.connect_db import conn
from ..scrape.scrape.spiders.local_Exchange import bank
from datetime import datetime
import pytz
import urllib.parse

phnom_penh = pytz.timezone('Asia/Phnom_Penh')
current_datetime = datetime.now(phnom_penh)
current_date_str = current_datetime.strftime("%Y-%m-%d")

exchange_ns = Namespace("api/v1/exchange_rate")


@exchange_ns.route('/global')
class ExchangeRate(Resource):

    @jwt_required(refresh=False)
    @exchange_ns.doc(security="Bearer")
    def post(self):
        try:
            # Create a CrawlerRunner with project settings
            runner = CrawlerRunner()

            # Add your Scrapy spider to the runner
            runner.crawl(GlobalExchangeRate)

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
            return {
                'success': False,
                'error': str(e)
            }, 404

    @jwt_required(refresh=False)
    @exchange_ns.doc(security="Bearer")
    @exchange_ns.doc(params={'scraped_date': 'The date timestamp for the update without millisecond'})
    def put(self):
        scraped_date = request.args.get('scraped_date')
        try:
            datetime.strptime(scraped_date, '%Y-%m-%d %H:%M:%S')
        except ValueError:

            return {
                'success': False,
                'message': 'Invalid scraped_date format, valid format is YYYY-MM-DD HH:MM:SS'
            }, 400

        # Validate the scraped_date parameter (you can add more validation)
        if scraped_date is None:
            return {
                'success': False,
                'message': 'Invalid or missing scraped_date parameter'
            }, 400

        # Update the resource in the database using the scraped_date parameter

        cursor = conn.cursor()
        check_date_query = "SELECT status FROM global_exchange_rate WHERE TO_CHAR(scraped_date, 'YYYY-MM-DD " \
                           "HH24:MI:SS')" \
                           " = %s"
        cursor.execute(check_date_query, (scraped_date,))
        date_exists = cursor.fetchone()
        if date_exists and date_exists[0] == 'published':
            return {
                'success': False,
                'message': 'data is already updated'
            }, 400
        if not date_exists:
            return {
                'success': False,
                'message': 'No data scraped in ' + scraped_date
            }, 404

        # Update the global_exchange_rate table using SQL with the scraped_date parameter
        update_query = """
            UPDATE global_exchange_rate
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
                "SELECT * FROM global_exchange_rate")
            global_exchange_rate = cursor.fetchall()
            if not global_exchange_rate:
                return {
                    'success': True,
                    "response: ": "data is empty"
                }, 200
            response_data = []
            for record in global_exchange_rate:
                response_data.append({
                    'id': record[0],
                    'event': record[1],
                    'currency_from': record[2],
                    'currency_to': record[3],
                    'currency_code_from': record[4],
                    'currency_code_to': record[5],
                    'buying': record[6],
                    'sale': record[7],
                    'high_price': record[8],
                    'low_price': record[9],
                    'variance': record[10],
                    'variance_per': record[11],
                    'time': record[12],
                    'status': record[13],
                    'scraped_date': record[14].isoformat()
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@exchange_ns.route('/global/historical_data')
class ExchangeRate(Resource):
    @jwt_required(refresh=False)
    @exchange_ns.doc(security="Bearer")
    def post(self):
        try:
            # Create a CrawlerRunner with project settings
            runner = CrawlerRunner()

            # Add your Scrapy spider to the runner
            runner.crawl(GlobalExchangeRateHistorical)

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
    @exchange_ns.doc(security="Bearer")
    @exchange_ns.doc(params={'scraped_date': 'The date timestamp for the update without millisecond'})
    def put(self):
        scraped_date = request.args.get('scraped_date')
        try:
            datetime.strptime(scraped_date, '%Y-%m-%d %H:%M:%S')
        except ValueError:

            return {
                'success': False,
                'message': 'Invalid scraped_date format, valid format is YYYY-MM-DD HH:MM:SS'
            }, 400

        # Validate the scraped_date parameter (you can add more validation)
        if scraped_date is None:
            return {
                'success': False,
                'message': 'Invalid or missing scraped_date parameter'
            }, 400

        # Update the resource in the database using the scraped_date parameter

        cursor = conn.cursor()
        check_date_query = "SELECT status FROM global_exchange_rate_historical_data WHERE TO_CHAR" \
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

        update_historical_query = """
                        UPDATE global_exchange_rate_historical_data
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
            return {
                'success': False,
                'message': 'Error updating resource: {}'.format(str(e))
            }, 400

    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM global_exchange_rate_historical_data")
            global_exchange_rate_historical_data = cursor.fetchall()
            if not global_exchange_rate_historical_data:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in global_exchange_rate_historical_data:
                response_data.append({
                    'id': record[0],
                    'date': record[1],
                    'closing_price': record[2],
                    'market_price': record[3],
                    'high_price': record[4],
                    'low_price': record[5],
                    'variance_per': record[6],
                    'category': record[7],
                    'date_scraped': record[8].isoformat(),
                    'status': record[9]
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@exchange_ns.route('/global/historical_data/published')
class ExchangeRate(Resource):
    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM global_exchange_rate_historical_data where status = 'published'")
            global_exchange_rate_historical_data = cursor.fetchall()
            if not global_exchange_rate_historical_data:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in global_exchange_rate_historical_data:
                response_data.append({
                    'id': record[0],
                    'date': record[1],
                    'closing_price': record[2],
                    'market_price': record[3],
                    'high_price': record[4],
                    'low_price': record[5],
                    'variance_per': record[6],
                    'category': record[7],
                    'date_scraped': record[8].isoformat(),
                    'status': record[9]
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@exchange_ns.route('/global/published')
class ExchangeRate(Resource):
    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM global_exchange_rate where status = 'published'")
            global_exchange_rate = cursor.fetchall()
            if not global_exchange_rate:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in global_exchange_rate:
                response_data.append({
                    'id': record[0],
                    'event': record[1],
                    'currency_from': record[2],
                    'currency_to': record[3],
                    'currency_code_from': record[4],
                    'currency_code_to': record[5],
                    'buying': record[6],
                    'sale': record[7],
                    'high_price': record[8],
                    'low_price': record[9],
                    'variance': record[10],
                    'variance_per': record[11],
                    'time': record[12],
                    'status': record[13],
                    'scraped_date': record[14].isoformat()
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@exchange_ns.route('/local/published')
class ExchangeRate(Resource):

    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM local_exchange where scraped_date = (SELECT MAX(scraped_date) FROM local_exchange where status = 'published')")
            local_exchange = cursor.fetchall()
            if not local_exchange:
                return {
                    "success": True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in local_exchange:
                response_data.append({
                    'id': record[0],
                    'currency': record[1],
                    'currency_from': record[2],
                    'currency_to': record[3],
                    'unit': record[4],
                    'buying': record[5],
                    'sale': record[6],
                    'medium': record[7],
                    'currency_date': record[8],
                    'scraped_date': record[9].isoformat(),
                    'status': record[10],
                    'current_to_name': record[11]
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@exchange_ns.route('/local/historical_data/published')
class ExchangeRate(Resource):

    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM local_exchange where status = 'published'")
            local_exchange = cursor.fetchall()
            if not local_exchange:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in local_exchange:
                response_data.append({
                    'id': record[0],
                    'currency': record[1],
                    'currency_from': record[2],
                    'currency_to': record[3],
                    'unit': record[4],
                    'buying': record[5],
                    'sale': record[6],
                    'medium': record[7],
                    'currency_date': record[8],
                    'scraped_date': record[9].isoformat(),
                    'status': record[10],
                    'current_to_name': record[11]
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@exchange_ns.route('/global/both')
class ExchangeRate(Resource):
    @jwt_required(refresh=False)
    @exchange_ns.doc(security="Bearer")
    def post(self):
        scrape_number = 0
        try:

            runner = CrawlerRunner()

            # Add your Scrapy spider to the runner
            history = runner.crawl(GlobalExchangeRateHistorical)
            normal = runner.crawl(GlobalExchangeRate)

            def run_reactor():
                reactor.run(installSignalHandlers=False)

            thread_pool = ThreadPoolExecutor(max_workers=1)
            thread_pool.submit(run_reactor)

            # Return a response immediately
            cursor = conn.cursor()
            scrape_number_query = "SELECT * FROM count_scrape_exchange_global(1)"
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


@exchange_ns.route('/local')
class ExchangeRate(Resource):
    @jwt_required(refresh=False)
    @exchange_ns.doc(security="Bearer")
    def post(self):
        scrape_number = 0
        try:

            runner = CrawlerRunner()

            # Add your Scrapy spider to the runner
            runner.crawl(bank)

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
            return {
                'success': False,
                'error': str(e)
            }, 404


# unpublished local

@exchange_ns.route('/local/unpublished')
class unpublished(Resource):
    @jwt_required(refresh=False)
    @exchange_ns.doc(security="Bearer")
    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                """
                    SELECT scraped_date FROM local_exchange where status = 'unpublished'
                    GROUP BY scraped_date
                    ORDER BY scraped_date DESC;
                """
            )
            exchange_response = cursor.fetchall()
            if not exchange_response:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in exchange_response:
                response_data.append({
                    'category': 'Exchange Rate',
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


parser = reqparse.RequestParser()
parser.add_argument('from', type=str, help='Input currency from (e.g., USD)')
parser.add_argument('to', type=str, help='Input currency to (e.g., KHR)')
parser.add_argument('amount', type=float, help='Amount to calculate')


@exchange_ns.route('/calculator/exchange/rate')
class CalculateExchangeRate(Resource):
    # @exchange_ns.doc(params={
    #     'from': 'Input currency from (e.g., USD)',
    #     'to': 'Input currency to (e.g., KHR)',
    #     'amount': 'Amount to calculate'
    # })
    @api.expect(parser)
    def post(self):
        # currency_from = request.args.get('from').upper()
        # currency_to = request.args.get('to').upper()
        # amount = float(request.args.get('amount'))
        args = parser.parse_args()
        print(current_date_str)
        # Check if 'from' parameter is missing or empty
        if args['from'] is None:
            return {
                "success": False,
                "message": "Missing or empty 'from' parameter"
            }, 400

        # Check if 'to' parameter is missing or empty
        if args['to'] is None:
            return {
                "success": False,
                "message": "Missing or empty 'to' parameter"
            }, 400

        # Check if 'amount' parameter is missing or empty
        if args['amount'] is None:
            return {
                "success": False,
                "message": "Missing or empty 'amount' parameter"
            }, 400

        currency_from = args['from'].upper()
        currency_to = args['to'].upper()
        amount = args['amount']

        try:
            amount = float(amount)
        except ValueError:
            return {
                "success": False,
                "message": "Invalid 'amount' parameter. It should be a number."
            }, 400

        with conn.cursor() as cursor:
            if currency_from == 'KHR':
                cursor.execute(
                    "SELECT buying FROM local_exchange WHERE currency_from = %s and currency_to = %s and currency_date = %s",
                    (currency_to, currency_from, current_date_str)
                )
            if currency_from != 'KHR':
                cursor.execute(
                    "SELECT buying FROM local_exchange WHERE currency_from = %s and currency_to = %s and currency_date = %s",
                    (currency_from, currency_to, current_date_str)
                )

            result = cursor.fetchone()

            if not result:
                cursor.execute(
                    "SELECT buy FROM global_exchange_rate WHERE currency_code_from = %s and currency_code_to = %s and TO_CHAR(scraped_date, 'YYYY-MM-DD') = %s",
                    (currency_from, currency_to, current_date_str)
                )
                result = cursor.fetchone()

            if not result:
                return {
                    "success": False,
                    "message": "no data in table"
                }, 404

            exchange_rate = float(result[0])

            if currency_from == 'KHR':
                # If 'KHR' is the source currency, divide the amount by the exchange rate
                converted_amount = amount / exchange_rate
            else:
                # Otherwise, multiply the amount by the exchange rate
                converted_amount = amount * exchange_rate
            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'calculate successfully',
                'payload': converted_amount
            }, 200


# unpublished global
@exchange_ns.route('/global/unpublished')
class unpublished(Resource):
    @jwt_required(refresh=False)
    @exchange_ns.doc(security="Bearer")
    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                """
                    SELECT scraped_date FROM global_exchange_rate where status = 'unpublished'
                    GROUP BY scraped_date
                    ORDER BY scraped_date DESC;
                """
            )
            exchange_response = cursor.fetchall()
            if not exchange_response:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in exchange_response:
                response_data.append({
                    'category': 'Exchange Rate',
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


# unpublished global historical
@exchange_ns.route('/global/historical_data/unpublished')
class unpublished(Resource):
    @jwt_required(refresh=False)
    @exchange_ns.doc(security="Bearer")
    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                """
                    SELECT scraped_date FROM global_exchange_rate_historical_data where status = 'unpublished'
                    GROUP BY scraped_date
                    ORDER BY scraped_date DESC;
                """
            )
            exchange_response = cursor.fetchall()
            if not exchange_response:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in exchange_response:
                response_data.append({
                    'category': 'Exchange Rate',
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


@exchange_ns.route('/local')
class ExchangeRate(Resource):
    @jwt_required(refresh=False)
    @exchange_ns.doc(security="Bearer")
    def post(self):

        scrape_number = 0
        try:

            runner = CrawlerRunner()

            # Add your Scrapy spider to the runner
            runner.crawl(bank)

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
    @exchange_ns.doc(security="Bearer")
    @exchange_ns.doc(params={'scraped_date': 'The date timestamp for the update without millisecond'})
    def put(self):
        scraped_date = request.args.get('scraped_date')
        try:
            datetime.strptime(scraped_date, '%Y-%m-%d %H:%M:%S')
        except ValueError:

            return {'message': 'Invalid scraped_date format, valid format is YYYY-MM-DD HH:MM:SS'}, 400

        # Validate the scraped_date parameter (you can add more validation)
        if scraped_date is None:
            return {'message': 'Invalid or missing scraped_date parameter'}, 400

        # Update the resource in the database using the scraped_date parameter

        cursor = conn.cursor()
        check_date_query = "SELECT status FROM local_exchange WHERE TO_CHAR" \
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

        update_historical_query = """
                            UPDATE local_exchange
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
            return {
                'success': False,
                'message': 'Error updating resource: {}'.format(str(e))
            }, 400

    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM local_exchange")
            local_exchange = cursor.fetchall()
            if not local_exchange:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in local_exchange:
                response_data.append({
                    'id': record[0],
                    'currency': record[1],
                    'currency_from': record[2],
                    'currency_to': record[3],
                    'unit': record[4],
                    'buying': record[5],
                    'sale': record[6],
                    'medium': record[7],
                    'currency_date': record[8],
                    'scraped_date': record[9].isoformat(),
                    'status': record[10],
                    'current_to_name': record[11]
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@exchange_ns.route('/local/detail/<currency>')
class Resource(Resource):
    @exchange_ns.doc(params={'currency': 'The currency to filter local category'})
    def get(self, currency):

        if currency is None:
            return {
                'success': False,
                'message': 'Invalid or missing category'}, 400

        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM local_exchange where status = 'published' and currency = %s", (currency,))

            local_exchange = cursor.fetchall()
            if not local_exchange:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in local_exchange:
                response_data.append({
                    'id': record[0],
                    'currency': record[1],
                    'currency_from': record[2],
                    'currency_to': record[3],
                    'unit': record[4],
                    'buying': record[5],
                    'sale': record[6],
                    'medium': record[7],
                    'currency_date': record[8],
                    'scraped_date': record[9].isoformat(),
                    'status': record[10],
                    'current_to_name': record[11]
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@exchange_ns.route('/global/historical_data/category')
class FilterResource(Resource):
    @exchange_ns.doc(params={'category': 'The category to filter global exchange rate'})
    def get(self):
        category = request.args.get('category')
        print("skdfjaskdf", category)
        if category is None:
            return {
                'success': False,
                'message': 'Invalid or missing category'
            }, 400

        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM global_exchange_rate_historical_data where status = 'published' and category = %s",
                (category,))

            global_exchange = cursor.fetchall()
            if not global_exchange:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in global_exchange:
                response_data.append({
                    'id': record[0],
                    'date': record[1],
                    'closing_price': record[2],
                    'market_price': record[3],
                    'high_price': record[4],
                    'low_price': record[5],
                    'variance_per': record[6],
                    'category': record[7],
                    'date_scraped': record[8].isoformat(),
                    'status': record[9]
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@exchange_ns.route('/local/getById/<int:exchange_id>')
class ExchangeRateById(Resource):

    def get(self, exchange_id):
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT * FROM local_exchange WHERE status = 'published' and id = %s", (exchange_id,))
            exchange_data = cursor.fetchone()
            if not exchange_data:
                return {
                    'success': False,
                    "message": f"No data found for exchange Id {exchange_id}"}, 404

            response_data = {
                'id': exchange_data[0],
                'currency': exchange_data[1],
                'currency_from': exchange_data[2],
                'currency_to': exchange_data[3],
                'unit': exchange_data[4],
                'buying': exchange_data[5],
                'sale': exchange_data[6],
                'medium': exchange_data[7],
                'currency_date': exchange_data[8],
                'scraped_date': exchange_data[9].isoformat(),
                'status': exchange_data[10],
                'current_to_name': exchange_data[11]
            }

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'payload': response_data
            }, 200


@exchange_ns.route('/global/getById/<int:exchange_id>')
class GlobalExchangeRateById(Resource):

    def get(self, exchange_id):
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT * FROM global_exchange_rate WHERE status = 'published' and id = %s", (exchange_id,))
            exchange_data = cursor.fetchone()
            if not exchange_data:
                return {"message": f"No data found for exchange ID {exchange_id}"}, 404

            response_data = {
                'id': exchange_data[0],
                'event': exchange_data[1],
                'currency_from': exchange_data[2],
                'currency_to': exchange_data[3],
                'currency_code_from': exchange_data[4],
                'currency_code_to': exchange_data[5],
                'buying': exchange_data[6],
                'sale': exchange_data[7],
                'high_price': exchange_data[8],
                'low_price': exchange_data[9],
                'variance': exchange_data[10],
                'variance_per': exchange_data[11],
                'time': exchange_data[12],
                'status': exchange_data[13],
                'scraped_date': exchange_data[14].isoformat()
            }

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'payload': response_data
            }, 200
