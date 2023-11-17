from flask import request

from ..scrape.scrape.spiders.global_bond import GlobalBondSpider
from flask_restx import Namespace, Resource
from twisted.internet import reactor
from scrapydo import setup
from scrapy.crawler import CrawlerRunner
from concurrent.futures import ThreadPoolExecutor
from ..database.connect_db import conn
from datetime import datetime
import pytz
from flask_jwt_extended import jwt_required, get_jwt_identity

setup()
ns_bond = Namespace("api/v1/bond")

phnom_penh = pytz.timezone('Asia/Phnom_Penh')
current_datetime = datetime.now(phnom_penh)


@ns_bond.route('')
class ScrapeResource(Resource):
    # method_decorators = [jwt_required()]
    @jwt_required(refresh=False)
    @ns_bond.doc(security="Bearer")
    def post(self):
        scrape_number = 0
        try:
            # Create a CrawlerRunner with project settings
            runner = CrawlerRunner()

            # Add your Scrapy spider to the runner
            runner.crawl(GlobalBondSpider)

            # Create a thread pool to run the reactor in
            def run_reactor():
                reactor.run(installSignalHandlers=False)

            thread_pool = ThreadPoolExecutor(max_workers=1)
            thread_pool.submit(run_reactor)

            # Return a response immediately
            cursor = conn.cursor()
            scrape_number_query = "SELECT * FROM count_scrape_bond(1)"
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

    @jwt_required(refresh=False)
    @ns_bond.doc(security="Bearer")
    @ns_bond.doc(params={'scraped_date': 'The date timestamp for the update without millisecond'})
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
        check_date_query = "SELECT status FROM global_bond WHERE TO_CHAR" \
                           "(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = %s"
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

        update_query = """
                        UPDATE global_bond
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
            return {
                'success': False,
                'message': 'Error updating resource: {}'.format(str(e))
            }, 400

    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM global_bond")
            bond = cursor.fetchall()
            if not bond:
                return {
                    'success': True,
                    "message: ": "data is empty"
                }, 200
            response_data = []
            for record in bond:
                response_data.append({
                    'event': record[0],
                    'bond_yield': record[1],
                    'before': record[2],
                    'high_price': record[3],
                    'low_price': record[4],
                    'variance': record[5],
                    'variance_percentage': record[6],
                    'hour': record[7],
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


@ns_bond.route('/published')
class Bond(Resource):

    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM global_bond where status = 'published'")
            bond = cursor.fetchall()
            if not bond:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in bond:
                response_data.append({
                    'event': record[0],
                    'bond_yield': record[1],
                    'before': record[2],
                    'high_price': record[3],
                    'low_price': record[4],
                    'variance': record[5],
                    'variance_percentage': record[6],
                    'hour': record[7],
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


# unpublished

@ns_bond.route('/unpublished')
class unpublished(Resource):
    @jwt_required(refresh=False)
    @ns_bond.doc(security="Bearer")
    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                """
                    SELECT scraped_date FROM global_bond where status = 'unpublished'
                    GROUP BY scraped_date
                    ORDER BY scraped_date DESC;
                """
            )
            bond_response = cursor.fetchall()
            if not bond_response:
                return {
                    'success': True,
                    "message: ": "data is empty"
                }, 200
            response_data = []
            for record in bond_response:
                response_data.append({
                    'category': 'Bonds',
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


@ns_bond.route('/<category>')
class ScrapeResource(Resource):
    @ns_bond.doc(params={'category': 'The category to filter bond'})
    def get(self, category):
        # category = request.args.get('category')
        print("*** category ", category)
        if category is None:
            return {
                'success': False,
                'message': 'Invalid or missing category'
            }, 400

        if category.lower() not in ["north", "asia", "european"]:
            return {
                'success': False,
                'message': 'Category is either north, asia, currencies, european'
            }, 400

        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM global_bond where status = 'published' and category = %s", (category.lower(),))
            bond = cursor.fetchall()
            if not bond:
                return {
                    'success': True,
                    "message: ": "data is empty"
                }, 200
            response_data = []
            for record in bond:
                response_data.append({
                    'event': record[0],
                    'bond_yield': record[1],
                    'before': record[2],
                    'high_price': record[3],
                    'low_price': record[4],
                    'variance': record[5],
                    'variance_percentage': record[6],
                    'hour': record[7],
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
