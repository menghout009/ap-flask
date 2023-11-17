from flask_jwt_extended import jwt_required
from flask_restx import Namespace, Resource
from scrapy.crawler import CrawlerRunner
from ..scrape.scrape.spiders.cambodia_gdp import CambodiaGDPSpider
from twisted.internet import reactor
from concurrent.futures import ThreadPoolExecutor
from ..database.connect_db import conn
from datetime import datetime
from flask import request
import pytz

# ______  100% all api/v1/gdp ______
gdp_ns = Namespace("api/v1/gdp")

phnom_penh = pytz.timezone('Asia/Phnom_Penh')
current_datetime = datetime.now(phnom_penh)


@gdp_ns.route('/cambodia')
class gdp(Resource):

    @jwt_required(refresh=False)
    @gdp_ns.doc(security="Bearer")
    def post(self):
        try:
            # Create a CrawlerRunner with project settings
            runner = CrawlerRunner()

            # Add your Scrapy spider to the runner
            runner.crawl(CambodiaGDPSpider)

            # Create a thread pool to run the reactor in
            def run_reactor():
                reactor.run(installSignalHandlers=False)

            thread_pool = ThreadPoolExecutor(max_workers=1)
            thread_pool.submit(run_reactor)

            # Return a response immediately
            cursor = conn.cursor()
            scrape_number_query = "SELECT * FROM count_scrape_gdp(1)"
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
    @gdp_ns.doc(security="Bearer")
    @gdp_ns.doc(params={'scraped_date': 'The date timestamp for the update without millisecond'})
    def put(self):
        scraped_date = request.args.get('scraped_date')

        try:
            datetime.strptime(scraped_date, '%Y-%m-%d %H:%M:%S')
        except ValueError:

            return {'message': 'Invalid scraped_date format, valid format is YYYY-MM-DD HH:MM:SS'}, 400
        # Validate the scraped_date parameter (you can add more validation)
        if scraped_date is None:
            return {
                'success': False,
                'message': 'Invalid or missing scraped_date parameter'
            }, 400

        # Update the resource in the database using the scraped_date parameter

        cursor = conn.cursor()
        check_date_query = "SELECT status FROM cambodia_gdp WHERE TO_CHAR(scraped_date, 'YYYY-MM-DD HH24:MI:SS')" \
                           " = %s"
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
                'message': 'data is already update'
            }, 400

        # Update the global_exchange_rate table using SQL with the scraped_date parameter
        update_query = """
                UPDATE cambodia_gdp
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
                "SELECT * FROM cambodia_gdp")
            cambodia_gdp_response = cursor.fetchall()
            if not cambodia_gdp_response:
                return {
                    'success': True,
                    "message: ": "data is empty"
                }, 200
            response_data = []
            for record in cambodia_gdp_response:
                response_data.append({
                    'id': record[0],
                    'year': record[1],
                    'gdp_nominal': float(record[2]),
                    'gdp_real': record[3],
                    'gdp_change': record[4],
                    'gdp_per_capita': record[5],
                    'population_change': record[6],
                    'population': record[7],
                    'status': record[8],
                    'scraped_date': record[9].isoformat()
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@gdp_ns.route('/cambodia/published')
class gdp(Resource):

    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM cambodia_gdp where status = 'published'")
            cambodia_gdp_response = cursor.fetchall()
            if not cambodia_gdp_response:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in cambodia_gdp_response:
                response_data.append({
                    'id': record[0],
                    'year': record[1],
                    'gdp_nominal': float(record[2]),
                    'gdp_real': record[3],
                    'gdp_change': record[4],
                    'gdp_per_capita': record[5],
                    'population_change': record[6],
                    'population': record[7],
                    'status': record[8],
                    'scraped_date': record[9].isoformat()
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@gdp_ns.route('/cambodia/unpublished')
class unpublished(Resource):
    @jwt_required(refresh=False)
    @gdp_ns.doc(security="Bearer")
    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                """
                    SELECT scraped_date FROM cambodia_gdp where status = 'unpublished'
                    GROUP BY scraped_date
                    ORDER BY scraped_date DESC
                """
            )
            cambodia_gdp_response = cursor.fetchall()
            if not cambodia_gdp_response:
                return {
                    'success': True,
                    "message: ": "data is empty"
                }, 200
            response_data = []
            for record in cambodia_gdp_response:
                response_data.append({
                    'category': 'GDP',
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
