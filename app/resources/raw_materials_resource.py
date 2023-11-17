from flask import request
from flask_restx import Namespace, Resource
from twisted.internet import reactor
from scrapydo import setup
from scrapy.crawler import CrawlerRunner
from concurrent.futures import ThreadPoolExecutor
from flask_jwt_extended import jwt_required
from ..database.connect_db import conn
from ..scrape.scrape.spiders.local_raw_materials import LocalRawMaterialsSpider
from ..scrape.scrape.spiders.rawmatDetail import RawmatdetailSpider
from ..scrape.scrape.spiders.rawmat import RawmatSpider
from datetime import datetime
import pytz

phnom_penh = pytz.timezone('Asia/Phnom_Penh')
current_datetime = datetime.now(phnom_penh)

setup()
ns_raw = Namespace("api/v1/raw_materials")


# ______  100%  ______
# unpublished local

@ns_raw.route('/local/unpublished')
class unpublished(Resource):
    @jwt_required(refresh=False)
    @ns_raw.doc(security="Bearer")
    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                """
                    SELECT scraped_date FROM local_raw_materials where status = 'unpublished'
                    GROUP BY scraped_date
                    ORDER BY scraped_date DESC;
                """
            )
            raw_response = cursor.fetchall()
            if not raw_response:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in raw_response:
                response_data.append({
                    'category': 'Raw Materials',
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


# unpublished global

@ns_raw.route('/global/unpublished')
class unpublished(Resource):
    @jwt_required(refresh=False)
    @ns_raw.doc(security="Bearer")
    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                """
                    SELECT scraped_date FROM global_raw_material where status = 'unpublished'
                    GROUP BY scraped_date
                    ORDER BY scraped_date DESC;
                """
            )
            raw_response = cursor.fetchall()
            if not raw_response:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in raw_response:
                response_data.append({
                    'category': 'Raw Materials',
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


# unpublished

@ns_raw.route('/global/detail/unpublished')
class unpublished(Resource):
    @jwt_required(refresh=False)
    @ns_raw.doc(security="Bearer")
    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                """
                    SELECT scraped_date FROM raw_materials_detail where status = 'unpublished'
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
                    'category': 'Raw Materials',
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


@ns_raw.route('/global/both')
class ScrapeResource(Resource):

    @jwt_required(refresh=False)
    @ns_raw.doc(security="Bearer")
    def post(self):
        scrape_number = 0
        try:
            # Create a CrawlerRunner with project settings
            runner = CrawlerRunner()

            # Add your Scrapy spider to the runner
            detail = runner.crawl(RawmatdetailSpider)
            normal = runner.crawl(RawmatSpider)

            def run_reactor():
                reactor.run(installSignalHandlers=False)

            thread_pool = ThreadPoolExecutor(max_workers=1)
            thread_pool.submit(run_reactor)

            # Return a response immediately
            cursor = conn.cursor()
            scrape_number_query = "SELECT * FROM count_scrape_rawmaterials_global(1)"
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
                'error': str(e)}, 404


@jwt_required(refresh=False)
@ns_raw.doc(security="Bearer")
@ns_raw.doc(params={'scraped_date': 'The date timestamp for the update without millisecond'})
def put(self):
    scraped_date = request.args.get('scraped_date')
    try:
        datetime.strptime(scraped_date, '%Y-%m-%d %H:%M:%S')
    except ValueError:

        return {'success': False, 'message': 'Invalid scraped_date format, valid format is YYYY-MM-DD HH:MM:SS'}, 400

    # Validate the scraped_date parameter (you can add more validation)
    if scraped_date is None:
        return {'success': False, 'message': 'Invalid or missing scraped_date parameter'}, 400

    # Update the resource in the database using the scraped_date parameter

    cursor = conn.cursor()
    check_date_query = "SELECT status FROM global_raw_materials WHERE TO_CHAR" \
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
                    UPDATE global_raw_materials
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


@ns_raw.route("/global/published")
class GlobalRawPublished(Resource):

    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM global_raw_material where status = 'published'")
            global_raw_materials_published = cursor.fetchall()
            if not global_raw_materials_published:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in global_raw_materials_published:
                response_data.append({
                    'id': record[8],
                    'goods': record[0],
                    'current_price': record[1],
                    'high_price': record[2],
                    'low_price': record[3],
                    'status': record[4],
                    'scraped_date': record[5].isoformat(),
                    'variance': record[6],
                    'variance_per': record[7]
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@ns_raw.route("/global/getById/<int:material_id>")
class GlobalRawMaterialByID(Resource):

    def get(self, material_id):
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM global_raw_material WHERE id = %s", (material_id,))
            material_data = cursor.fetchone()
            if not material_data:
                return {
                    'success': False,
                    "message": f"No data found for material Id {material_id}"
                }, 404

            response_data = {
                'id': material_data[8],
                'goods': material_data[0],
                'current_price': material_data[1],
                'high_price': material_data[2],
                'low_price': material_data[3],
                'status': material_data[4],
                'scraped_date': material_data[5].isoformat(),
                'variance': material_data[6],
                'variance_per': material_data[7]
            }

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'payload': response_data
            }, 200


@ns_raw.route('/local')
class ScrapeResource(Resource):

    @jwt_required(refresh=False)
    @ns_raw.doc(security="Bearer")
    def post(self):
        scrape_number = 0
        try:
            # Create a CrawlerRunner with project settings
            runner = CrawlerRunner()

            # Add your Scrapy spider to the runner
            runner.crawl(LocalRawMaterialsSpider)

            # Create a thread pool to run the reactor in
            def run_reactor():
                reactor.run(installSignalHandlers=False)

            thread_pool = ThreadPoolExecutor(max_workers=1)
            thread_pool.submit(run_reactor)

            # Return a response immediately
            cursor = conn.cursor()
            scrape_number_query = "SELECT * FROM count_scrape_rawmaterials_local(1)"
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

    @jwt_required(refresh=False)
    @ns_raw.doc(security="Bearer")
    @ns_raw.doc(params={'scraped_date': 'The date timestamp for the update without millisecond'})
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
        check_date_query = "SELECT status FROM local_raw_materials WHERE TO_CHAR" \
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
                        UPDATE local_raw_materials
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
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM local_raw_materials")
            local_raw_materials = cursor.fetchall()
            if not local_raw_materials:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in local_raw_materials:
                response_data.append({
                    'id': record[0],
                    'goods': record[1],
                    'unit': record[2],
                    'price': record[3],
                    'status': record[4],
                    'scraped_date': record[5].isoformat()
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


# ______  100% /local/published ______
@ns_raw.route('/local/published')
class ScrapeResource(Resource):

    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM local_raw_materials where status = 'published'")
            published_local_materials = cursor.fetchall()
            if not published_local_materials:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in published_local_materials:
                response_data.append({
                    'id': record[0],
                    'goods': record[1],
                    'unit': record[2],
                    'price': record[3],
                    'status': record[4],
                    'scraped_date': record[5].isoformat()
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@ns_raw.route('/global/detail')
class ScrapeResource(Resource):

    @jwt_required(refresh=False)
    @ns_raw.doc(security="Bearer")
    def post(self):
        try:
            # Create a CrawlerRunner with project settings
            runner = CrawlerRunner()

            # Add your Scrapy spider to the runner
            runner.crawl(RawmatdetailSpider)

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
            return {'success': False,
                    'error': str(e)}, 404

    @jwt_required(refresh=False)
    @ns_raw.doc(security="Bearer")
    @ns_raw.doc(params={'scraped_date': 'The date timestamp for the update without millisecond'})
    def put(self):
        scraped_date = request.args.get('scraped_date')
        try:
            datetime.strptime(scraped_date, '%Y-%m-%d %H:%M:%S')
        except ValueError:

            return {'success': False,
                    'message': 'Invalid scraped_date format, valid format is YYYY-MM-DD HH:MM:SS'}, 400

        # Validate the scraped_date parameter (you can add more validation)
        if scraped_date is None:
            return {'success': False,
                    'message': 'Invalid or missing scraped_date parameter'}, 400

        # Update the resource in the database using the scraped_date parameter

        cursor = conn.cursor()
        check_date_query = "SELECT status FROM raw_materials_detail WHERE TO_CHAR" \
                           "(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = %s"
        cursor.execute(check_date_query, (scraped_date,))
        date_exists = cursor.fetchone()

        if date_exists and date_exists[0] == 'published':
            return {
                'success': False,
                'message': 'data is already updated'
            }, 400

        if not date_exists:
            return {'success': False,
                    'message': 'No data scraped in ' + scraped_date}, 404

        update_query = """
                            UPDATE raw_materials_detail
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
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM raw_materials_detail")
            global_raw_materials = cursor.fetchall()
            if not global_raw_materials:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in global_raw_materials:
                response_data.append({
                    'id': record[8],
                    'date': record[0],
                    'close_price': record[1],
                    'market_price': record[2],
                    'high_price': record[3],
                    'low_price': record[4],
                    'category': record[5],
                    'status': record[6],
                    'trading_volumn': record[9],
                    'variance_per': record[10],
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


# ______  100% /global/detail/published ______
@ns_raw.route('/global/detail/published')
class ScrapeResource(Resource):

    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM raw_materials_detail where status = 'published'")
            published_global_materials_detail = cursor.fetchall()
            if not published_global_materials_detail:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in published_global_materials_detail:
                response_data.append({
                    'id': record[8],
                    'date': record[0],
                    'close_price': record[1],
                    'market_price': record[2],
                    'high_price': record[3],
                    'low_price': record[4],
                    'category': record[5],
                    'status': record[6],
                    'trading_volumn': record[9],
                    'variance_per': record[10],
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@ns_raw.route('/global/detail/<category>')
class ScrapeResource(Resource):
    @ns_raw.doc(params={'category': 'The category to filter knowledge'})
    def get(self, category):
        # print("*** category ", category)
        if category is None:
            return {
                'success': False,
                'message': 'Invalid or missing category'}, 400
            # Check if the category is "currencies" or "cryptocurrency"

        # if category not in ["Brent oil", "WTI Yu", "Natural gas", "Heating oil", "gold", "silver",
        #                     "copper", "platinum","American Coffee C","American corn","American wheat",
        #                     "London sugar", 'U.S. NO.2']:
        #     return {
        #         'success': False,
        #         'message': 'category is either Brent oil, WTI Yu, Natural gas, Heating oil, gold, silver,'
        #                    'copper, platinum,American Coffee C,American corn,American wheat,'
        #                    'London sugar, U.S. NO.2'}, 400

        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM raw_materials_detail where status = 'published' and category = %s", (category,))

            rmd = cursor.fetchall()
            if not rmd:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []

            for record in rmd:
                response_data.append({
                    'id': record[8],
                    'date': record[0],
                    'close_price': record[1],
                    'market_price': record[2],
                    'high_price': record[3],
                    'low_price': record[4],
                    'category': record[5],
                    'status': record[6],
                    'trading_volumn': record[9],
                    'variance_per': record[10],
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


# ______  100% ______
@ns_raw.route("/global")
class Resource(Resource):
    @jwt_required(refresh=False)
    @ns_raw.doc(security="Bearer")
    def post(self):
        try:
            # Create a CrawlerRunner with project settings
            runner = CrawlerRunner()

            # Add your Scrapy spider to the runner
            runner.crawl(RawmatSpider)
            print("skdfjaskdfalsdf")

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
    @ns_raw.doc(security="Bearer")
    @ns_raw.doc(params={'scraped_date': 'The date timestamp for the update without millisecond'})
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
        check_date_query = "SELECT status FROM global_raw_material WHERE TO_CHAR" \
                           "(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = %s"
        cursor.execute(check_date_query, (scraped_date,))
        date_exists = cursor.fetchone()

        print("$%", date_exists)

        if not date_exists:
            return {'success': False, 'message': 'No data scraped in ' + scraped_date}, 404

        if date_exists and date_exists[0] == 'published':
            return {
                'success': False,
                'message': 'data is already updated'
            }, 400
        update_query = """
                        UPDATE global_raw_material
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
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM global_raw_material")
            global_raw_materials = cursor.fetchall()
            if not global_raw_materials:
                return {
                    'success': True,
                    "message: ": "data is empty"
                }, 200
            response_data = []
            for record in global_raw_materials:
                response_data.append({
                    'goods': record[0],
                    'current_price': record[1],
                    'high_price': record[2],
                    'low_price': record[3],
                    'status': record[4],
                    'variance': record[6],
                    'variance_per': record[7]
                })
            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200
