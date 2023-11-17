from flask import request
from flask_restx import Namespace, Resource, fields
from ..scrape.scrape.spiders.global_new import GlobalNewsSpider
from ..scrape.scrape.spiders.cambodia_news import CambodiaNewsSpider
from twisted.internet import reactor
from scrapydo import setup
from scrapy.crawler import CrawlerRunner
from concurrent.futures import ThreadPoolExecutor
from flask_jwt_extended import jwt_required
from ..database.connect_db import conn
from datetime import datetime
import pytz

# Call setup() to configure Scrapy to run in a separate thread
setup()
ns_global_new = Namespace("api/v1/news")

phnom_penh = pytz.timezone('Asia/Phnom_Penh')
current_datetime = datetime.now(phnom_penh)

global_count = 0


@ns_global_new.route('/global')
class ScrapeResource(Resource):
    @jwt_required(refresh=False)
    @ns_global_new.doc(security="Bearer")
    def post(self):
        scrape_number = None
        try:
            # Create a CrawlerRunner with project settings
            runner = CrawlerRunner()

            # Add your Scrapy spider to the runner
            runner.crawl(GlobalNewsSpider)

            # Create a thread pool to run the reactor in
            def run_reactor():
                reactor.run(installSignalHandlers=False)

            thread_pool = ThreadPoolExecutor(max_workers=1)
            thread_pool.submit(run_reactor)

            cursor = conn.cursor()
            scrape_number_query = "SELECT * FROM count_scrape_news_global(1)"
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
    @ns_global_new.doc(security="Bearer")
    @ns_global_new.doc(params={'scraped_date': 'The date timestamp for the update without millisecond'})
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
        check_date_query = "SELECT status FROM global_news WHERE TO_CHAR" \
                           "(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = %s"
        cursor.execute(check_date_query, (scraped_date,))
        date_exists = cursor.fetchone()

        if date_exists and date_exists[0] == 'published':
            return {
                'success': False,
                'message': 'data is already updated'
            }, 400
        published_date = datetime.now()
        if not date_exists:
            return {
                'success': False,
                'message': 'No data scraped in ' + scraped_date
            }, 404

        update_query = """
                            UPDATE global_news
                            SET status = %s, published_date = %s
                            WHERE TO_CHAR(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = %s;
                            """
        try:
            cursor.execute(update_query, ('published', published_date, scraped_date))
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
                "SELECT * FROM global_news")
            global_news = cursor.fetchall()
            if not global_news:
                return {
                    'success': True,
                    "message: ": "data is empty"
                }, 200
            response_data = []
            print("________________________________________: ", global_news)
            for record in global_news:
                published_date = record[7]
                if published_date is not None:
                    published_date_str = published_date.isoformat()
                else:
                    published_date_str = None
                # print("________________________________________recode: ", response_data)
                response_data.append({
                    'id': record[9],
                    'category': record[0],
                    'title': record[1],
                    'image': record[2],
                    'paragraph': record[3],
                    'published': record[4],
                    'update': record[5],
                    'date_scrape': record[6].isoformat(),
                    'published_date': published_date_str,
                    'status': record[8]
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@ns_global_new.route('/global/published')
class ScrapeResource(Resource):

    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM global_news where status = 'published' order by published_date desc")
            global_news = cursor.fetchall()
            if not global_news:
                return {
                    'success': False,
                    "message: ": "data is empty"
                }, 200
            response_data = []
            print("________________________________________: ", global_news)
            for record in global_news:
                published_date = record[7]
                if published_date is not None:
                    published_date_str = published_date.isoformat()
                else:
                    published_date_str = None
                response_data.append({
                    'id':record[9],
                    'category': record[0],
                    'title': record[1],
                    'image': record[2],
                    'paragraph': record[3],
                    'published': record[4],
                    'update': record[5],
                    'date_scrape': record[6].isoformat(),
                    'published_date': published_date_str,
                    'status': record[8]
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@ns_global_new.route('/cambodia')
class ScrapeResource(Resource):

    @jwt_required(refresh=False)
    @ns_global_new.doc(security="Bearer")
    def post(self):
        scrape_number = 0
        try:
            # Create a CrawlerRunner with project settings
            runner = CrawlerRunner()

            # Add your Scrapy spider to the runner
            runner.crawl(CambodiaNewsSpider)

            # Create a thread pool to run the reactor in
            def run_reactor():
                reactor.run(installSignalHandlers=False)

            thread_pool = ThreadPoolExecutor(max_workers=1)
            thread_pool.submit(run_reactor)

            # Return a response immediately
            cursor = conn.cursor()
            scrape_number_query = "SELECT * FROM count_scrape_news_cambodia(1)"
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
    @ns_global_new.doc(security="Bearer")
    @ns_global_new.doc(params={'scraped_date': 'The date timestamp for the update without millisecond'})
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
        check_date_query = "SELECT status FROM cambodia_news WHERE TO_CHAR" \
                           "(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = %s"
        cursor.execute(check_date_query, (scraped_date,))
        date_exists = cursor.fetchone()

        if date_exists and date_exists[0] == 'published':
            return {
                'success': False,
                'message': 'data is already updated'
            }, 400

        published_date = datetime.now()
        if not date_exists:
            return {
                'success': False,
                'message': 'No data scraped in ' + scraped_date
            }, 404

        update_query = """
                                UPDATE cambodia_news
                                SET status = %s, published_date = %s
                                WHERE TO_CHAR(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = %s;
                                """
        try:
            cursor.execute(update_query, ('published', published_date, scraped_date))
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
                "SELECT * FROM cambodia_news")
            cambodia_news_response = cursor.fetchall()
            if not cambodia_news_response:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []

            for record in cambodia_news_response:
                published_date = record[5]
                if published_date is not None:
                    published_date_str = published_date.isoformat()
                else:
                    published_date_str = None
                response_data.append({
                    'id': record[7],
                    'published_date_scraped_website': record[0],
                    'title': record[1],
                    'description': record[2],
                    'image': record[3],
                    'scraped_date': record[4].isoformat(),
                    'published_date': published_date_str,
                    'status': record[6]
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@ns_global_new.route('/cambodia/published')
class ScrapeResource(Resource):

    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM cambodia_news where status = 'published' order by published_date desc ")
            cambodia_news_response = cursor.fetchall()
            if not cambodia_news_response:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []

            for record in cambodia_news_response:
                published_date = record[5]
                if published_date is not None:
                    published_date_str = published_date.isoformat()
                else:
                    published_date_str = None
                response_data.append({
                    'id': record[7],
                    'published_date_scraped_website': record[0],
                    'title': record[1],
                    'description': record[2],
                    'image': record[3],
                    'scraped_date': record[4].isoformat(),
                    'published_date': published_date_str,
                    'status': record[6]
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200



@ns_global_new.route('/global/<category>')
class ScrapeResource(Resource):
    @ns_global_new.doc(params={'category': 'The category to filter news'})
    def get(self, category):
        # category = request.args.get('category')
        # print("*** category ", category)
        if category is None:
            return {
                'success': False,
                'message': 'Invalid or missing category'
            }, 400

        if category.lower() not in ["currencies", "cryptocurrency", "commodities", "economy", "stock"]:
            return {
                'success': False,
                'message': 'Category is either stock, cryptocurrency, currencies, commodities, economy'
            }, 400

        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM global_news where status = 'published' and category = %s order by published_date desc", (category.lower(),) )
            global_new = cursor.fetchall()
            if not global_new:
                return {
                    "message: ": "data is empty"
                }, 200
            response_data = []

            for record in global_new:
                response_data.append({
                    'id': record[9],
                    'category': record[0],
                    'title': record[1],
                    'image': record[2],
                    'paragraph': record[3],
                    'published': record[4],
                    'update': record[5],
                    'date_scrape': record[6].isoformat(),
                    'published_date': record[7].isoformat(),
                    'status': record[8]
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


# unpublished global news

@ns_global_new.route('/global/unpublished')
class unpublished(Resource):
    @jwt_required(refresh=False)
    @ns_global_new.doc(security="Bearer")
    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                """
                    SELECT scraped_date FROM global_news where status = 'unpublished'
                    GROUP BY scraped_date
                    ORDER BY scraped_date DESC;
                """
            )
            news_response = cursor.fetchall()
            if not news_response:
                return {
                    'success': True,
                    "message: ": "data is empty"
                }, 200
            response_data = []
            for record in news_response:
                response_data.append({
                    'category': 'News',
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


# unpublished local news

@ns_global_new.route('/cambodia/unpublished')
class unpublished(Resource):
    @jwt_required(refresh=False)
    @ns_global_new.doc(security="Bearer")
    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                """
                    SELECT scraped_date FROM cambodia_news where status = 'unpublished'
                    GROUP BY scraped_date
                    ORDER BY scraped_date DESC;
                """
            )
            news_response = cursor.fetchall()
            if not news_response:
                return {
                    'success': True,
                    "message: ": "data is empty"
                }, 200
            response_data = []
            for record in news_response:
                response_data.append({
                    'category': 'News',
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


@ns_global_new.route('/global/<int:id>')
class ScrapeResource(Resource):
    @ns_global_new.doc(params={'id': 'search by id'})
    def get(self, id):
        if id is None:
            return {
                'success': False,
                'message': 'Invalid or missing category'
            }, 400

        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM global_news where status = 'published' and id = %s order by published_date desc", (id,))
            global_new = cursor.fetchall()

            # Check if any rows were returned
            if not global_new:
                return {
                    'success': False,
                    "message": "Data not found for id " + str(id)
                }, 404

            response_data = {
                'id': global_new[0][9],
                'category': global_new[0][0],
                'title': global_new[0][1],
                'image': global_new[0][2],
                'paragraph': global_new[0][3],
                'published': global_new[0][4],
                'update': global_new[0][5],
                'date_scrape': global_new[0][6].isoformat(),
                'published_date': global_new[0][7].isoformat(),
                'status': global_new[0][8]
            }
            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'Get data successfully',
                'payload': response_data
            }, 200


@ns_global_new.route('/cambodia/<int:id>')
class ScrapeResource(Resource):
    @ns_global_new.doc(params={'id': 'search by id'})
    def get(self, id):

        if id is None:
            return {'message': 'Invalid or missing category'}, 400

        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM cambodia_news where status = 'published' and id = %s order by published_date desc ", (id,))
            cam_new = cursor.fetchall()

            # Check if any rows were returned
            if not cam_new:
                return {
                    'success': False,
                    "message": "Data not found for id " + str(id)
                }, 404

            response_data = {
                'id': cam_new[0][7],
                'published_date_scraped_website': cam_new[0][0],
                'title': cam_new[0][1],
                'description': cam_new[0][2],
                'image': cam_new[0][3],
                'scraped_date': cam_new[0][4].isoformat(),
                'published_date': cam_new[0][5].isoformat(),
                'status': cam_new[0][6]
            }
            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'Get data successfully',
                'payload': response_data
            }, 200
