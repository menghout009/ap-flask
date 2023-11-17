from datetime import datetime

import pytz
from flask import request
from flask_restx import Namespace, Resource
from scrapy.crawler import CrawlerRunner
from flask_jwt_extended import jwt_required
from ..scrape.scrape.spiders.local_knowledge import LocalKnowledge
from twisted.internet import reactor
from concurrent.futures import ThreadPoolExecutor
from ..database.connect_db import conn

knowledge_ns = Namespace("api/v1/knowledge")

phnom_penh = pytz.timezone('Asia/Phnom_Penh')
current_datetime = datetime.now(phnom_penh)


@knowledge_ns.route('')
class gdp(Resource):

    @jwt_required(refresh=False)
    @knowledge_ns.doc(security="Bearer")
    def post(self):
        scrape_number = 0
        try:
            # Create a CrawlerRunner with project settings
            runner = CrawlerRunner()

            # Add your Scrapy spider to the runner
            runner.crawl(LocalKnowledge)

            # Create a thread pool to run the reactor in
            def run_reactor():
                reactor.run(installSignalHandlers=False)

            thread_pool = ThreadPoolExecutor(max_workers=1)
            thread_pool.submit(run_reactor)

            # Return a response immediately
            cursor = conn.cursor()
            scrape_number_query = "SELECT * FROM count_scrape_knowledge(1)"
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
    @knowledge_ns.doc(security="Bearer")
    @knowledge_ns.doc(params={'scraped_date': 'The date timestamp for the update without millisecond'})
    def put(self):
        scraped_date = request.args.get('scraped_date')

        try:
            datetime.strptime(scraped_date, '%Y-%m-%d %H:%M:%S')
        except ValueError:

            return {
                'success': False,
                'message': 'Invalid scraped_date format, valid format is YYYY-MM-DD HH:MM:SS'
            }, 400

        published_date = datetime.now()

        # Validate the scraped_date parameter (you can add more validation)
        if scraped_date is None:
            return {
                'success': False,
                'message': 'Invalid or missing scraped_date parameter'
            }, 400

        cursor = conn.cursor()
        check_date_query = "SELECT status FROM local_knowledge WHERE TO_CHAR" \
                           "(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = %s"

        cursor.execute(check_date_query, (scraped_date,))
        existing_status = cursor.fetchone()

        if not existing_status:
            return {
                'success': False,
                'message': 'No data scraped in ' + scraped_date
            }, 404
        if existing_status and existing_status[0] == 'published':
            return {
                'success': False,
                'message': 'data is already updated'
            }, 400
        update_query = """
                            UPDATE local_knowledge
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
                "SELECT * FROM local_knowledge")
            local_knowledge = cursor.fetchall()
            if not local_knowledge:
                return {
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in local_knowledge:
                published_date = record[8]
                if published_date is not None:
                    published_date_str = published_date.isoformat()
                else:
                    published_date_str = None
                response_data.append({
                    'id': record[0],
                    'no': record[1],
                    'name': record[2],
                    'link': record[3],
                    'posted_date': record[4],
                    'category': record[5],
                    'status': record[6],
                    'scraped_date': record[7].isoformat(),
                    'published_date': published_date_str
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@knowledge_ns.route('/published')
class gdp(Resource):

    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM local_knowledge where status = 'published'")
            local_knowledge = cursor.fetchall()
            if not local_knowledge:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in local_knowledge:
                published_date = record[8]
                if published_date is not None:
                    published_date_str = published_date.isoformat()
                else:
                    published_date_str = None
                response_data.append({
                    'id': record[0],
                    'no': record[1],
                    'name': record[2],
                    'link': record[3],
                    'posted_date': record[4],
                    'category': record[5],
                    'status': record[6],
                    'scraped_date': record[7].isoformat(),
                    'published_date': published_date_str
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


@knowledge_ns.route('/<category>')
class ScrapeResource(Resource):
    @knowledge_ns.doc(params={'category': 'The category to filter knowledge'})
    def get(self, category):
        # print("*** category ", category)
        if category is None:
            return {
                'success': False,
                'message': 'Invalid or missing category'}, 400
            # Check if the category is "currencies" or "cryptocurrency"

        if category.lower() not in ["ebook", "publication", "seminar", "regulation"]:
            return {
                'success': False,
                'message': 'category is either ebook, publication, seminar, regulation'}, 400

        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT * FROM local_knowledge where status = 'published' and category = %s", (category.lower(),))
            knowledge = cursor.fetchall()
            if not knowledge:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []

            for record in knowledge:
                published_date = record[8]
                if published_date is not None:
                    published_date_str = published_date.isoformat()
                else:
                    published_date_str = None
                response_data.append({
                    'id': record[0],
                    'no': record[1],
                    'name': record[2],
                    'link': record[3],
                    'posted_date': record[4],
                    'category': record[5],
                    'status': record[6],
                    'scraped_date': record[7].isoformat(),
                    'published_date': published_date_str
                })

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': response_data
            }, 200


# unpublished

@knowledge_ns.route('/unpublished')
class unpublished(Resource):
    @jwt_required(refresh=False)
    @knowledge_ns.doc(security="Bearer")
    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                """
                    SELECT scraped_date FROM local_knowledge where status = 'unpublished'
                    GROUP BY scraped_date
                    ORDER BY scraped_date DESC;
                """
            )
            knowledge_response = cursor.fetchall()
            if not knowledge_response:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            response_data = []
            for record in knowledge_response:
                response_data.append({
                    'category': 'Knowledge',
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
