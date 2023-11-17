from datetime import datetime
import pytz
from flask import request

from ..database.connect_db import conn

from flask_restx import Namespace, Resource, reqparse

mock_ns = Namespace("api/v1/endpoint")

phnom_penh = pytz.timezone('Asia/Phnom_Penh')
current_datetime = datetime.now(phnom_penh)

# Assuming 'conn' is a global connection to your PostgreSQL database
endpoint = 'http://127.0.0.1:5000/api/v1/endpoint/'


@mock_ns.route('')
class MyResource(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('name', type=str, help='Name parameter')
    parser.add_argument('size', type=int, help='Size of the data')
    parser.add_argument('page', type=int, help='Page number')

    @mock_ns.doc(params={'name': 'category', 'size': 'Number of rows user wants to get', 'page': 'Page number'},
                 required=False)
    def get(self):
        args = self.parser.parse_args()

        if 'name' not in request.args:
            return {
                'success': False,
                'message': "Name parameter can't be empty"
            }, 400

        name = args['name']
        size = args['size']
        page = args['page']

        print("name ", name, " size ", size, " page ", page)

        valid_names = ['stock', 'cambodia-exchange-rate', 'crypto', 'gdp', 'raw-material']
        if args['name'] not in valid_names:
            return {
                'success': False,
                'message': f"Invalid value for 'name', should be one of {valid_names}."
            }, 400

        with conn.cursor() as cursor:

            cursor.execute(
                """
                   SELECT COUNT(*) FROM global_stock where status = 'published' 
                """,
            )
            stock_count = cursor.fetchone()[0]

        if name == 'stock':
            if size is not None and size > stock_count:
                return {
                    'success': False,
                    'message': f"Size cannot exceed the record count of 107 for {name}."
                }, 400
            if page is not None and page > stock_count:
                return {
                    'success': False,
                    'message': f"Page cannot exceed the record count of 107 for {name}."
                }, 400

            if page is not None and size is not None and page * size > stock_count:
                return {
                    'success': False,
                    'message': f"Page cannot exceed the record count for {name}."
                }, 400

        with conn.cursor() as cursor:

            cursor.execute(
                """
                   SELECT COUNT(*) FROM global_stock where status = 'published' 
                """,
            )
            crypto_count = cursor.fetchone()[0]
        if name == 'crypto':
            if size is not None and size > crypto_count:
                return {
                    'success': False,
                    'message': f"Size cannot exceed the record count of 10 for {name}."
                }, 400
            if page is not None and page > crypto_count:
                return {
                    'success': False,
                    'message': f"Page cannot exceed the record count of 10 for {name}."
                }, 400

            if page is not None and size is not None and page * size > crypto_count:
                return {
                    'success': False,
                    'message': f"Page cannot exceed the record count for {name}."
                }, 400

        if name == 'cambodia-exchange-rate':
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                        SELECT COUNT(*) FROM local_exchange where status = 'published'
                    """
                )
                row = cursor.fetchone()

            if row is not None and size is not None and size > row[0]:
                return {
                    'success': False,
                    'message': f"Size cannot exceed the record count for {name}."
                }, 400
            if page is not None and page > row[0]:
                return {
                    'success': False,
                    'message': f"Page cannot exceed the record count for {name}."
                }, 400

            if page is not None and size is not None and page * size > row[0]:
                return {
                    'success': False,
                    'message': f"Page cannot exceed the record count for {name}."
                }, 400

        if name == 'gdp':
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                        SELECT COUNT(*) FROM cambodia_gdp where status = 'published'
                    """
                )
                row = cursor.fetchone()

            if row is not None and size is not None and size > row[0]:
                return {
                    'success': False,
                    'message': f"Size cannot exceed the record count for {name}."
                }, 400
            if page is not None and page > row[0]:
                return {
                    'success': False,
                    'message': f"Page cannot exceed the record count for {name}."
                }, 400

            if page is not None and size is not None and page * size > row[0]:
                return {
                    'success': False,
                    'message': f"Page cannot exceed the record count for {name}."
                }, 400

        if name == 'raw-material':
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                        SELECT COUNT(*) FROM global_raw_material where status = 'published'
                    """
                )
                row = cursor.fetchone()

            if row is not None and size is not None and size > row[0]:
                return {
                    'success': False,
                    'message': f"Size cannot exceed the record count for {name}."
                }, 400
            if page is not None and page > row[0]:
                return {
                    'success': False,
                    'message': f"Page cannot exceed the record count for {name}."
                }, 400

            if page is not None and size is not None and page * size > row[0]:
                return {
                    'success': False,
                    'message': f"Page cannot exceed the record count for {name}."
                }, 400

        # Check if the 'name' parameter is empty or None
        if not name:
            return {
                'success': False,
                'message': "Name parameter can't be empty"
            }, 400

        if size is not None and page is None:
            return {'response': f'{endpoint}{name}?size={size}&page=1'}

        if page is not None and size is None:
            return {'response': f'{endpoint}{name}?size=10&page={page}'}

        # If size and page are provided
        if size is not None and page is not None:
            return {'response': f'{endpoint}{name}?size={size}&page={page}'}

        # If only name is provided
        return {'response': f'{endpoint}{name}'}


@mock_ns.route('/stock')
class FakeData(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('size', type=int, default=10, help='Number of rows user wants to get')
    parser.add_argument('page', type=int, default=1, help='Page number')

    @mock_ns.doc(params={'size': 'Number of rows user wants to get', 'page': 'Page number'}, required=False)
    def get(self):
        args = self.parser.parse_args()
        size = args['size']
        page = args['page']

        with conn.cursor() as cursor:
            offset = (page - 1) * size

            cursor.execute(
                """
                    SELECT * FROM global_stock WHERE status = 'published' LIMIT %s OFFSET %s
                """, (size, offset)
            )
            response = []
            stock = cursor.fetchall()
            if not stock:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            for record in stock:
                response.append({
                    'stock': record[0],
                    'close_price': record[1],
                    'change': record[2],
                    'percentage_change': record[3],
                    'category': record[4]
                })

        return {"response": response}


@mock_ns.route('/cambodia-exchange-rate')
class FakeData(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('size', type=int, default=10, help='Number of rows user wants to get')
    parser.add_argument('page', type=int, default=1, help='Page number')

    @mock_ns.doc(params={'size': 'Number of rows user wants to get', 'page': 'Page number'}, required=False)
    def get(self):
        args = self.parser.parse_args()
        size = args['size']
        page = args['page']

        with conn.cursor() as cursor:
            offset = (page - 1) * size

            cursor.execute(
                """
                    SELECT * FROM local_exchange where status = 'published' LIMIT %s OFFSET %s 
                """, (size, offset)
            )
            response = []
            exchange = cursor.fetchall()
            if not exchange:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            for record in exchange:
                response.append({
                    'id': record[0],
                    'currency': record[1],
                    'symbol': f"{record[2]}/{record[3]}",
                    'unit': record[4],
                    'buying': record[5],
                    'sale': record[6],
                    'average': record[7],
                })
        return {"response": response}


@mock_ns.route('/crypto')
class FakeData(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('size', type=int, default=10, help='Number of rows user wants to get')
    parser.add_argument('page', type=int, default=1, help='Page number')

    @mock_ns.doc(params={'size': 'Number of rows user wants to get', 'page': 'Page number'}, required=False)
    def get(self):
        args = self.parser.parse_args()
        size = args['size']
        page = args['page']

        with conn.cursor() as cursor:
            offset = (page - 1) * size

            cursor.execute(
                """
                    SELECT * FROM crypto where status = 'published' LIMIT %s OFFSET %s 
                """, (size, offset)
            )
            response = []
            crypto = cursor.fetchall()
            if not crypto:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            for record in crypto:
                response.append({
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
        return {"response": response}


@mock_ns.route('/gdp')
class FakeData(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('size', type=int, default=10, help='Number of rows user wants to get')
    parser.add_argument('page', type=int, default=1, help='Page number')

    @mock_ns.doc(params={'size': 'Number of rows user wants to get', 'page': 'Page number'}, required=False)
    def get(self):
        args = self.parser.parse_args()
        size = args['size']
        page = args['page']

        with conn.cursor() as cursor:
            offset = (page - 1) * size

            cursor.execute(
                """
                    SELECT * FROM cambodia_gdp where status = 'published' LIMIT %s OFFSET %s 
                """, (size, offset)
            )
            response = []
            gdp = cursor.fetchall()
            if not gdp:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            for record in gdp:
                response.append({
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
        return {"response": response}


@mock_ns.route('/raw-material')
class FakeData(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('size', type=int, default=10, help='Number of rows user wants to get')
    parser.add_argument('page', type=int, default=1, help='Page number')

    @mock_ns.doc(params={'size': 'Number of rows user wants to get', 'page': 'Page number'}, required=False)
    def get(self):
        args = self.parser.parse_args()
        size = args['size']
        page = args['page']

        with conn.cursor() as cursor:
            offset = (page - 1) * size

            cursor.execute(
                """
                    SELECT * FROM global_raw_material where status = 'published' LIMIT %s OFFSET %s 
                """, (size, offset)
            )
            response = []
            gdp = cursor.fetchall()
            if not gdp:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
            for record in gdp:
                response.append({
                    'goods': record[0],
                    'current_price': record[1],
                    'high_price': record[2],
                    'low_price': record[3],
                    'status': record[4],
                    'variance': record[6],
                    'variance_per': record[7]
                })
        return {"response": response}


@mock_ns.route('/gdp/count')
class Count(Resource):

    def get(self):
        with conn.cursor() as cursor:
            cursor.execute(
                """
                   SELECT COUNT(*) FROM cambodia_gdp where status = 'published' 
                """,
            )
            gdp_count = cursor.fetchone()
            if not gdp_count:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
        return {
            'success': True,
            "response": gdp_count[0]
        }


@mock_ns.route('/raw-material/count')
class Count(Resource):

    def get(self):
        with conn.cursor() as cursor:
            cursor.execute(
                """
                   SELECT COUNT(*) FROM global_raw_material where status = 'published' 
                """,
            )
            gdp_count = cursor.fetchone()
            if not gdp_count:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
        return {
            'success': True,
            "response": gdp_count[0]
        }


@mock_ns.route('/crypto/count')
class Count(Resource):

    def get(self):
        with conn.cursor() as cursor:
            cursor.execute(
                """
                   SELECT COUNT(*) FROM crypto where status = 'published' 
                """,
            )
            gdp_count = cursor.fetchone()
            if not gdp_count:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
        return {
            'success': True,
            "response": gdp_count[0]
        }


@mock_ns.route('/cambodia-exchange-rate/count')
class Count(Resource):

    def get(self):
        with conn.cursor() as cursor:
            cursor.execute(
                """
                   SELECT COUNT(*) FROM local_exchange where status = 'published' 
                """,
            )
            gdp_count = cursor.fetchone()
            if not gdp_count:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
        return {
            'success': True,
            "response": gdp_count[0]
        }


@mock_ns.route('/stock/count')
class Count(Resource):

    def get(self):
        with conn.cursor() as cursor:
            cursor.execute(
                """
                   SELECT COUNT(*) FROM global_stock where status = 'published' 
                """,
            )
            gdp_count = cursor.fetchone()
            if not gdp_count:
                return {
                    'success': True,
                    "message: ": "data is empty"}, 200
        return {
            'success': True,
            "response": gdp_count[0]
        }


@mock_ns.route('/count')
class FakeData(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('count', type=str, help='category you want to count')

    @mock_ns.doc(params={'count': 'category you want to count'})
    def get(self):
        args = self.parser.parse_args()
        count = args['count']

        allowed_categories = ['global_stock', 'local_exchange', 'crypto', 'cambodia_gdp', 'global_raw_material']

        with conn.cursor() as cursor:
            try:
                if count not in allowed_categories:
                    return {
                        'success': False,
                        'response': "Invalid category: valid category should be global_stock, local_exchange, crypto, "
                                    "cambodia_gdp, global_raw_material"
                    }, 400

                # Use string concatenation for the table name
                query = f"SELECT COUNT(*) FROM {count} WHERE status = 'published'"

                cursor.execute(query)
                count_result = cursor.fetchone()

                if not count_result:
                    return {
                        'success': True, 'message': 'Data is empty'
                    }, 200
                if count == 'global_stock':
                    response_category = 'Stocks'
                elif count == 'local_exchange':
                    response_category = 'Exchange Rate'
                elif count == 'crypto':
                    response_category = 'Crypto'
                elif count == 'cambodia_gdp':
                    response_category = 'GDP'
                elif count == 'global_raw_material':
                    response_category = 'Raw Materials'
                return {
                    'success': True,
                    'response': {
                        "category": response_category,
                        "count": count_result[0]
                    }
                }

            except ValueError as ve:
                return {'success': False, 'error': str(ve)}, 400
