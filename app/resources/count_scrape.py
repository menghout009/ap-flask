from flask_jwt_extended import jwt_required
from flask_restx import Namespace, Resource
from ..database.connect_db import conn
import datetime
import pytz

count_scrape_ns = Namespace("api/v1/count_scrape")

phnom_penh = pytz.timezone('Asia/Phnom_Penh')
current_datetime = datetime.datetime.now(phnom_penh)


@count_scrape_ns.route('/all')
class gdp(Resource):
    @jwt_required(refresh=False)
    @count_scrape_ns.doc(security="Bearer")
    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT sum(news_global) + sum(news_cambodia) + sum(stock_local) + sum(stock_global)+ "
                "sum(raw_material_local) + sum(raw_material_global) + sum(gdp) + sum(exchange_rate_local)+ "
                "sum(exchange_rate_global) + sum(crypto) + sum(knowledge) + sum(bond)AS count FROM scrape_number")
            count_response = cursor.fetchone()[0]
            if not count_response:
                return {
                    'success': False,
                    "message: ": "data is empty"}, 200
            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'count': count_response
            }, 200


@count_scrape_ns.route('/category')
class gdp(Resource):
    @jwt_required(refresh=False)
    @count_scrape_ns.doc(security="Bearer")
    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT sum(news_global) + sum(news_cambodia) as news ,sum(stock_local) + sum(stock_global) as stock,"
                "sum(raw_material_local) + sum(raw_material_global) as raw_material, sum(gdp) as gdp, "
                "sum(exchange_rate_local)+ sum(exchange_rate_global) as exchange_rate, sum(crypto) as crypto,"
                "sum(knowledge) as knowledge, sum(bond)AS bond FROM scrape_number")
            record = cursor.fetchone()
            if not record:
                return {
                    "message: ": "data is empty"}, 200

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': {
                    'news': record[0],
                    'stock': record[1],
                    'raw_material': record[2],
                    'gdp': record[3],
                    'exchange_rate': record[4],
                    'crypto': record[5],
                    'knowledge': record[6],
                    'bond': record[7],
                }
            }, 200


@count_scrape_ns.route('/part')
class gdp(Resource):
    @jwt_required(refresh=False)
    @count_scrape_ns.doc(security="Bearer")
    def get(self):
        with conn.cursor() as cursor:
            # Specify the column names in your SQL query
            cursor.execute(
                "SELECT sum(news_global) as news_global, sum(news_cambodia) as news_cambodia ,"
                "sum(stock_local) as stock_local, sum(stock_global) as stock_global,"
                "sum(raw_material_local) as raw_material_local,sum(raw_material_global) as raw_material_global"
                ",sum(gdp) as gdp, sum(exchange_rate_local) as exchange_rate_local, sum(exchange_rate_global) "
                "as exchange_rate_global, sum(crypto) as crypto,"
                "sum(knowledge) as knowledge, sum(bond)AS bond FROM scrape_number")
            record = cursor.fetchone()
            if not record:
                return {
                    "message: ": "data is empty"}, 200

            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'get data successfully',
                'payload': {
                    'global_news': record[0],
                    'cambodia_news': record[1],
                    'global_stock': record[3],
                    'local_stock': record[2],
                    'global_raw_material': record[5],
                    'local_raw_material': record[4],
                    'gdp': record[6],
                    'global_exchange_rate': record[8],
                    'local_exchange_rate': record[7],
                    'crypto': record[9],
                    'knowledge': record[10],
                    'bond': record[11],
                }
            }, 200
