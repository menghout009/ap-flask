from flask import Flask

from .resources.stock_resource import ns_stock
from .resources.raw_materials_resource import ns_raw
from .resources.auto_scape import ns_link
from .resources.cambodia_gdp_resource import gdp_ns
from apscheduler.schedulers.background import BackgroundScheduler
from .resources.auth_resource import auth_ns

from .resources.finance_resource import ns_f

from .resources.global_new_resource import ns_global_new
from .resources.exchange_rate_resource import exchange_ns
from .resources.global_bond_resource import ns_bond
from .resources.crypto_resource import crypto_ns
from .resources.knowledge_resource import knowledge_ns
from .resources.count_scrape import count_scrape_ns
from app.chatbot.chatbot_resource import chatbot_ns
from .extensions import jwt, api

from .authentication.auth_header import authorizations
from .authentication.jwt_config import JWTConfig
from .authentication.cors_config import configure_cors

from .resources.stock_resource import ns_stock
from .resources.raw_materials_resource import ns_raw
from flask_restx import Api, Resource
from .authentication.auth_header import authorizations
from .database.connect_db import conn
from .scrape.scrape.spiders.rawmat import RawmatSpider
from .scrape.scrape.spiders.stock_historical_data import StockHistoricalDataSpider
from .scrape.scrape.spiders.most_active import MostActiveSpider
from .scrape.scrape.spiders.rawmatDetail import RawmatdetailSpider
from .scrape.scrape.spiders.finance_data import FinanceDataSpider
from .scrape.scrape.spiders.global_new import GlobalNewsSpider
from .scrape.scrape.spiders.global_bond import GlobalBondSpider
from .scrape.scrape.spiders.stock_trade_summary import StockTradeSummarySpider
from .scrape.scrape.spiders.crypto_historical_date import CryptoHistoricalSpider
from .scrape.scrape.spiders.global_ExchangeRate_historical_data import GlobalExchangeRateHistorical
from .scrape.scrape.spiders.global_ExchangeRate import GlobalExchangeRate
from .scrape.scrape.spiders.stock_detail_data import StockDetailDataSpider
from .scrape.scrape.spiders.local_raw_materials import LocalRawMaterialsSpider
from .scrape.scrape.spiders.rawmat import RawmatSpider
from .scrape.scrape.spiders.crypto_data import CryptoSpider
from .scrape.scrape.spiders.local_Exchange import bank

from .resources.stock_resource import ScrapeGlobalStock

from .resources.mock_api_resource import mock_ns

import scrapydo

scrapydo.setup()


def create_app():
    app = Flask(__name__)
    app.debug = True
    app.run(debug=True)
    api = Api(
        app,
        version='1.0',
        title='Data Vue API',
        authorizations=authorizations
    )

    api.add_namespace(ns_stock)
    api.add_namespace(ns_raw)
    api.add_namespace(auth_ns)
    api.add_namespace(ns_link)
    api.add_namespace(gdp_ns)

    api.add_namespace(ns_global_new)
    api.add_namespace(exchange_ns)
    api.add_namespace(crypto_ns)
    api.add_namespace(knowledge_ns)
    api.add_namespace(count_scrape_ns)
    api.add_namespace(mock_ns)
    api.add_namespace(chatbot_ns)
    # api.add_namespace(chat_ns)

    api.add_namespace(ns_bond)
    # api.add_namespace(ns_f)
    api.add_namespace(ns_stock)
    api.add_namespace(ns_raw)
    configure_cors(app)
    jwt.init_app(app)

    app.config.from_object(JWTConfig)

    # Define a list of table names
    table_names = ["global_stock_most_active", "local_stock_summary", "global_bond", "global_stock", "crypto",
                   "global_exchange_rate", "global_raw_materials"]  # Replace with your table names
    table_historical = ["global_exchange_rate_historical_data", "local_stock_historical_data", "crypto_historical",
                        "raw_materials_detail", "local_raw_materials", "local_exchange"]

    # Define a function to update the status in multiple tables
    def update_status_in_multiple_tables():
        cursor = conn.cursor()

        update_query = f"""
                           UPDATE global_stock_most_active
                           SET status = 'published'
                           WHERE status = 'unpublished';
                           """
        cursor.execute(update_query)
        conn.commit()

        update_query = f"""
                            UPDATE local_stock_summary
                            SET status = 'published'
                            WHERE status = 'unpublished';
                            """
        cursor.execute(update_query)
        conn.commit()

        update_query = f"""
                            UPDATE global_bond
                            SET status = 'published'
                            WHERE status = 'unpublished';
                            """
        cursor.execute(update_query)
        conn.commit()

        update_query = f"""
                            UPDATE global_stock
                            SET status = 'published'
                            WHERE status = 'unpublished';
                            """
        cursor.execute(update_query)
        conn.commit()

        update_query = f"""
                            UPDATE crypto
                            SET status = 'published'
                            WHERE status = 'unpublished';
                            """
        cursor.execute(update_query)
        conn.commit()

        update_query = f"""
                            UPDATE global_exchange_rate
                            SET status = 'published'
                            WHERE status = 'unpublished';
                            """
        cursor.execute(update_query)
        conn.commit()

        update_query = f"""
                            UPDATE global_raw_material
                            SET status = 'published'
                            WHERE status = 'unpublished';
                            """
        cursor.execute(update_query)
        conn.commit()
        # for table_name in table_names:
        #     # Find records with status "unpublished" and update them to "published" in each table
        #     update_query = f"""
        #     UPDATE {table_name}
        #     SET status = 'published'
        #     WHERE status = 'unpublished';
        #     """
        #     cursor.execute(update_query)
        #     conn.commit()

        cursor.close()

    def update_status_in_multiple_tables_historical_data():
        cursor = conn.cursor()

        for table_name in table_historical:
            # Find records with status "unpublished" and update them to "published" in each table
            update_query = f"""
                    UPDATE {table_name}
                    SET status = 'published'
                    WHERE status = 'unpublished';
                    """
            cursor.execute(update_query)
            conn.commit()

        cursor.close()

    # Define a function to run all your spiders
    def run_all_spiders():

        spider_list = [MostActiveSpider, StockTradeSummarySpider, GlobalBondSpider, FinanceDataSpider, CryptoSpider,
                       GlobalExchangeRate, RawmatSpider]  # Replace with your spider classes
        # spider_list = [FinanceDataSpider]

        for spider in spider_list:
            scrapydo.run_spider(spider)
            print("spider ", spider)

        # After all spiders are finished, update the status
        update_status_in_multiple_tables()

    def run_all_spiders_historical():
        spider_list = [StockHistoricalDataSpider, CryptoHistoricalSpider, GlobalExchangeRateHistorical,
                       RawmatdetailSpider, LocalRawMaterialsSpider,
                       bank]  # Replace with your spider classes

        for spider in spider_list:
            scrapydo.run_spider(spider)
        update_status_in_multiple_tables_historical_data()

    # Define the endpoint to run all spiders and update status in multiple tables
    @app.route('/run_spiders')
    def run_spiders():
        try:
            # Your logic here
            run_all_spiders()
            print('Spiders run successfully.')
            return "Spiders run successfully.", 200
        except Exception as e:
            app.logger.error(f"An error occurred: {str(e)}")
            return "An error occurred while running spiders."

    @app.route('/run_spiders_historical')
    def run_spiders_historical():
        try:
            # Your logic here
            run_all_spiders_historical()
            print('Spiders run successfully.')
            return "Spiders run successfully.", 200
        except Exception as e:
            app.logger.error(f"An error occurred: {str(e)}")
            return "An error occurred while running spiders."

    # Set up the scheduler
    scheduler = BackgroundScheduler()
    scheduler.add_job(lambda: app.test_client().get('/run_spiders'), 'interval',
                      minutes=480)
    scheduler.add_job(lambda: app.test_client().get('/run_spiders_historical'), 'interval',
                      hours=24)
    scheduler.start()

    return app
