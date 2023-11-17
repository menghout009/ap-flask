# from flask import Flask
# from apscheduler.schedulers.background import BackgroundScheduler
# from .database.connect_db import connect_database
# from .spiders.most_active import MostActiveSpider
# from .spiders.global_bond import GlobalBondSpider
# from .spiders.stock_trade_summary import StockTradeSummarySpider
# import scrapydo
#
# app = Flask(__name__)
# scrapydo.setup()
#
# # Define the database connection
# db_connection = connect_database()
#
# # Define a list of table names
# table_names = ["global_stock_most_active", "local_stock_summary", "global_bond"]  # Replace with your table names
#
# # Define a function to update the status in multiple tables
# def update_status_in_multiple_tables():
#     cursor = db_connection.cursor()
#
#     for table_name in table_names:
#         # Find records with status "unpublished" and update them to "published" in each table
#         update_query = f"""
#         UPDATE {table_name}
#         SET status = 'published'
#         WHERE status = 'unpublished';
#         """
#         cursor.execute(update_query)
#         db_connection.commit()
#
#     cursor.close()
#
# # Define a function to run all your spiders
# def run_all_spiders():
#     spider_list = [MostActiveSpider, StockTradeSummarySpider, GlobalBondSpider]  # Replace with your spider classes
#
#     for spider in spider_list:
#         scrapydo.run_spider(spider)
#
#     # After all spiders are finished, update the status
#     update_status_in_multiple_tables()
#
# # Define the endpoint to run all spiders and update status in multiple tables
# @app.route('/run_spiders')
# def run_spiders():
#     # run_all_spiders()
#     # return "Spiders run initiated."
#     try:
#         # Your logic here
#         run_all_spiders()
#         print('Spiders run successfully.')
#         return "Spiders run successfully.", 200
#     except Exception as e:
#         app.logger.error(f"An error occurred: {str(e)}")
#         return "An error occurred while running spiders."
#
# # Set up the scheduler
# scheduler = BackgroundScheduler()
# scheduler.add_job(lambda: app.test_client().get('/run_spiders'), 'interval', minutes=2)  # Run all spiders every hour
# scheduler.start()
#
# if __name__ == '__main__':
#     app.run()
