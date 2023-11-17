import scrapy
from ..database.connect_db import connect_database
from datetime import datetime as time

class StockDetailDataSpider(scrapy.Spider):
    name = "stock_detail_data"

    def start_requests(self):
        yield scrapy.Request(url="http://csx.com.kh/data/lstcom/listPosts.do?MNCD=60204", callback=self.parse)

    def parse(self, response):
        symbol = response.xpath(
            "//div[@class='board_list']/table[@class='board_list']/tr/td[@class='num']/a/text()").getall()

        company_name = response.xpath(
            "//div[@class='board_list']/table[@class='board_list']/tr/td[@class='title']/a/text()").getall()

        date = response.xpath("//div[@class='board_list']/table[@class='board_list']/tr/td[@class='date']/text()").getall()

        conn = connect_database()
        cur = conn.cursor()
        # print('len date:::::', len(date))
        current_date_scrape = time.now()
        for item in range(len(date)):
            data_to_insert = (
                symbol[item],
                company_name[item],
                date[item],
                current_date_scrape
            )
            # Check if the date already exists for the given category in the table
            check_query = "SELECT COUNT(*) FROM stock_detail WHERE registered_date = %s"
            cur.execute(check_query, (date[item],))
            result = cur.fetchone()
            # If the date doesn't exist, insert the data
            if result[0] == 0:
                insert_query = "INSERT INTO stock_detail (symbol, company_name, registered_date,scraped_date) VALUES (%s,%s,%s,%s)"
                cur.execute(insert_query, data_to_insert)
                # Commit the transaction to save the changes
                conn.commit()
            else:
                print(f"Data for date {date[item]} already exists in the table, skipping insertion.")

            data_item = ["stock "]
        conn.commit()
        # conn.close()

        data = {
            'Symbol': symbol,
            'Company_name': company_name,
            'Date': date
        }

        yield data
