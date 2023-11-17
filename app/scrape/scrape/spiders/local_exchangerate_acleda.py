import scrapy
from ..database.connect_db import connect_database
from datetime import datetime

class ACLEDAExchangeRateSpider(scrapy.Spider):
    name = 'acleda_exchange_rate'
    start_urls = ['https://www.acledabank.com.kh/assets/unity/exchangerate_khm']

    def __init__(self, *args, **kwargs):
        super(ACLEDAExchangeRateSpider, self).__init__(*args, **kwargs)
        self.conn = connect_database()
        self.cur = self.conn.cursor()

    def parse(self, response):
        # Assuming the data is in a table, you can adjust the XPath selector accordingly
        for row in response.xpath('//table[@class="table"]/tr[2]'):
            currency = row.xpath('//td[1]/text()').get()
            buy_rate = row.xpath('//td[2]/text()').get()
            sell_rate = row.xpath('//td[3]/text()').get()
            buy_rate = [str(data.replace(',', '').replace('KHR', '')) for data in buy_rate if
                                       data.strip() and data.strip().replace(',', '').replace('KHR', '').isdigit()]
            buy_rate = ''.join(buy_rate)
            sell_rate = [str(data.replace(',', '').replace('KHR', '').strip()) for data in sell_rate if
                            data.strip() and data.strip().replace(',', '').replace('KHR', '').isdigit()]
            sell_rate = ''.join(sell_rate)

            # Check if the currency and today's date exist in the database
            today_date = datetime.now().strftime("%Y-%m-%d")
            self.cur.execute("SELECT * FROM local_exchange WHERE currency = %s AND scraped_date = %s", (currency, today_date))
            existing_data = self.cur.fetchone()
            current_date = datetime.now()

            # If the data doesn't exist, insert it into the database
            if not existing_data:
                self.cur.execute("INSERT INTO local_exchange (currency, symbol, unit, buying, sale, scraped_date) VALUES (%s, %s, %s, %s, %s, %s)",
                                 ('Khmer Dollar', 'USD/KHR', '1',  buy_rate, sell_rate, current_date))
                self.conn.commit()

            data = {
                'Currency': currency,
                'Buy Rate': buy_rate,
                'Sell Rate': sell_rate,
            }
            yield data

    def close(self, reason):
        self.conn.close()
