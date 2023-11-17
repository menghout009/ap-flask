import scrapy
from ..database.connect_db import connect_database
from datetime import datetime


class LocalStockToday(scrapy.Spider):
    name = 'local_stock_today'
    start_urls = ['http://csx.com.kh/data/stock/daily.do?MNCD=60202']

    def parse(self, response):
        scraped_date = datetime.now()
        pre_day_price = response.xpath('//*[@id="index_summary"]/table/tr[2]/td[1]/text()').get()
        current_price = response.xpath('normalize-space(//*[@id="index_summary"]/table/tr[2]/td[2]/text()[normalize-space()])').get()
        change = response.xpath('normalize-space(//*[@id="index_summary"]/table/tr[2]/td[3]/text()[normalize-space()])').get()
        change_per = response.xpath('normalize-space(//*[@id="index_summary"]/table/tr[2]/td[4]/text()[normalize-space()])').get()
        opening = response.xpath('normalize-space(//*[@id="index_summary"]/table/tr[2]/td[5]/text()[normalize-space()])').get()
        high = response.xpath('//*[@id="index_summary"]/table/tr[2]/td[6]/text()').get()
        low = response.xpath('//*[@id="index_summary"]/table/tr[2]/td[7]/text()').get()
        trading_volume = response.xpath('//*[@id="index_summary"]/table/tr[2]/td[8]/text()').get()
        trading_value = response.xpath('//*[@id="index_summary"]/table/tr[2]/td[9]/text()').get()


        conn = connect_database()
        cur = conn.cursor()

        insert_data_query = '''
                    INSERT INTO stock_local_today (previous_day_price, current_price, change, change_per, opening_price,
                     high_price, low_price,trading_volumn, trading_value, scraped_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                '''
        cur.execute(insert_data_query,
                    (pre_day_price, current_price, change, change_per, opening, high, low, trading_volume, trading_value,
                     scraped_date))
        conn.commit()
