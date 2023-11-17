import scrapy
from ..database.connect_db import connect_database
from datetime import datetime


class StockCSXIndex(scrapy.Spider):
    name = 'stock_csx_index'
    start_urls = ['http://csx.com.kh/data/index/daily.do?MNCD=60101']

    def parse(self, response):
        scraped_date = datetime.now()
        for row in response.xpath('//*[@id="table-index"]/tbody/tr'):
            date = row.xpath('td[1]/text()').get()
            current_index = row.xpath('td[2]//text()').get()
            change_per = row.xpath('normalize-space(td[3]//text()[normalize-space()])').get()
            change_img = row.xpath('td[3]/span/img/@src').get()
            opening = row.xpath('td[4]//text()').get()
            high_price = row.xpath('td[5]//text()').get()
            low_price = row.xpath('td[6]//text()').get()
            trading_volume_share = row.xpath('td[7]//text()').get()
            trading_value_khr = row.xpath('td[8]//text()').get()
            market_cap = row.xpath('td[9]//text()').get()

            print("change_img:", change_per)


            conn = connect_database()
            cur = conn.cursor()

            # Check if a record with the same year already exists
            check_query = "SELECT COUNT(*) FROM stock_csx_index WHERE date = %s"

            cur.execute(check_query, (date,))
            count = cur.fetchone()[0]
            print("+++++++++++++ check_query ++++++++++++", date)
            if count == 0:
                # If no record with the same year, insert the data
                insert_data_query = '''
                            INSERT INTO stock_csx_index (date, current_index, change_per, opening, high_price, low_price, trading_volume_share,
                             market_cap, trading_value_khr, scraped_date, change_status)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        '''
                cur.execute(insert_data_query,
                            (date, current_index, change_per, opening, high_price, low_price, trading_volume_share,
                             market_cap, trading_value_khr, scraped_date,change_img))
                conn.commit()

            yield {
                'date': date,
                'current_index': current_index,
                'change_per': change_per,
                'opening': opening,
                'high_price': high_price,
                'low_price': low_price,
                'trading_volume_share': trading_volume_share
            }