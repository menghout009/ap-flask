import scrapy
from ..database.connect_db import connect_database
from datetime import datetime


class StockCSXCurrentIndex(scrapy.Spider):
    name = 'stock_csx_current_index'
    start_urls = ['http://csx.com.kh/data/index/daily.do?MNCD=60101']

    def parse(self, response):
        scraped_date = datetime.now()
        current_index = response.xpath("//div[@id='index_summary']/table[@class='summary']/tr/td[1]/text()").get()
        change = response.xpath("normalize-space(//div[@id='index_summary']/table[@class='summary']/tr/td[2]//text()[normalize-space()])").get()
        change_status = response.xpath("//div[@id='index_summary']/table[@class='summary']/tr/td[2]/span/img/@src").get()
        change_per = response.xpath("normalize-space(//div[@id='index_summary']/table[@class='summary']/tr/td[3]//text()[normalize-space()])").get()
        change_per_status = response.xpath("//div[@id='index_summary']/table[@class='summary']/tr/td[3]/span/img/@src").get()
        # change_per = row.xpath('normalize-space(td[3]//text()[normalize-space()])').get()
        # change_img = row.xpath('td[3]/span/img/@src').get()
        opening = response.xpath("//div[@id='index_summary']/table[@class='summary']/tr/td[4]//text()").get()
        high_price = response.xpath("//div[@id='index_summary']/table[@class='summary']/tr/td[5]//text()").get()
        low_price = response.xpath("//div[@id='index_summary']/table[@class='summary']/tr/td[6]//text()").get()
        trading_volume_share = response.xpath("//div[@id='index_summary']/table[@class='summary']/tr/td[7]//text()").get()
        trading_value_khr = response.xpath("//div[@id='index_summary']/table[@class='summary']/tr/td[8]//text()").get()

        print("change_img:", current_index)

        conn = connect_database()
        cur = conn.cursor()

        # Check if a record with the same year already exists
        # If no record with the same year, insert the data
        insert_data_query = '''
                    INSERT INTO stock_csx_current_index (current_index, change, change_status, change_per, 
                    change_per_status, opening_price, high_price, low_price, trading_volume_shr, trading_value_kdr, scraped_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                '''
        cur.execute(insert_data_query,
                    (current_index, change, change_status, change_per, change_per_status, opening, high_price,
                     low_price, trading_volume_share, trading_value_khr, scraped_date))
        conn.commit()

        yield {
            'id': id,
            'current_index': current_index,
            'change_per': change_per,
            'opening': opening,
            'high_price': high_price,
            'low_price': low_price,
            'trading_volume_share': trading_volume_share
        }
