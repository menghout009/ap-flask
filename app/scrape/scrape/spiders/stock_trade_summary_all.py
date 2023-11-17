import scrapy
from ..database.connect_db import connect_database
from datetime import datetime


class StockTradeSummaryAll(scrapy.Spider):
    name = 'stock_trade_summary_all'
    start_urls = ['http://csx.com.kh/data/allboard/listPosts.do?MNCD=60201']

    def parse(self, response):
        scraped_date = datetime.now()
        board = response.xpath('normalize-space(//*[@id="index_summary"]/table[@class="summary"]/tr[2]/td[1]//text()[normalize-space()])').get()
        volume_share = response.xpath('//*[@id="index_summary"]/table[@class="summary"]/tr[2]/td[2]//text()').get()
        value_khr = response.xpath('//*[@id="index_summary"]/table[@class="summary"]/tr[2]/td[3]//text()').get()
        market_cap = response.xpath('normalize-space(//*[@id="index_summary"]/table[@class="summary"]/tr[2]/td[4]//text()[normalize-space()])').get()
        full_market_cap = response.xpath('//*[@id="index_summary"]/table[@class="summary"]/tr[2]/td[5]//text()').get()

        print("board:", board)
        print("scraped_date:", scraped_date)

        conn = connect_database()
        cur = conn.cursor()

        insert_data_query = '''
            INSERT INTO stock_trade_summary_all (board, volume_share, value_khr, market_cap, 
            full_market_cap, scraped_date)
            VALUES (%s, %s, %s, %s, %s, %s)
        '''
        cur.execute(insert_data_query,
                    (board, volume_share, value_khr, market_cap, full_market_cap, scraped_date))
        conn.commit()

        yield {
            'id': int(datetime.timestamp(scraped_date)),  # Generate an identifier
            'board': board,
            'volume_share': volume_share,
            'value_khr': value_khr,
            'market_cap': market_cap,
        }

