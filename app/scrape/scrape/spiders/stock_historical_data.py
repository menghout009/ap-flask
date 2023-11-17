import scrapy
from ..database.connect_db import connect_database
import datetime

from datetime import datetime as time


class StockHistoricalDataSpider(scrapy.Spider):
    name = "stock_historical_data"
    allowed_domains = ["csx.com.kh"]
    start_urls = ["http://csx.com.kh/data/stock"]



    def start_requests(self):
        yield scrapy.Request(url="http://csx.com.kh/data/stock/daily.do?MNCD=60202", callback=self.parse)

    def parse(self, response):
        # List of base URLs
        base_urls = [
            'http://csx.com.kh/data/stock/daily.do?lang=en&MNCD=60202&board_type=M&issueCode=KH1000010004&forma=ALL&fromDate=20230905&toDate=',
            'http://csx.com.kh/data/stock/daily.do?lang=en&MNCD=60202&board_type=M&issueCode=KH1000020003&forma=ALL&fromDate=20230905&toDate=',
            'http://csx.com.kh/data/stock/daily.do?lang=en&MNCD=60202&board_type=M&issueCode=KH1000040001&forma=ALL&fromDate=20230905&toDate=',
            'http://csx.com.kh/data/stock/daily.do?lang=en&MNCD=60202&board_type=M&issueCode=KH1000050000&forma=ALL&fromDate=20230905&toDate=',
            'http://csx.com.kh/data/stock/daily.do?lang=en&MNCD=60202&board_type=M&issueCode=KH1000060009&forma=ALL&fromDate=20230905&toDate=',
            'http://csx.com.kh/data/stock/daily.do?lang=en&MNCD=60202&board_type=M&issueCode=KH1000100003&forma=ALL&fromDate=20230905&toDate=',
            'http://csx.com.kh/data/stock/daily.do?lang=en&MNCD=60202&board_type=M&issueCode=KH1000140009&forma=ALL&fromDate=20230905&toDate=',
            'http://csx.com.kh/data/stock/daily.do?lang=en&MNCD=60202&board_type=M&issueCode=KH1000210000&forma=ALL&fromDate=20230905&toDate=',
            'http://csx.com.kh/data/stock/daily.do?lang=en&MNCD=60202&board_type=M&issueCode=KH1000220009&forma=ALL&fromDate=20230905&toDate=',
            'http://csx.com.kh/data/growthdaily/listPosts.do?lang=en&MNCD=60202&board_type=G&issueCode=KH1000150008&forma=ALL&fromDate=20230905&toDate=',
            'http://csx.com.kh/data/growthdaily/listPosts.do?lang=en&MNCD=60202&board_type=G&issueCode=KH1000160007&forma=ALL&fromDate=20230905&toDate='
            # Add other URLs here
        ]
        current_date_insert = time.now()

        # Get the current date in the format 'yyyymmdd'
        current_date = datetime.date.today().strftime('%Y%m%d')

        # Generate the complete links with the current date
        complete_links = [base_url + current_date for base_url in base_urls]

        for link in complete_links:
            absolute_link = response.urljoin(link)
            yield scrapy.Request(absolute_link, callback=self.parse_linked_page, meta={'current_date_insert': current_date_insert})

    def parse_linked_page(self, response):
        current_date_insert = response.meta['current_date_insert']  # Retrieve from meta
        print("*current_date_insert", current_date_insert)
        global category
        response_text = str(response)
        issue_code = response_text.split("issueCode=")[1].split("&")[0]
        print('split:::', issue_code)
        date = response.xpath("//div[@id='index_table']/table[@class='summary']/tbody/tr/td[1]/text()").getall()
        close_price = response.xpath("//div[@id='index_table']/table[@class='summary']/tbody/tr/td[2]/text()").getall()
        change = response.xpath("//div[@id='index_table']/table[@class='summary']/tbody/tr/td[3]/text()").getall()
        # get change_status
        changes_status = response.xpath("//table[@class='summary']/tbody/tr/td[3]/span/img/@src").getall()
        trading_volume_shr = response.xpath(
            "//div[@id='index_table']/table[@class='summary']/tbody/tr/td[4]/text()").getall()
        trading_volume_khr = response.xpath(
            "//div[@id='index_table']/table[@class='summary']/tbody/tr/td[5]/text()").getall()
        opening = response.xpath("//div[@id='index_table']/table[@class='summary']/tbody/tr/td[6]/text()").getall()
        high = response.xpath("//div[@id='index_table']/table[@class='summary']/tbody/tr/td[7]/text()").getall()
        low = response.xpath("//div[@id='index_table']/table[@class='summary']/tbody/tr/td[8]/text()").getall()
        market_cap_mil_khr = response.xpath(
            "//div[@id='index_table']/table[@class='summary']/tbody/tr/td[9]/text()").getall()
        full_market_cap_mil_khr = response.xpath(
            "//div[@id='index_table']/table[@class='summary']/tbody/tr/td[10]/text()").getall()

        # Convert trading_volume_shr to integers (if it's in thousands, you may need to adjust accordingly)
        trading_volume_shr = [int(volume.replace(',', '')) for volume in trading_volume_shr]

        # Convert trading_volume_khr to integers (if it's in thousands, you may need to adjust accordingly)
        trading_volume_khr = [int(volume.replace(',', '')) for volume in trading_volume_khr]

        # Convert opening to integers
        # opening = [int(open_value.replace(',', '')) for open_value in opening]
        opening = [int(opening.replace(',', '').replace('-', '0').strip()) for opening in opening if
                                   opening.strip() and opening.strip().replace(',', '').replace('-', '0').isdigit()]

        # Convert high to integers
        high = [int(high_value.replace(',', '')) for high_value in high]

        # Convert low to integers
        low = [int(low_value.replace(',', '')) for low_value in low]

        # Convert market_cap_mil_khr to integers (if it's in millions, you may need to adjust accordingly)
        market_cap_mil_khr = [int(market_cap.replace(',', '')) for market_cap in market_cap_mil_khr]

        # Convert full_market_cap_mil_khr to integers (if it's in millions, you may need to adjust accordingly)
        full_market_cap_mil_khr = [int(market_cap.replace(',', '')) for market_cap in full_market_cap_mil_khr]

        int_cleaned_data = [int(data.replace(',', '').strip()) for data in close_price if
                            data.strip() and data.strip().replace(',', '').isdigit()]

        # Clean and convert change to integers (if change is a percentage, remove the '%' sign first)
        int_cleaned_data_change = [int(data.replace(',', '').replace('%', '').strip()) for data in change if
                                   data.strip() and data.strip().replace(',', '').replace('%', '').isdigit()]

        conn = connect_database()
        cur = conn.cursor()

        for item in range(len(int_cleaned_data)):
            if issue_code == "KH1000010004":
                # Define the data to be inserted
                category = "PWSA"
            elif issue_code == "KH1000020003":
                # Define the data to be inserted
                category = "GTI"
            elif issue_code == "KH1000040001":
                # Define the data to be inserted
                category = "PPAP"
            elif issue_code == "KH1000050000":
                # Define the data to be inserted
                category = "PPSP"
            elif issue_code == "KH1000060009":
                # Define the data to be inserted
                category = "PAS"
            elif issue_code == "KH1000100003":
                # Define the data to be inserted
                category = "ABC"
            elif issue_code == "KH1000140009":
                # Define the data to be inserted
                category = "PEPC"
            elif issue_code == "KH1000210000":
                # Define the data to be inserted
                category = "MJQE"
            elif issue_code == "KH1000220009":
                # Define the data to be inserted
                category = "CGSM"
            elif issue_code == "KH1000150008":
                # Define the data to be inserted
                category = "DBDE"
            elif issue_code == "KH1000160007":
                # Define the data to be inserted
                category = "JSL"

            current_date = datetime.date.today().strftime('%Y%m%d')

            data_to_insert = (
                date[item],
                int_cleaned_data[item],
                int_cleaned_data_change[item],
                changes_status[item],
                trading_volume_shr[item],
                trading_volume_khr[item],
                opening[item],
                high[item],
                low[item],
                market_cap_mil_khr[item],
                full_market_cap_mil_khr[item],
                category,
                current_date_insert
            )

            # Check if the date already exists for the given category in the table
            check_query = "SELECT COUNT(*) FROM local_stock_historical_data WHERE date = %s AND category = %s"
            cur.execute(check_query, (date[item], category))
            result = cur.fetchone()

            if result[0] == 0:
                insert_query = "INSERT INTO local_stock_historical_data (date, close_price, change, change_status, " \
                               "trading_volume_shr, trading_volume_khr, opening, high, low, market_cap, " \
                               "full_market_cap, category, scraped_date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                               "%s)"
                cur.execute(insert_query, data_to_insert)

            conn.commit()

        data = {
            'Stock': date,
            'Close_price': int_cleaned_data,
            'change': int_cleaned_data_change,
            'trading_volume_shr': trading_volume_shr,
            'trading_volume_khr': trading_volume_khr,
            'opening': opening,
            'high': high,
            'low': low,
            'market_cap_mil_khr': market_cap_mil_khr,
            'full_market_cap_mil_khr': full_market_cap_mil_khr
        }

        # yield data
