import scrapy
from ..database.connect_db import connect_database
from datetime import datetime
import json
import os


class StockTradeSummarySpider(scrapy.Spider):
    name = "stock_trade_summary"

    def start_requests(self):
        yield scrapy.Request(url="http://csx.com.kh/data/allboard/listPosts.do?MNCD=60201", callback=self.parse)

    def parse(self, response):

        # Initialize a list to store scraped data
        scraped_data = []
        #

        stocks = response.xpath(
            "//table[@class='summary']/tbody/tr/td[1]/text() | //table[@class='summary']/tbody/tr/td[1]/div/text()").getall()
        # clean data
        stock_cleaned = [stock_value.replace(',', '').replace(' ', '').replace('\n', '').replace('\t', '') for
                         stock_value in stocks]
        # Remove empty or blank values from stock_cleaned
        stock_cleaned = [value for value in stock_cleaned if value.strip() != '']
        closes = response.xpath("//table[@class='summary']/tbody/tr/td[2]/text()").getall()
        # clean data
        closes_cleaned = [close_value.replace(',', '').replace('\n', '').replace('\t', '') for close_value in closes]
        # Convert close to integers (if it's in thousands, you may need to adjust accordingly)
        integer_close = [int(volume.replace(',', '')) for volume in closes_cleaned]
        changes = response.xpath("//table[@class='summary']/tbody/tr/td[3]/text()").getall()
        # clean data
        changes_cleaned = [changes_value.replace(',', '').replace(' ', '').replace('\n', '').replace('\t', '') for
                           changes_value in changes]
        filtered_data_change = [value.strip() for value in changes_cleaned if value.strip()]
        # Convert change to integers (if it's in thousands, you may need to adjust accordingly)
        integer_change = [int(volume.replace(',', '')) for volume in filtered_data_change]

        # get change_status
        changes_status = response.xpath("//table[@class='summary']/tbody/tr/td[3]/span/img/@src").getall()

        opens = response.xpath("//table[@class='summary']/tbody/tr/td[4]/text()").getall()
        # clean data
        opens_cleaned = [opens_value.replace(',', '').replace('\n', '').replace('\t', '') for opens_value in opens]
        # Convert opens to integers (if it's in thousands, you may need to adjust accordingly)
        # integer_opens = [int(volume.replace(',', '')) for volume in opens_cleaned]
        highs = response.xpath("//table[@class='summary']/tbody/tr/td[5]/text()").getall()
        # Convert highs to integers (if it's in thousands, you may need to adjust accordingly)
        integer_highs = [int(volume.replace(',', '')) for volume in highs]
        low = response.xpath("//table[@class='summary']/tbody/tr/td[6]/text()").getall()
        # Convert low to integers (if it's in thousands, you may need to adjust accordingly)
        integer_low = [int(volume.replace(',', '')) for volume in low]
        volume_share = response.xpath("//table[@class='summary']/tbody/tr/td[7]/text()").getall()
        # Convert close to integers (if it's in thousands, you may need to adjust accordingly)
        integer_volume_share = [int(volume.replace(',', '')) for volume in volume_share]
        value_khr = response.xpath("//table[@class='summary']/tbody/tr/td[8]/text()").getall()
        # Convert value_khr to integers (if it's in thousands, you may need to adjust accordingly)
        integer_value_khr = [int(volume.replace(',', '')) for volume in value_khr]
        p_e = response.xpath("//table[@class='summary']/tbody/tr/td[9]/text()").getall()
        # clean data
        p_e_cleaned = [p_e_value.replace(',', '').replace('\n', '').replace('\t', '') for p_e_value in p_e]
        p_b = response.xpath("//table[@class='summary']/tbody/tr/td[10]/text()").getall()
        # clean data
        p_b_cleaned = [p_b_value.replace(',', '').replace('\n', '').replace('\t', '') for p_b_value in p_b]

        conn = connect_database()
        cur = conn.cursor()
        # Get the current date and time
        current_date = datetime.now()

        for item in range(len(highs)):
            cur.execute(
                "INSERT INTO local_stock_summary (stock, close, change, open, high, low, volume_share,"
                " value_khr,p_e, p_b, change_status,scraped_date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (stock_cleaned[item], integer_close[item], integer_change[item], opens_cleaned[item],
                 integer_highs[item], integer_low[item], integer_volume_share[item], integer_value_khr[item],
                 p_e_cleaned[item], p_b_cleaned[item], changes_status[item], current_date))
            conn.commit()

        data = {
            'stock': stock_cleaned,
            'closes': integer_close,
            'change': integer_change,
            'opens': opens_cleaned,
            'high': integer_highs,
            'low': integer_low,
            'volume_share': integer_volume_share,
            'values_khr': integer_value_khr,
            'p_e': p_e_cleaned,
            'p_b': p_b_cleaned,
        }

        yield data

        for i in range(len(highs)):
            if stock_cleaned[i] == "ABC":
                stock_cleaned[i] = "ACLEDA Bank Plc"
            elif stock_cleaned[i] == "MJQE":
                stock_cleaned[i] = "MENGLY J. QUACH EDUCATION PLC"
            elif stock_cleaned[i] == "CGSM":
                stock_cleaned[i] = "CAMGSM Plc"
            elif stock_cleaned[i] == "JSL":
                stock_cleaned[i] = "JS LAND PLC"
            elif stock_cleaned[i] == "DBDE":
                stock_cleaned[i] = "DBD Engineering Plc"
            elif stock_cleaned[i] == "PEPC":
                stock_cleaned[i] = "Pestech (Cambodia) Plc"
            elif stock_cleaned[i] == "PAS":
                stock_cleaned[i] = "Sihanoukville Autonomous Port"
            elif stock_cleaned[i] == "PPSP":
                stock_cleaned[i] = "Phnom Penh SEZ Plc"
            elif stock_cleaned[i] == "PPAP":
                stock_cleaned[i] = "Phnom Penh Autonomous Port"
            elif stock_cleaned[i] == "GTI":
                stock_cleaned[i] = "Grand Twins International (Cambodia) Plc"
            elif stock_cleaned[i] == "PWSA":
                stock_cleaned[i] = "Phnom Penh Water Supply Authority"
            item = [stock_cleaned[i].lower(), "The stock: " + stock_cleaned[i].lower() + " has " +
                    "close price of " + str(integer_close[i]) + " " + "with change price: " + str(
                integer_change[i]) + ". " + "The opening price is also " +
                    opens_cleaned[i] + ", " + "with a high of " + str(integer_highs[i]) + " " + "and a low of " + str(
                integer_low[i]) + ". " + "The trading volume is " + str(integer_volume_share[i]) + " " + "shares, and the total value traded is " + str(
                integer_value_khr[i]) +
                    " " + "Khmer Riel. The price-to-earnings (P/E) ratio is " + p_e_cleaned[i] + ", " + "and the price-to-book (P/B) ratio is " + p_b_cleaned[i]]
            scraped_data.append(item)  # Append the newly scraped data to the list

        # Clear the previously scraped data
        self.scraped_data = []

        # Export the newly scraped data
        self.export_data(scraped_data)

    def export_data(self, data_to_export):
        output_file = '/data_vue_api/app/chatbot/local_stock.json'  # Replace with the desired file path

        # Delete the old JSON file if it exists
        if os.path.exists(output_file):
            os.remove(output_file)

        # Export the scraped data to the specified JSON file
        with open(output_file, 'w') as f:
            json.dump(data_to_export, f, indent=4)

        self.log(f'Data exported to {output_file}')
