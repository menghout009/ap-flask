import datetime
from ..database.connect_db import connect_database
import scrapy
from datetime import datetime as time
import json
import os


class bank(scrapy.Spider):
    name = "bank"

    def __init__(self):
        self.scraped_data = None
        self.conn = connect_database()
        self.cur = self.conn.cursor()

    def start_requests(self):
        url = 'https://www.nbc.gov.kh/english/economic_research/exchange_rate.php'
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        scraped_data = []  # Initialize a list to store scraped data
        current_date_scraped = time.now()
        rows = response.xpath("//table[@class='tbl-responsive']/tr")
        usd = response.xpath("//div[@class='content-text']/form/table/tr[2]/td/font/text()").get()
        currency_date = response.xpath("//div[@class='content-text']/form/table/tr[1]/td/font/text()").get()
        datepath = response.xpath(
            '//form/table/tbody/tr[3]/td[3]/br/input[@class="required hasDatepicker"]/@value').get()

        # Check if the date already exists for the given category in the table
        check_query = "SELECT COUNT(*) FROM local_exchange WHERE currency_date = %s"
        self.cur.execute(check_query, (currency_date,))
        result = self.cur.fetchone()
        print("result", result)

        if result[0] == 0:
            self.cur.execute(
                "INSERT INTO local_exchange(currency, currency_from, currency_to, unit, buying, sale, medium,currency_date,scraped_date) VALUES ("
                "%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                ("United States Dollar", "USD", "KHR", "1", usd, "None", "None", currency_date, current_date_scraped))
            self.conn.commit()

            for row in rows[1:]:  # Skip the first row (header)
                currency = row.xpath("./td[1]/text()").getall()
                currency_rate = row.xpath("./td[2]/text()").getall()
                unit = row.xpath("./td[3]/text()").getall()
                buying = row.xpath("./td[4]/text()").getall()
                sale = row.xpath("./td[5]/text()").getall()
                medium = row.xpath("./td[6]/text()").getall()
                currency_from = [rate.split("/")[0] for rate in currency_rate]
                currency_to = [rate.split("/")[1] for rate in currency_rate]
                current_date = datetime.date.today().strftime('%Y-%m-%d')
                print("ggg", current_date)

                for item in range(len(currency)):
                    self.cur.execute(
                        "INSERT INTO local_exchange(currency, currency_from, currency_to, unit, buying, sale, medium, currency_date,scraped_date) VALUES ("
                        "%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (currency[item], currency_from[item], currency_to[item], unit[item], buying[item], sale[item],
                         medium[item], currency_date, current_date_scraped))
                    self.conn.commit()


        else:
            print(f"Data for date {currency_date} already exists in the table, skipping insertion.")
        # else:
        #     print(f"Data for date {currency_date} already exists in the table, skipping insertion.")

        for row in rows[1:]:  # Skip the first row (header)
            currency = row.xpath("./td[1]/text()").getall()
            currency_rate = row.xpath("./td[2]/text()").getall()
            unit = row.xpath("./td[3]/text()").getall()
            buying = row.xpath("./td[4]/text()").getall()
            sale = row.xpath("./td[5]/text()").getall()
            medium = row.xpath("./td[6]/text()").getall()
            currency_from = [rate.split("/")[0] for rate in currency_rate]
            currency_to = [rate.split("/")[1] for rate in currency_rate]
            current_date = datetime.date.today().strftime('%Y-%m-%d')
            for item in range(len(currency)):
                datas = [currency[item].lower() + " to khmer", "as of " + currency_date + ", the exchange rate for " +
                         unit[item] + " " + currency[item].lower() + " to khmer unit is " + buying[item] + " for buying and " +
                         sale[item] + " for selling" + ", with a medium rate of " + medium[item]]
                scraped_data.append(datas)

            # Clear the previously scraped data
            self.scraped_data = []

            # Export the newly scraped data
            self.export_data(scraped_data)

    def close_spider(self, spider):
        self.conn.commit()
        self.cur.close()
        self.conn.close()

    def export_data(self, data_to_export):
        output_file = '/data_vue_api/app/chatbot/local_exchange_rate.json'  # Replace with the desired file path

        if os.path.exists(output_file):
            os.remove(output_file)

        # Export the scraped data to the specified JSON file
        with open(output_file, 'w') as f:
            json.dump(data_to_export, f, indent=4)

        self.log(f'Data exported to {output_file}')
