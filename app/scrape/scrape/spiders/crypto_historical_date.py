import json
import os

import scrapy
from datetime import datetime
from ..database.connect_db import connect_database


class CryptoHistoricalSpider(scrapy.Spider):
    name = "crptohistorical"
    scraped_date = datetime.now()
    scraped_data = []
    existing_data = set()

    def start_requests(self):
        yield scrapy.Request(url="https://www.investing.com/crypto/", callback=self.parse)

    def parse(self, response):
        crypto_detial_links = response.xpath(
            '//*[@id="fullColumn"]/div[6]/div[2]/table/tbody/tr/td[3]/a/@href').extract()
        crypto_con = [crypto_detial_links + '/historical-data' for crypto_detial_links in crypto_detial_links]
        for crypto_detial_links in crypto_con:
            ob_crypto = response.urljoin(crypto_detial_links)
            yield response.follow(ob_crypto, callback=self.historical)

    def historical(self, response, scraped_date=scraped_date):
        date = response.xpath("//tbody/tr/td[1]/time/text()").getall()
        price = response.xpath("//tbody/tr/td[2]/text()").getall()
        open = response.xpath("//tbody/tr/td[3]/text()").getall()
        high = response.xpath('//tbody/tr[@class="h-[41px] hover:bg-[#F5F5F5] relative after:absolute after:bottom-0 '
                              'after:bg-[#ECEDEF] after:h-px after:left-0 after:right-0 '
                              'historical-data-v2_price__atUfP"]/td[4]/text()').getall()
        low = response.xpath('//tbody/tr[@class="h-[41px] hover:bg-[#F5F5F5] relative after:absolute after:bottom-0 '
                             'after:bg-[#ECEDEF] after:h-px after:left-0 after:right-0 '
                             'historical-data-v2_price__atUfP"]/td[5]/text()').getall()
        vol = response.xpath('//tbody/tr[@class="h-[41px] hover:bg-[#F5F5F5] relative after:absolute after:bottom-0 '
                             'after:bg-[#ECEDEF] after:h-px after:left-0 after:right-0 '
                             'historical-data-v2_price__atUfP"]/td[6]/text()').getall()
        change = response.xpath('//tbody/tr[@class="h-[41px] hover:bg-[#F5F5F5] relative after:absolute '
                                'after:bottom-0 after:bg-[#ECEDEF] after:h-px after:left-0 after:right-0 '
                                'historical-data-v2_price__atUfP"]/td[7]/text()').getall()
        conn = connect_database()
        cursor = conn.cursor()
        for item in range(len(date)):
            if str(response.url) == "https://www.investing.com/crypto/bitcoin/historical-data":
                category = "Bitcoin"
            elif str(response.url) == "https://www.investing.com/crypto/ethereum/historical-data":
                category = "Ethereum"
            elif str(response.url) == "https://www.investing.com/crypto/tether/historical-data":
                category = "Tether USDt"
            elif str(response.url) == "https://www.investing.com/crypto/bnb/historical-data":
                category = "BNB"
            elif str(response.url) == "https://www.investing.com/crypto/xrp/historical-data":
                category = "XRP"
            elif str(response.url) == "https://www.investing.com/crypto/usd-coin/historical-data":
                category = "USD Coin"
            elif str(response.url) == "https://www.investing.com/crypto/steth/historical-data":
                category = "Lido Staked ETH"
            elif str(response.url) == "https://www.investing.com/crypto/solana/historical-data":
                category = "Solana"
            elif str(response.url) == "https://www.investing.com/crypto/cardano/historical-data":
                category = "Cardano"
            elif str(response.url) == "https://www.investing.com/crypto/dogecoin/historical-data":
                category = "Dogecoin"
            # else:
            #     exit()  # Add this line

            try:
                price_float = float(price[item].replace(',', ''))  # Handle commas in numbers if present
            except ValueError:
                price_float = None
            data_to_insert = (
                date[item],
                price_float,
                open[item],
                high[item],
                low[item],
                vol[item],
                change[item],
                category,
                scraped_date
            )
            print("convert to float", data_to_insert)
            yield data_to_insert

            check_query = "SELECT COUNT(*) FROM crypto_historical WHERE date = %s AND category = %s"
            cursor.execute(check_query, (date[item], category))
            result = cursor.fetchone()

            if result[0] == 0:
                insert_query = ("INSERT INTO crypto_historical(date,price,open,high_price,low_price,volume,change,"
                                "category,scraped_date) VALUES ("
                                "%s,%s,%s,%s,%s,%s,%s,%s,%s)")
                cursor.execute(insert_query, data_to_insert)
                conn.commit()
            else:
                print(f"Data for date {date[item]} already exists in the table, skipping insertion.")

            data_item = [category.lower() + " in " + date[item], category.lower() + " in " + date[item] + " has price at " +  price[item] + " with open price" + open[item] + " high price " + high[item]
                         + " low price "+ low[item] + " volume " + vol[item] + " and volume change is " + change[item]]
            self.scraped_data.append(data_item)

        self.export_data(self.scraped_data)

    def export_data(self, data_to_export):
        output_file = '/data_vue_api/app/chatbot/history.json'

        # Load existing data from the file
        if os.path.exists(output_file):
            with open(output_file, 'r') as f:
                existing_data_list = json.load(f)
                existing_data_set = set(map(tuple, existing_data_list))
                self.existing_data.update(existing_data_set)

        # Convert data_to_export to tuple for hashability
        data_tuples = [tuple(item) for item in data_to_export]

        # Check if data_to_export already exists
        for data_tuple in data_tuples:
            category_date_exists = any(
                existing_tuple for existing_tuple in self.existing_data if existing_tuple[0] == data_tuple[0]
                and existing_tuple[1] == data_tuple[1]
            )
            if category_date_exists:
                self.log(
                    f'Data for category {data_tuple[0]} and date {data_tuple[1]} already exists in the file, skipping append.')
            else:
                # Append the new data to the existing data
                self.existing_data.add(data_tuple)

        # Write the combined data back to the JSON file
        with open(output_file, 'w') as f:
            json.dump(list(self.existing_data), f, indent=4)

        self.log(f'Data exported and appended to {output_file}')

