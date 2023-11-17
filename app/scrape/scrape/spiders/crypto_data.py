import scrapy
from ..database.connect_db import connect_database
from datetime import datetime
import json
import os
class CryptoSpider(scrapy.Spider):
    name = "crypto"


    def start_requests(self):
        yield scrapy.Request(url="https://www.investing.com/crypto/", callback=self.parse)

    def parse(self, response):
        scraped_data = []  # Initialize a list to store scraped data
        scraped_date = datetime.now()
        no = response.xpath("//tbody/tr/td[1]/text()").getall()
        name = response.xpath("//tbody/tr/td[3]/a/text()").getall()
        symbol = response.xpath("//tbody/tr/td[4]/text()").getall()
        price = response.xpath("//tbody/tr/td[5]/a/text()").getall()
        market_cap = response.xpath("//tbody/tr/td[6]/text()").getall()
        vol = response.xpath("//tbody/tr/td[7]/text()").getall()
        total_vol = response.xpath("//tbody/tr/td[8]/text()").getall()
        chg_24h = response.xpath("//tbody/tr/td[9]/text()").getall()
        ch_7d = response.xpath("//tbody/tr/td[10]/text()").getall()

        name = [item.replace('\n', '') for item in name]
        symbol = [item.replace('\n', '') for item in symbol]
        market_cap = [item.replace('\n', '') for item in market_cap]
        vol = [item.replace('\n', '') for item in vol]
        chg_24h = [item.replace('\n', '') for item in chg_24h]
        ch_7d = [item.replace('\n', '') for item in ch_7d]

        for item in range(len(no)):
            data_insert = (
                no[item], name[item], symbol[item], price[item], market_cap[item], vol[item], total_vol[item],
                chg_24h[item], ch_7d[item], scraped_date)

            conn = connect_database()
            cursor = conn.cursor()
            insert_query = (
                "INSERT INTO crypto (no,name,symbol,price,market_cap,vol,total_vol,chg_24,chg_7d,scraped_date) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            )
            cursor.execute(insert_query, data_insert)
            conn.commit()

            datas = [name[item].lower() + " ", name[item].lower() +
                    " symbol " + symbol[item] + " " + "is currently trading at " + price[item] + ", " + "with a market capitalization of " +
                    market_cap[item] + ". " + "The daily trading volume is " + vol[item] + ", " + "which accounts for  " + total_vol[item] + " of the total volume. " + "Bitcoin has gained " + chg_24h[item] + " " + "in the past 24 hours and " + ch_7d[item] + " in the past 7 days."]
            scraped_data.append(datas)  # Append the newly scraped data to the list

        # Clear the previously scraped data
        self.scraped_data = []

        # Export the newly scraped data
        self.export_data(scraped_data)

    def export_data(self, data_to_export):
        output_file = '/data_vue_api/app/chatbot/global_crypto.json'  # Replace with the desired file path

        # Delete the old JSON file if it exists
        if os.path.exists(output_file):
            os.remove(output_file)

        # Export the scraped data to the specified JSON file
        with open(output_file, 'w') as f:
            json.dump(data_to_export, f, indent=4)

        self.log(f'Data exported to {output_file}')

