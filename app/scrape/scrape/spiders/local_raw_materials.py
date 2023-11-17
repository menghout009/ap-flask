import json
import os

import scrapy
from ..database.connect_db import connect_database
from datetime import datetime


class LocalRawMaterialsSpider(scrapy.Spider):
    name = "local_raw_material"
    scraped_data = []

    def start_requests(self):
        yield scrapy.Request(url='https://phnompenh.gov.kh/en/marketing-price/', callback=self.parse)

    def parse(self, response):
        goods = response.xpath("//div[@class='content emergency-number']/table/tr/td[2]/text()").getall()
        unit = response.xpath("//div[@class='content emergency-number']/table/tr/td[3]/text()").getall()
        price = response.xpath("//div[@class='content emergency-number']/table/tr/td[4]/text()").getall()

        conn = connect_database()
        cur = conn.cursor()
        current_date = datetime.now()
        for item in range(len(goods)):
            print("goods:::", item)

            if goods[item] != "Goods":

                items = [goods[item].lower(), goods[item].lower() + " is price " + price[item] + " " + unit[item]]
                self.scraped_data.append(items)

                cur.execute(
                    "INSERT INTO local_raw_materials (goods, unit, price,scraped_date) VALUES (%s,%s,%s,%s)",
                    (goods[item], unit[item], price[item], current_date))
        self.export_data(self.scraped_data)
        conn.commit()
        conn.close()

        data = {
            'Goods': goods,
            'Unit': unit,
            'Price': price
        }

        yield data

        print("$%(() scraped data", self.scraped_data)

    def export_data(self, data_to_export):
        # Specify the file path
        output_file = '/data_vue_api/app/chatbot/local_raw_material.json'

        existing_data = []

        # Check if the JSON file already exists
        if os.path.exists(output_file):
            if os.path.exists(output_file):
                os.remove(output_file)

        # Append the new data to the existing data
        existing_data.extend(data_to_export)

        # Write the combined data back to the JSON file
        with open(output_file, 'w') as f:
            json.dump(existing_data, f, indent=4)

        self.log(f'Data exported to {output_file}')
