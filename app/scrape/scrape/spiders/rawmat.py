import os
import json
import scrapy
from googletrans import Translator
from ..database.connect_db import connect_database
from datetime import datetime as time


class RawmatSpider(scrapy.Spider):
    name = "rawmat"
    scraped_data = []

    def start_requests(self):
        url = 'https://kr.investing.com/commodities/'
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):

        conn = connect_database()
        cur = conn.cursor()
        translator = Translator()

        current_date_scrape = time.now()

        energy = response.xpath('//*[@id="energy"]/tbody/tr')

        for row in energy:
            goods = row.xpath('.//td[2]/a/text()').get()

            current_price = row.xpath('.//td[4]/text()').get()
            high_price = row.xpath('.//td[6]/text()').get()
            low_price = row.xpath('.//td[7]/text()').get()

            variance = row.xpath('.//td[8]/text()').get()

            variance_per = row.xpath('.//td[9]/text()').get()

            translated_goods = translator.translate(goods, dest='en').text

            energy_data = {
                'goods': translated_goods,
                'current_price': current_price,
                'high_price': high_price,
                'low_price': low_price,
                'variance': variance,
                'variance_per': variance_per
            }
            item1 = [translated_goods.lower(),
                     translated_goods.lower() + ", a type of raw material, is currently priced at " + current_price +
                     ". Its highest recorded price is " + high_price
                     + " while low price is " + low_price + " resulting in a variance of " + variance +
                     " and a variance percentage of" + variance_per]

            self.scraped_data.append(item1)

            insert_query = """
                            INSERT INTO global_raw_material (goods, current_price, high_price, low_price, scraped_date, variance, variance_per)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """
            values = (
                energy_data['goods'], energy_data['current_price'], energy_data['high_price'], energy_data['low_price'],
                current_date_scrape, energy_data['variance'], energy_data['variance_per'])
            cur.execute(insert_query, values)
            conn.commit()

        metals = response.xpath('//*[@id="metals"]/tbody/tr')

        for row in metals:
            goods = row.xpath('.//td[2]/a/text()').get()
            current_price = row.xpath('.//td[4]/text()').get()
            high_price = row.xpath('.//td[6]/text()').get()
            low_price = row.xpath('.//td[7]/text()').get()
            variance = row.xpath('.//td[8]/text()').get()
            variance_per = row.xpath('.//td[9]/text()').get()
            translated_goods = translator.translate(goods, dest='en').text
            metals_data = {
                'goods': translated_goods,
                'current_price': current_price,
                'high_price': high_price,
                'low_price': low_price,
                'variance': variance,
                'variance_per': variance_per
            }

            item2 = [translated_goods.lower(),
                     translated_goods.lower() + ", a type of raw material, is currently priced at " + current_price +
                     ". Its highest recorded price is " + high_price
                     + " while low price is " + low_price + " resulting in a variance of " + variance +
                     " and a variance percentage of" + variance_per]

            self.scraped_data.append(item2)

            insert_query = """
                              INSERT INTO global_raw_material (goods, current_price, high_price, low_price, scraped_date, variance, variance_per)
                             VALUES (%s, %s, %s, %s, %s, %s, %s)
                                    """
            values = (
                metals_data['goods'], metals_data['current_price'], metals_data['high_price'], metals_data['low_price'],
                current_date_scrape, metals_data['variance'], metals_data['variance_per'])
            cur.execute(insert_query, values)
            conn.commit()

        agriculture = response.xpath('//*[@id="agriculture"]/tbody/tr')

        for row in agriculture:
            goods = row.xpath('.//td[2]/a/text()').get()
            current_price = row.xpath('.//td[4]/text()').get()
            high_price = row.xpath('.//td[6]/text()').get()
            low_price = row.xpath('.//td[7]/text()').get()
            variance = row.xpath('.//td[8]/text()').get()
            variance_per = row.xpath('.//td[9]/text()').get()
            translated_goods = translator.translate(goods, dest='en').text
            agriculture_data = {
                'goods': translated_goods,
                'current_price': current_price,
                'high_price': high_price,
                'low_price': low_price,
                'variance': variance,
                'variance_per': variance_per
            }

            item3 = [translated_goods.lower(),
                     translated_goods.lower() + ", a type of raw material, is currently priced at " + current_price +
                     ". Its highest recorded price is " + high_price
                     + " while low price is " + low_price + " resulting in a variance of " + variance +
                     " and a variance percentage of" + variance_per]

            self.scraped_data.append(item3)
            self.export_data(self.scraped_data)


            print("$%(() scraped data",self.scraped_data)

            insert_query = """
                              INSERT INTO global_raw_material (goods, current_price, high_price, low_price, scraped_date, variance, variance_per)
                             VALUES (%s, %s, %s, %s, %s, %s, %s)
                                    """
            values = (
                agriculture_data['goods'], agriculture_data['current_price'], agriculture_data['high_price'],
                agriculture_data['low_price'], current_date_scrape, agriculture_data['variance'],
                agriculture_data['variance_per'])
            cur.execute(insert_query, values)

            conn.commit()

    def export_data(self, data_to_export):
        # Specify the file path
        output_file = '/data_vue_api/app/chatbot/global_raw_material.json'

        existing_data = []

        # Check if the JSON file already exists
        if os.path.exists(output_file):
            # If it exists, read the existing data
            if os.path.exists(output_file):
                os.remove(output_file)

        # Append the new data to the existing data
        existing_data.extend(data_to_export)

        # Write the combined data back to the JSON file
        with open(output_file, 'w') as f:
            json.dump(existing_data, f, indent=4)

        self.log(f'Data exported to {output_file}')