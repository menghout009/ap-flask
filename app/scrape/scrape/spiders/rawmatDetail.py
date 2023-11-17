import json
import os

import scrapy
from ..database.connect_db import connect_database
from datetime import datetime as time
import functools

class RawmatdetailSpider(scrapy.Spider):
    name = "rawmatDetail"
    scraped_data = []
    existing_data = set()

    def start_requests(self):
        url = 'https://kr.investing.com/commodities/'
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        current_date_scrape = time.now()
        link = response.xpath("//*[@id='dailyTab']/tbody/tr/td/a/@href").getall()
        complete_links = [link + "-historical-data" for link in link]
        for links in complete_links:
            callback = functools.partial(self.navigateLink, current_date_scrape=current_date_scrape)
            yield response.follow(links, callback=callback)  # Pass it as a parameter

    def navigateLink(self, response, current_date_scrape):
        date = response.xpath(
            "//div[@class='flex flex-col items-start p-0 md:pl-1 overflow-x-auto mt-6']/table/tbody/tr/td/time/text()"
        ).getall()
        close_price = response.xpath(
            "//div[@class='flex flex-col items-start p-0 md:pl-1 overflow-x-auto mt-6']/table/tbody/tr/td[contains("
            "@class, 'datatable_cell__LJp3C datatable_cell--align-end__qgxDQ datatable_cell--up__hIuZF text-right "
            "text-sm font-normal leading-5 align-middle min-w-[77px] rtl:text-right text-[#007C32]') or contains("
            "@class, 'datatable_cell__LJp3C datatable_cell--align-end__qgxDQ datatable_cell--down___c4Fq text-right "
            "text-sm font-normal leading-5 align-middle min-w-[77px] rtl:text-right text-[#D91400]')]/text()").getall()

        close_price_float = [float(price_value.replace(',', '')) for price_value in close_price]

        market_price = response.xpath(
            "//div[@class='flex flex-col items-start p-0 md:pl-1 overflow-x-auto mt-6']/table/tbody/tr/td[3]/text()").getall()
        # Convert market_price to float
        market_price_float = [float(market_price_value.replace(',', '')) for market_price_value in market_price]
        high_price = response.xpath(
            "//div[@class='flex flex-col items-start p-0 md:pl-1 overflow-x-auto mt-6']/table/tbody/tr/td[4]/text()").getall()

        # Convert high_price to float
        high_price_float = [float(high_price_value.replace(',', '')) for high_price_value in high_price]
        low_price = response.xpath(
            "//div[@class='flex flex-col items-start p-0 md:pl-1 overflow-x-auto mt-6']/table/tbody/tr/td[4]/text()").getall()
        # Convert low_price to float
        low_price_float = [float(low_price_value.replace(',', '')) for low_price_value in low_price]

        volume = response.xpath(
            "//div[@class='flex flex-col items-start p-0 md:pl-1 overflow-x-auto mt-6']/table/tbody/tr/td[6]/text()").getall()

        variance_per = response.xpath(
            "//div[@class='flex flex-col items-start p-0 md:pl-1 overflow-x-auto mt-6']/table/tbody/tr/td[7]/text()").getall()

        # Set the category based on the response URL
        if "gold-historical-data" in str(response):
            category = "gold"
        elif "silver-historical-data" in str(response):
            category = "silver"
        elif "copper-historical-data" in str(response):
            category = "copper"
        elif "platinum-historical-data" in str(response):
            category = "platinum"
        elif "brent-oil-historical-data" in str(response):
            category = "Brent oil"
        elif "crude-oil-historical-data" in str(response):
            category = "WTI Yu"
        elif "natural-gas-historical-data" in str(response):
            category = "Natural gas"
        elif "heating-oil-historical-data" in str(response):
            category = "Heating oil"
        elif "us-coffee-c-historical-data" in str(response):
            category = "American Coffee C"
        elif "us-corn-historical-data" in str(response):
            category = "American corn"
        elif "us-wheat-historical-data" in str(response):
            category = "American wheat"
        elif "london-sugar-historical-data" in str(response):
            category = "London sugar"
        else:
            category = "U.S. NO.2"

        for item in range(len(date)):
            if item < len(volume):
                data_to_insert = (
                    date[item],
                    close_price_float[item],
                    market_price_float[item],
                    high_price_float[item],
                    low_price_float[item],
                    category,
                    current_date_scrape,
                    volume[item],
                    variance_per[item]
                )

            conn = connect_database()
            cur = conn.cursor()
            # Check if the date already exists for the given category in the table
            check_query = "SELECT COUNT(*) FROM raw_materials_detail WHERE date = %s AND category = %s"
            cur.execute(check_query, (date[item], category))
            result = cur.fetchone()

            # If the date doesn't exist, insert the data
            if result[0] == 0:
                insert_query = "INSERT INTO raw_materials_detail (date, close_price, market_price,high_price, " \
                               "low_price, category,scraped_date, trading_volumn, variance_per) " \
                               "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                cur.execute(insert_query, data_to_insert)
                # Commit the transaction to save the changes
                conn.commit()
            # else:
            #     print(f"Data for date {date[item]} already exists in the table, skipping insertion.")
            if item < len(volume):
                data_item = [
                    f"raw material {category.lower()} on {date[item]}", "On " + date[item] + ", " + category.lower() + " recorded a price of "
                    + ", with closing price of " + close_price[item] + ". It reached a high of " + high_price[item] + " and a low of "
                    + low_price[item] + ". The trading volume for the day was" + volume[item] + ", with a variance percentage of " + variance_per[item] + "."
                ]
                self.scraped_data.append(data_item)
                print("scraped data", self.scraped_data)
            self.export_data(self.scraped_data)

    def export_data(self, data_to_export):
        output_file = '/data_vue_api/app/chatbot/history.json'

        # Load existing data from the file
        if os.path.exists(output_file):
            with open(output_file, 'r') as f:
                existing_data_list = json.load(f)

                def convert_to_tuple(item):
                    if isinstance(item, list):
                        return tuple(map(convert_to_tuple, item))
                    return item

                existing_data_set = set(map(convert_to_tuple, existing_data_list))
                self.existing_data.update(existing_data_set)

        # Convert data_to_export to tuple for hashability
        data_tuples = tuple(tuple(item) for item in data_to_export)

        # Check if data_to_export already exists
        for data_tuple in data_tuples:
            if data_tuple not in self.existing_data:
                # Append the new data to the existing data
                self.existing_data.add(data_tuple)
                print("&*())data", data_tuple)

        # Write the combined data back to the JSON file
        with open(output_file, 'w') as f:
            json.dump(list(self.existing_data), f, indent=4)

        self.log(f'Data exported and appended to {output_file}')




