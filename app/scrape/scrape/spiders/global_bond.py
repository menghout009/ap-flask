import json
import os

import scrapy
from ..database.connect_db import connect_database
from datetime import datetime
from googletrans import Translator


class GlobalBondSpider(scrapy.Spider):
    name = "global_bond_data"
    scraped_data = []

    # Define a class-level dictionary to store all scraped data
    def start_requests(self):
        yield scrapy.Request(url="https://kr.investing.com/rates-bonds/", callback=self.parse)

    def parse(self, response):# Initialize a list to store scraped data
        scraped_date = datetime.now()
        # North, Central and South American government bonds
        north_event = response.xpath(
            "//table/tbody/tr[@id='pair_23705' or @id='pair_25275' or @id='pair_24029']/td[2]/a/text()").getall()
        north_bond_yield = response.xpath(
            "//table/tbody/tr[@id='pair_23705' or @id='pair_25275' or @id='pair_24029']/td[3]/text()").getall()
        north_before = response.xpath(
            "//table/tbody/tr[@id='pair_23705' or @id='pair_25275' or @id='pair_24029']/td[4]/text()").getall()
        north_high_price = response.xpath(
            "//table/tbody/tr[@id='pair_23705' or @id='pair_25275' or @id='pair_24029']/td[5]/text()").getall()
        north_low_price = response.xpath(
            "//table/tbody/tr[@id='pair_23705' or @id='pair_25275' or @id='pair_24029']/td[6]/text()").getall()
        north_variance = response.xpath(
            "//table/tbody/tr[@id='pair_23705' or @id='pair_25275' or @id='pair_24029']/td[7]/text()").getall()
        north_variance_percentage = response.xpath(
            "//table/tbody/tr[@id='pair_23705' or @id='pair_25275' or @id='pair_24029']/td[8]/text()").getall()
        north_hour = response.xpath(
            "//table/tbody/tr[@id='pair_23705' or @id='pair_25275' or @id='pair_24029']/td[9]/text()").getall()

        # european government bonds
        european_event = response.xpath(
            "//table/tbody/tr[@id='pair_23693' or @id='pair_23778' or @id='pair_23673' or @id='pair_23806' or @id='pair_23738']/td[2]/a/text()").getall()

        european_bond_yield = response.xpath(
            "//table/tbody/tr[@id='pair_23693' or @id='pair_23778' or @id='pair_23673' or @id='pair_23806' or @id='pair_23738']/td[3]/text()").getall()
        european_before = response.xpath(
            "//table/tbody/tr[@id='pair_23693' or @id='pair_23778' or @id='pair_23673' or @id='pair_23806' or @id='pair_23738']/td[4]/text()").getall()
        european_high_price = response.xpath(
            "//table/tbody/tr[@id='pair_23693' or @id='pair_23778' or @id='pair_23673' or @id='pair_23806' or @id='pair_23738']/td[5]/text()").getall()
        european_low_price = response.xpath(
            "//table/tbody/tr[@id='pair_23693' or @id='pair_23778' or @id='pair_23673' or @id='pair_23806' or @id='pair_23738']/td[6]/text()").getall()
        european_variance = response.xpath(
            "//table/tbody/tr[@id='pair_23693' or @id='pair_23778' or @id='pair_23673' or @id='pair_23806' or @id='pair_23738']/td[7]/text()").getall()
        european_variance_percentage = response.xpath(
            "//table/tbody/tr[@id='pair_23693' or @id='pair_23778' or @id='pair_23673' or @id='pair_23806' or @id='pair_23738']/td[8]/text()").getall()
        european_hour = response.xpath(
            "//table/tbody/tr[@id='pair_23693' or @id='pair_23778' or @id='pair_23673' or @id='pair_23806' or "
            "@id='pair_23738']/td[9]/text()").getall()

        # Asia Pacific Government Bonds

        asia_event = response.xpath(
            "//table/tbody/tr[@id='pair_23901' or @id='pair_23878' or @id='pair_23917']/td[2]/a/text()").getall()

        asia_bond_yield = response.xpath(
            "//table/tbody/tr[@id='pair_23901' or @id='pair_23878' or @id='pair_23917']/td[3]/text()").getall()
        asia_before = response.xpath(
            "//table/tbody/tr[@id='pair_23901' or @id='pair_23878' or @id='pair_23917']/td[4]/text()").getall()
        asia_high_price = response.xpath(
            "//table/tbody/tr[@id='pair_23901' or @id='pair_23878' or @id='pair_23917']/td[5]/text()").getall()
        asia_low_price = response.xpath(
            "//table/tbody/tr[@id='pair_23901' or @id='pair_23878' or @id='pair_23917']/td[6]/text()").getall()
        asia_variance = response.xpath(
            "//table/tbody/tr[@id='pair_23901' or @id='pair_23878' or @id='pair_23917']/td[7]/text()").getall()
        asia_variance_percentage = response.xpath(
            "//table/tbody/tr[@id='pair_23901' or @id='pair_23878' or @id='pair_23917']/td[8]/text()").getall()
        asia_hour = response.xpath(
            "//table/tbody/tr[@id='pair_23901' or @id='pair_23878' or @id='pair_23917']/td[9]/text()").getall()

        translator = Translator()
        # Translate individual elements in the lists
        translated_north_event = [translator.translate(text, dest='en').text for text in north_event]
        translated_european_event = [translator.translate(text, dest='en').text for text in european_event]
        translated_asia_event = [translator.translate(text, dest='en').text for text in asia_event]


        conn = connect_database()
        cur = conn.cursor()

        for item in range(len(translated_north_event)):
            north_item = [translated_north_event[item].lower() + " bond", translated_north_event[item].lower() + " has bond yield " + north_bond_yield[item]
                          + " which has previous bond yield of " + north_before[item] + " high price " + north_high_price[item] + " low price " +
                          north_low_price[item] + " with variance " + north_variance[item] + " and variance percentage " + north_variance_percentage[item]]
            self.scraped_data.append(north_item)
            category = "north"
            cur.execute(
                "INSERT INTO global_bond(event, bond_yield, before, high_price, low_price, variance, "
                "variance_percentage, hour, category,scraped_date)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (translated_north_event[item], north_bond_yield[item], north_before[item], north_high_price[item],
                 north_low_price[item], north_variance[item], north_variance_percentage[item], north_hour[item],
                 category, scraped_date))
            conn.commit()
        for item in range(len(translated_european_event)):
            print("european", item)
            category = "european"

            european_item = [translated_european_event[item].lower() + " bond" , translated_european_event[item].lower() + " has bond yield " +
                          european_bond_yield[item] + " which has previous bond yield of " + european_before[item] + " high price "
                          + european_high_price[item] + " low price " + european_low_price[item] + " with variance " + european_variance[item]
                          + " and variance percentage " + european_variance_percentage[item]]
            self.scraped_data.append(european_item)


            cur.execute(
                "INSERT INTO global_bond(event, bond_yield, before, high_price, low_price, variance, "
                "variance_percentage, hour, category,scraped_date)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (translated_european_event[item], european_bond_yield[item], european_before[item],
                 european_high_price[item], european_low_price[item], european_variance[item],
                 european_variance_percentage[item], european_hour[item], category, scraped_date))
        for item in range(len(translated_asia_event)):
            print("asia", item)
            category = "asia"

            european_item = [
                translated_asia_event[item].lower() + " bond" , translated_asia_event[item].lower() + " has bond yield " +
                asia_bond_yield[item] + " which has previous bond yield of " + asia_before[item] + " high price "
                + asia_high_price[item] + " low price " + asia_low_price[item] + " with variance " +
                asia_variance[item]
                + " and variance percentage " + asia_variance_percentage[item]]
            self.scraped_data.append(european_item)

            self.export_data(self.scraped_data)

            cur.execute(
                "INSERT INTO global_bond(event, bond_yield, before, high_price, low_price, variance, "
                "variance_percentage, hour, category,scraped_date)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (translated_asia_event[item], asia_bond_yield[item], asia_before[item], asia_high_price[item],
                 asia_low_price[item], asia_variance[item], asia_variance_percentage[item], asia_hour[item], category,
                 scraped_date))

            # Commit the changes and close the connection
            conn.commit()
            # conn.close()

        print("translated_north_event", translated_north_event)
        print(translated_european_event)
        print(translated_asia_event)

    def export_data(self, data_to_export):
        # Specify the file path
        output_file = '/data_vue_api/app/chatbot/global_bond.json'

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
