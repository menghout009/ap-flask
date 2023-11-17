import scrapy
from ..database.connect_db import connect_database
from datetime import datetime
import json
import os


class CambodiaGDPSpider(scrapy.Spider):
    name = 'cambodia_gdp'
    start_urls = ['https://www.worldometers.info/gdp/cambodia-gdp/']

    def parse(self, response):

        # Initialize a list to store scraped data
        scraped_data = []

        scraped_date = datetime.now()
        for row in response.xpath(
                '//table[@class="table table-striped table-bordered table-hover table-condensed table-list"]/tbody/tr'):
            year = row.xpath('td[1]/text()').get()
            gdp_nominal_str = row.xpath('td[2]//text()').get()
            gdp_real = row.xpath('td[3]//text()').get()
            gdp_change = row.xpath('td[4]//text()').get()
            gdp_per_capita = row.xpath('td[5]//text()').get()
            pop_change = row.xpath('td[6]//text()').get()
            population = row.xpath('td[7]//text()').get()

            gdp_nominal = int(gdp_nominal_str.replace('$', '').replace(',', ''))

            conn = connect_database()
            cur = conn.cursor()

            # Check if a record with the same year already exists
            check_query = "SELECT COUNT(*) FROM cambodia_gdp WHERE year = %s"
            print("+++++++++++++ check_query ++++++++++++", check_query)
            cur.execute(check_query, (year,))
            count = cur.fetchone()[0]

            if count == 0:
                # If no record with the same year, insert the data
                insert_data_query = '''
                            INSERT INTO cambodia_gdp (year, gdp_nominal, gdp_real, gdp_change, gdp_per_capita, pop_change, population,scraped_date)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        '''
                cur.execute(insert_data_query,
                            (year, gdp_nominal, gdp_real, gdp_change, gdp_per_capita, pop_change, population,
                             scraped_date))
                conn.commit()

            yield {
                'Year': year,
                'GDP Nominal': gdp_nominal,
                'GDP Real': gdp_real,
                'GDP Change': gdp_change,
                'GDP Per Capita': gdp_per_capita,
                'Population Change': pop_change,
                'Population': population
            }

            # script

            item = ["cambodia gdp " + year, "Cambodia's nominal GDP in " + year+" " +
                    "is " + gdp_nominal_str + " " + "GDP in real words:" + gdp_real + " " + "GDP Change:" +
                    gdp_change + " " + "GDP Per capita is" + gdp_per_capita + " " + "Population Change:" +
                    pop_change + " " + "in Population:" + population]
            scraped_data.append(item)  # Append the newly scraped data to the list
            #
            # Clear the previously scraped data
            self.scraped_data = []
            # Export the newly scraped data
            self.export_data(scraped_data)


    def export_data(self, data_to_export):
        output_file = 'data_vue_api/app/chatbot/cambodia_gdp.json'  # Replace with the desired file path

        # Delete the old JSON file if it exists
        if os.path.exists(output_file):
            os.remove(output_file)

        # Export the scraped data to the specified JSON file
        with open(output_file, 'w') as f:
            json.dump(data_to_export, f, indent=4)

        self.log(f'Data exported to {output_file}')
