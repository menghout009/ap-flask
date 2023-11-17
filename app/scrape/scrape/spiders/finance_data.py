import scrapy
from ..database.connect_db import connect_database
from datetime import datetime
import json
import os


class FinanceDataSpider(scrapy.Spider):
    name = "finance_data"
    scraped_date = None

    def start_requests(self):
        yield scrapy.Request(url="https://www.google.com/finance/markets/indexes", callback=self.parse)

    def parse(self, response):
        base_url = 'https://www.google.com/finance'
        links = response.xpath("//*[@class='Sy70mc']/div[@class='LTwfK']/span[@class='r6XbWb']/a/@href").getall()
        self.scraped_date = datetime.now()
        # scraped_date = now.strftime("%Y-%m-%d-%H-%M")

        for link in links:
            absolute_link = response.urljoin(link)
            yield response.follow(absolute_link, callback=self.parse_linked_page)

    def parse_linked_page(self, response):
        scraped_data = []  # Initialize a list to store scraped data
        print(self.scraped_date)
        stock = response.xpath(
            "//div[@class='iLEcy']/div[@class='Q8lakc W9Vc1e']/div[@class='ZvmM7']/text()").getall()
        close = response.xpath("//div[@class='Bu4oXd']/div[@class='YMlKec']/text()").getall()
        change = response.xpath("//div[@class='BAftM']/span/text()").getall()
        category = response.xpath("//div[@class='LTwfK']/span[@class='r6XbWb']/text()").getall()
        percentage_change = response.xpath("//div[@class='zWwE1']/div[@class='JwB6zf']/text()").getall()
        # # Establish a connection to your PostgreSQL database
        conn = connect_database()
        cur = conn.cursor()
        # now = datetime.now()
        # scraped_date = now.strftime("%Y-%m-%d-%H-%M")
        # Insert the value into the database
        for item in range(len(stock)):
            # print("scraped_date", scraped_date)
            if str(response) == "<200 https://www.google.com/finance/markets/indexes/asia-pacific>":
                cur.execute(
                    "INSERT INTO global_stock (stock, close_price, change, percentage_change, category,scraped_date)"
                    " VALUES (%s,%s,%s,%s,%s,%s)",
                    (
                        stock[item], close[item], change[item], percentage_change[item], "asia-pacific",
                        self.scraped_date))
            elif str(response) == "<200 https://www.google.com/finance/markets/indexes/americas>":
                cur.execute(
                    "INSERT INTO global_stock (stock, close_price, change,percentage_change, category,scraped_date)"
                    " VALUES (%s,%s,%s,%s,%s,%s)",
                    (stock[item], close[item], change[item], percentage_change[item], "americas", self.scraped_date))
            else:
                cur.execute(
                    "INSERT INTO global_stock (stock, close_price, change,percentage_change, category,scraped_date)"
                    " VALUES (%s,%s,%s,%s,%s,%s)",
                    (stock[item], close[item], change[item], percentage_change[item], "europe-middle-east-africa",
                     self.scraped_date))
        # Commit the changes and close the connection
        conn.commit()
        conn.close()

        data = {
            'Stock': stock,
            'Close': close,
            'Change': change,
            'percentage_change': percentage_change,
            'Category': category
        }
        yield data

        for i in range(len(stock)):
            if str(response) == "<200 https://www.google.com/finance/markets/indexes/asia-pacific>":
                category = "asia-pacific"
            elif str(response) == "<200 https://www.google.com/finance/markets/indexes/americas>":
                category = "americas"
            else:
                category = "europe-middle-east-africa"

            item = [stock[i].lower(), stock[i].lower() + " company stock has" +
                    " close price of " + close[i] + " " + " price change:" + change[i] + " " + "percentage_change:" +
                    percentage_change[i] + " " + " in category: " + category]
            scraped_data.append(item)  # Append the newly scraped data to the list

        # Clear the previously scraped data
        self.scraped_data = []

        # Export the newly scraped data
        self.export_data(scraped_data)

    def export_data(self, data_to_export):
        output_file = '/data_vue_api/app/chatbot/global_stock.json'  # Replace with the desired file path

        # Delete the old JSON file if it exists
        if os.path.exists(output_file):
            os.remove(output_file)

        # Export the scraped data to the specified JSON file
        with open(output_file, 'w') as f:
            json.dump(data_to_export, f, indent=4)

        self.log(f'Data exported to {output_file}')
