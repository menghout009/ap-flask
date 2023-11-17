import scrapy
from ..database.connect_db import connect_database
from datetime import datetime
import json
import os



class MostActiveSpider(scrapy.Spider):
    name = "most_active"

    # Define a class-level dictionary to store all scraped data
    def start_requests(self):
        yield scrapy.Request(url="https://www.google.com/finance/markets/most-active", callback=self.parse)

    def parse(self, response):
        scraped_data = []  # Initialize a list to store scraped data
        symbol = response.xpath("//div[@class='iLEcy']/div[@class='rzR5Id ']/div/div[@class='COaKTb']/text()").getall()
        stock = response.xpath("//div[@class='iLEcy']/div[@class='Q8lakc W9Vc1e']/div[@class='ZvmM7']/text()").getall()
        close = response.xpath("//div[@class='Bu4oXd']/div[@class='YMlKec']/text()").getall()
        change = response.xpath(
            "//div[@class='SEGxAb']/div[@class='BAftM']/span[@class='P2Luy Ez2Ioe']/text() | //div[@class='SEGxAb']/div[@class='BAftM']/span[@class='P2Luy Ebnabc']/text()| //div[@class='SEGxAb']/div[@class='BAftM']/span[@class='P2Luy TrEAYc']/text()").getall()
        percentage_change = response.xpath("//div[@class='zWwE1']/div[@class='JwB6zf']/text()").getall()

        conn = connect_database()
        cur = conn.cursor()
        # Get the current date and time
        current_date = datetime.now()

        for item in range(len(stock)):
            print(item)
            cur.execute(
                "INSERT INTO global_stock_most_active (symbol, stock, close_price, change, percentage_change,scraped_date) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                (symbol[item], stock[item], close[item], change[item], percentage_change[item], current_date))

        conn.commit()
        conn.close()

        data = [
            stock,
            close,
            change,
            percentage_change,
        ]
        for i in range(len(stock)):
            item = [stock[i].lower(), stock[i].lower() + " company stock has" +
                    " close price of " + close[i] + " " + "price change: " + change[i] + " percentage_change: " +
                    percentage_change[i]]
            scraped_data.append(item)  # Append the newly scraped data to the list

        # Clear the previously scraped data
        self.scraped_data = []

        # Export the newly scraped data
        self.export_data(scraped_data)

        yield data

    # def closed(self, reason):
    #     self.export_data(data_to_export)

    def export_data(self, data_to_export):
        output_file = '/data_vue_api/app/chatbot/global_stock_most_active.json'  # Replace with the desired file path

        # Delete the old JSON file if it exists
        if os.path.exists(output_file):
            os.remove(output_file)

        # Export the scraped data to the specified JSON file
        with open(output_file, 'w') as f:
            json.dump(data_to_export, f, indent=4)

        self.log(f'Data exported to {output_file}')
