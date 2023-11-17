import scrapy
from ..database.connect_db import connect_database
from datetime import datetime
import json
import os


class CambodiaNewsSpider(scrapy.Spider):
    name = "cambodia_news_data"
    scraped_data = []

    def start_requests(self):
        yield scrapy.Request(url="https://asianbankingandfinance.net/market/cambodia", callback=self.parse)

    def parse(self, response):
        links = response.xpath("//div[@class='view-content']/div[@class='item with-border-bottom']/h2/a/@href").getall()
        for link in links:
            absolute_link = response.urljoin(link)
            yield response.follow(absolute_link, callback=self.parse_linked_page)

    def parse_linked_page(self, response):

        scraped_date = datetime.now()
        published = response.xpath("//div[@class='nf-value position-relative']/em/time/text()").get()
        image = response.xpath("//div[@class='nf__image']/img/@src").get()
        title = response.xpath("//h1[@class='nf__title page-header text-black size-37 striptags']/text()").get()
        cleaned_title = ' '.join(title.strip().split())
        description = response.xpath(
            "//div[@class='nf__description']/p/text() | //div[@class='nf__description']/p/span/span/span/span/span/span/text() | //div[@class='nf__description']/p/span/span/span/span/span/span/span/text()").getall()

        cleaned_description = [data.strip() for data in description if data.strip()]
        # Join the cleaned paragraphs into a single string with spaces between paragraphs
        normalized_text_description = ' '.join(cleaned_description)

        conn = connect_database()
        cur = conn.cursor()
        cur.execute("SELECT title FROM cambodia_news WHERE title = %s", (cleaned_title,))
        existing_record = cur.fetchone()

        if not existing_record:
            # Article doesn't exist, insert it into the database
            cur.execute(
                "INSERT INTO cambodia_news (published, title, description, image,scraped_date) VALUES (%s,%s, %s, %s, "
                "%s)",
                (published, cleaned_title, normalized_text_description, image, scraped_date)
            )
            conn.commit()

        data = {
            'published': published,
            'image': image,
            'title': cleaned_title,
            'description': normalized_text_description
        }

        item = ["cambodia news " + cleaned_title, "" + normalized_text_description]

        print("&*item", item)

        self.scraped_data.append(item)

        print("234123 scrape_data", self.scraped_data)
        # self.scraped_data = []
        # Export the newly scraped data
        self.export_data(self.scraped_data)

    def export_data(self, data_to_export):
        # Define the file path for data export
        output_file = '/data_vue_api/app/chatbot/cambodia_news.json'

        existing_data = []
        if os.path.exists(output_file):
            os.remove(output_file)

        # Check if the JSON file already exists
        if os.path.exists(output_file):
            if os.path.exists(output_file):
                os.remove(output_file)
        existing_data.extend(data_to_export)

        # Write the combined data back to the JSON file
        with open(output_file, 'w') as file:
            json.dump(existing_data, file, indent=4)

        self.log(f'Data has been exported to {output_file}')
