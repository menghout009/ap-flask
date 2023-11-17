import functools

import scrapy
from ..database.connect_db import connect_database
from datetime import datetime
import json
import os


class LocalKnowledge(scrapy.Spider):
    name = "csx"

    def start_requests(self):
        url = 'https://phsarhun.com/main#'
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        current_date = datetime.now()
        knowledge1 = response.xpath(
            "//ul[@class='dropdown-menu']/li[@class='dropdown-submenu khmer-text-navbar']/ul[@class='dropdown-menu  "
            "sub-menu']/li/a/@href").getall()
        csc_link = ['https://phsarhun.com' + knowledge1 for knowledge1 in knowledge1]

        for knowledge in csc_link:
            callback = functools.partial(self.parse_page, current_date=current_date)
            yield response.follow(knowledge, callback=callback)

    def parse_page(self, response, current_date):
        scraped_data = []  # Initialize a list to store scraped data
        no = response.xpath("//table/tbody/tr[@class='khmer_text khmer-text-body']/td[1]/text()").getall()
        name = response.xpath("//table/tbody/tr[@class='khmer_text khmer-text-body']/td[2]/a/text()").getall()
        link = response.xpath("//table/tbody/tr[@class='khmer_text khmer-text-body']/td[3]/a/@href").getall()
        advertise = response.xpath("//table/tbody/tr[@class='khmer_text khmer-text-body']/td[4]/text()").getall()
        conn = connect_database()
        cur = conn.cursor()
        for item in range(len(no)):
            # Check if the data already exists
            cur.execute("SELECT * FROM local_knowledge WHERE no = %s AND name = %s", (no[item], name[item]))
            result = cur.fetchone()
            conn.commit()

            # If the data does not exist, insert it into the database
            if result is None:
                if str(response.url) == "https://phsarhun.com/knowledge/document/publication":
                    cur.execute(
                        "INSERT INTO local_knowledge(no,name,link, posted_on,category,scraped_date) VALUES(%s,%s,%s,%s,%s,%s)",
                        (no[item], name[item], 'https://phsarhun.com/' + link[item], advertise[item], "publication",
                         current_date))
                elif str(response.url) == "https://phsarhun.com/knowledge/document/ebook":
                    cur.execute(
                        "INSERT INTO local_knowledge(no,name,link, posted_on,category,scraped_date) VALUES(%s,%s,%s,%s,%s,%s)",
                        (no[item], name[item], 'https://phsarhun.com/' + link[item], advertise[item], "ebook",
                         current_date))
                elif str(response.url) == "https://phsarhun.com/knowledge/document/regulation":
                    cur.execute(
                        "INSERT INTO local_knowledge(no,name,link, posted_on,category,scraped_date) VALUES(%s,%s,%s,%s,%s,%s)",
                        (no[item], name[item], 'https://phsarhun.com/' + link[item], advertise[item], "regulation",
                         current_date))
                elif str(response.url) == "https://phsarhun.com/knowledge/document/seminars":
                    cur.execute(
                        "INSERT INTO local_knowledge(no,name,link, posted_on,category,scraped_date) VALUES(%s,%s,%s,%s,%s,%s)",
                        (no[item], name[item], 'https://phsarhun.com/' + link[item], advertise[item], "seminar",
                         current_date))
                conn.commit()
            yield {
                'no': no,
                'name': name,
                'link': link,
            }
        for item in range(len(no)):
            category = ""
            if str(response.url) == "https://phsarhun.com/knowledge/document/publication":
                category = "publication"
            elif str(response.url) == "https://phsarhun.com/knowledge/document/ebook":
                category = "ebook"
            elif str(response.url) == "https://phsarhun.com/knowledge/document/regulation":
                category = "regulation"
            elif str(response.url) == "https://phsarhun.com/knowledge/document/seminars":
                category = "seminars"

            datas = [name[item], name[item] + " click this link for download information to pdf " +
                     "https://phsarhun.com/" + link[item] + " " + " in category " + category]

            scraped_data.append(datas)  # Append the newly scraped data to the list

            # Clear the previously scraped data
        self.scraped_data = []

        # Export the newly scraped data
        self.export_data(scraped_data)

    def export_data(self, data_to_export):
        output_file = '/data-vue-api/app/chatbot/local_knowledge.json'  # Replace with the desired file path

        # Delete the old JSON file if it exists
        if os.path.exists(output_file):
            os.remove(output_file)

        # Export the scraped data to the specified JSON file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data_to_export, f, ensure_ascii=False, indent=4)

        self.log(f'Data exported to {output_file}')
