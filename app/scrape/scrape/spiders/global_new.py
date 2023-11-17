import time

import scrapy
from ..database.connect_db import connect_database
from datetime import datetime


class GlobalNewsSpider(scrapy.Spider):
    name = "global_news"

    def start_requests(self):
        yield scrapy.Request(url="https://www.investing.com/news/", callback=self.parse)

    def parse(self, response):
        base_url = 'https://www.investing.com'
        links = response.xpath(
            "//li[@class='selected']/div[@class='navBarDropDown']/ul[@class='main']/li/a/@href").getall()

        for link in links:
            absolute_link = response.urljoin(link)
            yield response.follow(absolute_link, callback=self.category)

    def category(self, response):
        links = response.xpath("//div[@class='textDiv']/a/@href").getall()
        for link in links:
            absolute_link = response.urljoin(link)
            yield response.follow(absolute_link, callback=self.parse_linked_page)

    scraped_date = datetime.now()

    def parse_linked_page(self, response, scraped_date=scraped_date):
        date = datetime.now()
        # category = response.xpath("//div[@class='contentSectionDetails']/a[2]/text()").get()
        title = response.xpath("//h1[@class='articleHeader']/text()").get()
        image = response.xpath("//div[@class='imgCarousel']/img/@src").get()
        paragraphs = response.xpath("//div[@class='WYSIWYG articlePage']/p/text()").getall()
        cleaned_paragraphs = [data.strip() for data in paragraphs if data.strip()]
        # Join the cleaned paragraphs into a single string with spaces between paragraphs
        normalized_text_paragraph = ' '.join(cleaned_paragraphs)
        published = response.xpath("//div[@class='contentSectionDetails']/span[1]/text()").get()
        update = response.xpath("//div[@class='contentSectionDetails']/span[2]/text()").get()
        conn = connect_database()
        cur = conn.cursor()
        parts = str(response).split()

        # Extract the URL part
        if len(parts) >= 2:
            url = parts[1]
            url_parts = url.split('/')

            # Extract the desired part
            desired_part = '/'.join(url_parts[:5])
            stock_market = "https://www.investing.com/news/stock-market-news"
            cryptocurrency = 'https://www.investing.com/news/cryptocurrency-news'
            commodities = "https://www.investing.com/news/commodities-news"
            currencies = "https://www.investing.com/news/forex-news"
            economy = "https://www.investing.com/news/economy"
            breaking_news = "https://www.investing.com/news/headlines"

            if desired_part == stock_market:
                category = "stock"
            elif desired_part == cryptocurrency:
                category = "cryptocurrency"
            elif desired_part == commodities:
                category = "commodities"
            elif desired_part == currencies:
                category = "currencies"
            elif desired_part == economy:
                category = "economy"
            elif desired_part == breaking_news:
                category = "breaking_news"
            else:
                category = 'other'

            if category != "other":
                cur.execute("SELECT title FROM global_news WHERE title = %s", (title,))
                existing_record = cur.fetchone()
                if not existing_record:
                    status = "unpublished"
                    cur.execute(
                        "INSERT INTO global_news (category, title, image, paragraph, published, update, status, "
                        "scraped_date)"
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                        (category, title, image, normalized_text_paragraph, published, update, status, scraped_date)
                    )
                conn.commit()

            data = {
                'category': category,
                'title': title,
                'image': image,
                'paragraph': normalized_text_paragraph,
                'published': published,
                'update': update
            }

            yield data



