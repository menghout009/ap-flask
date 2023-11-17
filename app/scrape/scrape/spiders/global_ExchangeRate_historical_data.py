import scrapy
from ..database.connect_db import connect_database
from datetime import datetime


class GlobalExchangeRateHistorical(scrapy.Spider):
    name = "history"


    def start_requests(self):
        url = 'https://kr.investing.com/currencies/streaming-forex-rates-majors'
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):

        invest_links = response.xpath(
            "//div[@class='dynamic-table_dynamic-table-wrapper__MhGMX']/table/tbody/tr/td/div["
            "@class='datatable_cell__wrapper__4bnlr']/a/@href").getall()
        complete_link = [invest_links + '-historical-data' for invest_links in invest_links]
        scraped_date = datetime.now()
        for invest_link in complete_link:
            ob_invest_links = response.urljoin(invest_link)
            yield response.follow(ob_invest_links, callback=self.parse_invest, cb_kwargs={'scraped_date': scraped_date})

    def parse_invest(self, response, scraped_date):
        date = response.xpath("//tbody/tr/td/time/text()").getall()
        closing_price = response.xpath("//tbody/tr/td[@class='datatable_cell__LJp3C datatable_cell--align-end__qgxDQ "
                                       "datatable_cell--down___c4Fq' or @class='datatable_cell__LJp3C "
                                       "datatable_cell--align-end__qgxDQ datatable_cell--up__hIuZF']/text()").getall()
        market_price = response.xpath("//tbody/tr/td[@class='datatable_cell__LJp3C "
                                      "datatable_cell--align-end__qgxDQ']/text()").getall()
        high_price = response.xpath(
            "//table[@class='datatable_table__DE_1_ datatable_table--border__XOKr2 "
            "datatable_table--mobile-basic__rzXxT datatable_table--freeze-column__XKTDf']/tbody["
            "@class='datatable_body__tb4jX']/tr[@class='datatable_row__Hk3IV']/td[4]/text()").getall()

        low_price = response.xpath("//table[@class='datatable_table__DE_1_ datatable_table--border__XOKr2 "
                                   "datatable_table--mobile-basic__rzXxT "
                                   "datatable_table--freeze-column__XKTDf']/tbody[@class='datatable_body__tb4jX']//tr["
                                   "@class='datatable_row__Hk3IV']/td[5]/text()").getall()
        variance_per = response.xpath("//table[@class='datatable_table__DE_1_ datatable_table--border__XOKr2 "
                                      "datatable_table--mobile-basic__rzXxT "
                                      "datatable_table--freeze-column__XKTDf']/tbody["
                                      "@class='datatable_body__tb4jX']//tr["
                                      "@class='datatable_row__Hk3IV']/td[7]/text()").getall()
        print("response link", response.url)
        # Create a cursor object
        conn = connect_database()
        cursor = conn.cursor()
        for item in range(len(date)):
            if str(response.url) == "https://kr.investing.com/currencies/eur-usd-historical-data":
                category = "Euro/dollar"
            elif str(response.url) == "https://kr.investing.com/currencies/usd-jpy-historical-data":
                category = "Dollar/yen"
            elif str(response.url) == " https://kr.investing.com/currencies/gbp-usd-historical-data":
                category = "Pound/dollar"
            elif str(response.url) == "https://kr.investing.com/currencies/usd-try-historical-data":
                category = "Dollar/Turkish Lira"
            elif str(response.url) == "https://kr.investing.com/currencies/usd-chf-historical-data":
                category = "Dollar/Swiss franc"
            elif str(response.url) == "https://kr.investing.com/currencies/usd-cad-historical-data":
                category = "Dollar/Canadian dollar"
            elif str(response.url) == "https://kr.investing.com/currencies/eur-jpy-historical-data":
                category = "Euro/Yen"
            elif str(response.url) == "https://kr.investing.com/currencies/aud-usd-historical-data":
                category = "Australian dollar/dollar"
            elif str(response.url) == "https://kr.investing.com/currencies/nzd-usd-historical-data":
                category = "New Zealand Dollar/Dollar"
            elif str(response.url) == "https://kr.investing.com/currencies/eur-gbp-historical-data":
                category = "Euro/Pound"
            elif str(response.url) == "https://kr.investing.com/currencies/eur-chf-historical-data":
                category = "Euro/Swiss franc"
            elif str(response.url) == "https://kr.investing.com/currencies/aud-jpy-historical-data":
                category = "Australian dollar/yen"
            elif str(response.url) == "https://kr.investing.com/currencies/gbp-jpy-historical-data":
                category = "Pound/yen"
            elif str(response.url) == "https://kr.investing.com/currencies/chf-jpy-historical-data":
                category = "Swiss franc/yen"
            elif str(response.url) == "https://kr.investing.com/currencies/eur-cad-historical-data":
                category = "Euro/Canadian dollar"
            elif str(response.url) == "https://kr.investing.com/currencies/aud-cad-historical-data":
                category = "Australian dollar/Canadian dollar"
            elif str(response.url) == "https://kr.investing.com/currencies/cad-jpy-historical-data":
                category = "Canadian dollar/yen"
            elif str(response.url) == "https://kr.investing.com/currencies/nzd-jpy-historical-data":
                category = "New Zealand dollar/yen"
            elif str(response.url) == "https://kr.investing.com/currencies/aud-nzd-historical-data":
                category = "Australian Dollar/New Zealand Dollar"
            elif str(response.url) == "https://kr.investing.com/currencies/gbp-aud-historical-data":
                category = "Pound/Australian dollar"
            elif str(response.url) == "https://kr.investing.com/currencies/eur-aud-historical-data":
                category = "Euro/Australian dollar"
            elif str(response.url) == "https://kr.investing.com/currencies/gbp-chf-historical-data":
                category = "Pound/Swiss franc"
            elif str(response.url) == "https://kr.investing.com/currencies/eur-nzd-historical-data":
                category = "Euro/New Zealand Dollar"
            elif str(response.url) == "https://kr.investing.com/currencies/aud-chf-historical-data":
                category = "Australian dollar/Swiss franc"
            elif str(response.url) == "https://kr.investing.com/currencies/usd-inr-historical-data":
                category = "Dollar/Indian Luffy"
            elif str(response.url) == "https://kr.investing.com/currencies/usd-cny-historical-data":
                category = "Dollar/China comfort"
            elif str(response.url) == "https://kr.investing.com/currencies/usd-sgd-historical-data":
                category = "Dollar/Singapore Dollar"
            elif str(response.url) == "https://kr.investing.com/currencies/usd-hkd-historical-data":
                category = "Dollar/Hong Kong Dollar"
            elif str(response.url) == "https://kr.investing.com/currencies/usd-dkk-historical-data":
                category = "Dollar/Denmark Krone"
            elif str(response.url) == "https://kr.investing.com/currencies/gbp-cad-historical-data":
                category = "Pound/Canadian dollar"
            elif str(response.url) == "https://kr.investing.com/currencies/usd-sek-historical-data":
                category = "Dollar/Sweden Krona"
            elif str(response.url) == "https://kr.investing.com/currencies/usd-rub-historical-data":
                category = "Dollar/Russian Ruble"
            elif str(response.url) == "https://kr.investing.com/currencies/usd-ils-historical-data":
                category = "Dollar/Israel Sekel"
            elif str(response.url) == "https://kr.investing.com/currencies/usd-mxn-historical-data":
                category = "Dollar/Mexico Pesos"
            elif str(response.url) == "https://kr.investing.com/currencies/usd-zar-historical-data":
                category = "Dollar/South Africa Rand"
            elif str(response.url) == "https://kr.investing.com/currencies/cad-chf-historical-data":
                category = "Canadian dollar/Swiss franc"
            elif str(response.url) == "https://kr.investing.com/currencies/nzd-cad-historical-data":
                category = "New Zealand Dollar/Canadian Dollar"
            elif str(response.url) == "https://kr.investing.com/currencies/nzd-chf-historical-data":
                category = "New Zealand Dollar/Swiss Franc"
            elif str(response.url) == "https://kr.investing.com/currencies/gbp-nzd-historical-data":
                category = "Pound/New Zealand Dollar"


            try:
                closing_price_float = float(closing_price[item].replace(',', ''))
            except ValueError:
                closing_price_float = None

            data_to_insert = (
                date[item],
                closing_price_float,
                market_price[item],
                high_price[item],
                low_price[item],
                variance_per[item],
                category,
                scraped_date
            )
            # yield data_to_insert

            check_query = "SELECT COUNT(*) FROM global_exchange_rate_historical_data  WHERE date = %s AND category = %s"
            cursor.execute(check_query, (date[item], category))
            result = cursor.fetchone()

            if result[0] == 0:
                insert_query = (
                    "INSERT INTO global_exchange_rate_historical_data (date,closing_price,market_price,high_price,"
                    "low_price,variant_per,category,scraped_date) VALUES ("
                    "%s,%s,%s,%s,%s,%s,%s,%s)")
                cursor.execute(insert_query, data_to_insert)
                conn.commit()
