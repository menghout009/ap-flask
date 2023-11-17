from datetime import datetime
import psycopg2
import scrapy
from googletrans import Translator
from ..database.connect_db import connect_database


class GlobalExchangeRate(scrapy.Spider):
    name = "global_exchange_rate"

    def start_requests(self):
        url = "https://kr.investing.com/currencies/streaming-forex-rates-majors"
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        scraped_date = datetime.now()
        translator = Translator()
        pgconn = connect_database()
        cursor = pgconn.cursor()

        rows = response.xpath("//div[@class='dynamic-table_dynamic-table-wrapper__MhGMX']/table/tbody/tr")
        for row in rows:
            event = row.xpath("td[1]/div/a/h4/span/text()").get()
            buy = row.xpath("td[2]/text()").get()
            buy = buy.replace(',', '')
            sell = row.xpath("td[3]/text()").get()
            high = row.xpath("td[4]/text()").get()
            low = row.xpath("td[5]/text()").get()
            variance = row.xpath("td[6]/text()").get()
            variancePer = row.xpath("td[7]/text()").get()
            hour = row.xpath("td[8]/span/span/time/text()").get()
            translated_event = translator.translate(event, dest='en').text
            currency_from = translated_event.split("/")[0].upper()
            currency_to = translated_event.split("/")[1].upper()
            currency_code_from = ''
            currency_code_to = ''
            if currency_from == 'EURO':
                currency_code_from = 'EUR'
            elif currency_from == 'DOLLAR':
                currency_code_from = 'USD'
            elif currency_from == 'AUSTRALIAN DOLLAR':
                currency_code_from = 'AUD'
            elif currency_from == 'NEW ZEALAND DOLLAR':
                currency_code_from = 'NZD'
            elif currency_from == 'POUND':
                currency_code_from = 'GBP'
            elif currency_from == 'SWISS FRANC':
                currency_code_from = 'CHF'
            elif currency_from == 'CANADIAN DOLLAR':
                currency_code_from = 'CAD'
            elif currency_from == 'BITCOIN':
                currency_code_from = 'BTC'
            elif currency_from == 'ETHEREUM':
                currency_code_from = 'ETH'
            else:
                currency_from

            if currency_to == 'DOLLAR':
                currency_code_to = 'USD'
            elif currency_to == 'YEN':
                currency_code_to = 'JPY'
            elif currency_to == 'AUSTRALIAN DOLLAR':
                currency_code_to = 'AUD'
            elif currency_to == 'EURO':
                currency_code_to = 'EUR'
            elif currency_to == 'TURKISH LIRA':
                currency_code_to = 'TRY'
            elif currency_to == 'SWISS FRANC':
                currency_code_to = 'CHF'
            elif currency_to == 'CANADIAN DOLLAR':
                currency_code_to = 'CAD'
            elif currency_to == 'POUND':
                currency_code_to = 'GBP'
            elif currency_to == 'NEW ZEALAND DOLLAR':
                currency_code_to = 'NZD'
            elif currency_to == 'INDIAN LUFFY':
                currency_code_to = 'IDR'
            elif currency_to == 'CHINA COMFORT':
                currency_code_to = 'CNY'
            elif currency_to == 'SINGAPORE DOLLAR':
                currency_code_to = 'SGD'
            elif currency_to == 'HONG KONG DOLLAR':
                currency_code_to = 'HKD'
            elif currency_to == 'DENMARK KRONE':
                currency_code_to = 'DKK'
            elif currency_to == 'SWEDEN KRONA':
                currency_code_to = 'SEK'
            elif currency_to == 'RUSSIAN RUBLE':
                currency_code_to = 'RUB'
            elif currency_to == 'ISRAEL SEKEL':
                currency_code_to = 'ILS'
            elif currency_to == 'MEXICO PESOS':
                currency_code_to = 'MXN'
            elif currency_to == 'SOUTH AFRICA RAND':
                currency_code_to = 'ZAR'
            else:
                currency_to

            print('Translated Event:', translated_event)

            if translated_event not in ['Bitcoin/Dollar', 'Ethereum/Dollar', 'Bitcoin/Euro']:
                try:

                    # Insert new data into the table
                    cursor.execute("""INSERT INTO global_exchange_rate (event, currency_from, currency_to, currency_code_from, currency_code_to,buy,sell,high_price,low_price,variance,
                    variance_per,time,scraped_date) VALUES ( %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                                   (translated_event, currency_from, currency_to, currency_code_from, currency_code_to,
                                    buy, sell, high, low, variance, variancePer, hour, scraped_date))
                    pgconn.commit()

                    data = {
                        'Event': event,
                        'currency_from': currency_from,
                        'currency_to': currency_to,
                        'Buy': buy,
                        'sell': sell,
                        'high price': high,
                        'low price': low,
                        'variance': variance,
                        'variance %': variancePer,
                        'hour': hour,
                    }

                    yield data

                except Exception as e:
                    print(f"An error occurred: {e}")

            # pgconn.close()
