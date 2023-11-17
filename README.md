<!-- PROJECT LOGO -->
<br />
<p align="center">

[//]: # (  <a href="http://localhost:5000/swagger-ui/index.html">)

[//]: # (     <img src="https://i.postimg.cc/rw9y9wgW/academate-logo.png" alt="academate-logo" width="210" height="210">)

[//]: # (  </a>)
</p>
<h3 align="center">DataVue</h3>

  <p align="center">
    DataVue is a web-based application that empowered user with comprehensive financial information, real-time market data, and valuable insights.
    <br />
    <br />
    <a href="http://localhost:5000/swagger-ui/index.html">View Demo</a>
  </p>

<!-- TABLE OF CONTENTS -->
<details open="open">
  <summary><h2 style="display: inline-block">Table of Contents</h2></summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#installation">Installation</a>
    </li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
<li><a href="#what-we-have-done">What we have done</a></li>
  </ol>
</details>

<!-- ABOUT THE PROJECT -->

## About The Project

### Built With

- [Python](https://www.python.org/)
- [Flask-RESTX](https://flask-restx.readthedocs.io/en/latest/)
- [Postgresql](https://www.postgresql.org/)

<!-- GETTING STARTED -->

## Getting Started

### Installation

### User need to install
- PYTHON==3.11.4
- Flask==3.0.0
- Flask-Cors==4.0.0
- Flask-JWT-Extended==4.5.3
- Flask-Mail==0.9.1
- flask-restx==1.1.0
- googletrans==4.0.0rc1
- httpcore==0.9.1
- httpx==0.13.3
- passlib==1.7.4
- psycopg2==2.9.8
- Pygments==2.16.1
- PyJWT==2.8.0
- requests==2.31.0
- schedule==1.2.1
- Scrapy==2.11.0
- scrapydo==0.2.2
- selenium==4.13.0
- bcrypt==4.0.1

## Usage
- write <b>flask run</b> to start a Flask application


## Steps to test API

### 1. Authentication
- Step 01 -> /api/v1/auth/register:      register a new user by insert username, email and password
- Step 02 -> /api/v1/auth/login:         login user by username or email with correct password return a token
- Step 03 -> /api/v1/auth/send-email:    send verify code to user's email

### 2. Stock
- Step 04 -> POST /api/v1/stock/global/both:  scraped both global stock current data and most active stock from urls 
(https://www.google.com/finance/markets/indexes, https://www.google.com/finance/markets/most-active) after scraped data, 
it will into table (global_stock, global_stock_most_active) and all the data not published yet
- Step 05 -> PUT /api/v1/stock/global_stock?scraped_date: published scraped data to user
- Step 06 -> GET /api/v1/stock/global_stock: get all global stock current data
- Step 07 -> GET /api/v1/stock/global_stock/published: get all global stock current data that already published
- Step 08 -> GET /api/v1/stock/global_stock/unpublished: get a group of unpublished global stock and group all data by scraped date
- Step 09 -> GET /api/v1/stock/local_stock_symbol: get local stock with name and symbol of name
- Step 10 -> GET /api/v1/stock/local_stock/stock_name_detail: get local stock data with full name and symbol of name

### 3. Raw Materials
- Step 11 -> POST /api/v1/raw_materials/global:  scraped global raw materials current data from urls 
(https://kr.investing.com/commodities/) after scraped data, it will into table (global_raw_materials) and all the data not published yet
- Step 12 -> PUT  /api/v1/raw_materials/global?scraped_date: published scraped data to user
- Step 13 -> GET  /api/v1/raw_materials/global/published: get all global raw materials current data that already published
- Step 14 -> GET  /api/v1/raw_materials/global/unpublished: get a group of unpublished global raw materials and group all data by scraped date
- Step 15 -> POST /api/v1/raw_materials/global/both: scraped global raw materials current data and historical data from urls 
(https://kr.investing.com/commodities/, https://kr.investing.com/commodities/) after scraped data, it will into table (global_raw_materials,raw_materials_details) and all the data not published yet


### 4. Auto Scrape
- Step 16 -> /api/v1/auto_scrape/extract_data: link input url to scrape, method css or xpath, extraction xpath or css selector expressions to target specific elements on the page (e.g., to extract product titles, prices, or descriptions), output will show in json format


### 5. GDP
- Step 17 -> POST /api/v1/gdp/cambodia:  scraped cambodia gdp for each year from urls 
(https://www.worldometers.info/gdp/cambodia-gdp/) after scraped data, it will into table (cambodia_gdp) and all the data not published yet
- Step 18 -> PUT /api/v1/gdp/cambodia?scraped_date: published scraped data to user
- Step 19 -> GET /api/v1/gdp/cambodia: get all cambodia gdp
- Step 20 -> GET /api/v1/gdp/cambodia/published: get all cambodia gdp that already published
- Step 21 -> GET /api/v1/gdp/cambodia/unpublished: get a group of unpublished cambodia gdp and group all data by scraped date


### 6. NEWS
- Step 17 -> POST /api/v1/news/cambodia:  scraped cambodia news from urls 
(https://asianbankingandfinance.net/market/cambodia) after scraped data, it will into table (cambodia_news) and all the data not published yet
- Step 22 -> PUT /api/v1/news/cambodia?scraped_date: published scraped data to user
- Step 23 -> GET /api/v1/news/cambodia: get all cambodia news
- Step 24 -> GET /api/v1/news/cambodia/published: get all cambodia news that already published
- Step 25 -> GET /api/v1/news/cambodia/unpublished: get a group of unpublished cambodia news and group all data by scraped date
- Step 26 -> GET /api/v1/news/cambodia?id : find cambodia new by id

### 7. Exchange Rate
- Step 27 -> POST /api/v1/exchange_rate/calculator/exchange/rate?from=usd&to=khr&amount=2: calculate exchange by input currency name and amount to calculate
- Step 28 -> POST /api/v1/news/cambodia:  scraped cambodia news from urls 
(https://asianbankingandfinance.net/market/cambodia) after scraped data, it will into table (cambodia_news) and all the data not published yet
- Step 29 -> PUT /api/v1/news/cambodia?scraped_date: published scraped data to user
- Step 30 -> GET /api/v1/news/cambodia: get all cambodia news
- Step 31 -> GET /api/v1/news/cambodia/published: get all cambodia news that already published
- Step 32 -> GET /api/v1/news/cambodia/unpublished: get a group of unpublished cambodia news and group all data by scraped date
- Step 33 -> GET /api/v1/news/cambodia?id : find cambodia new by id



### 6. Crypto
- Step 34 -> POST /api/v1/crypto:  scraped crypto from urls (https://www.investing.com/crypto) after scraped data, it will into table (crypto) and all the data not published yet
- Step 35 -> PUT /api/v1/crypto?scraped_date: published scraped data to user
- Step 36 -> GET /api/v1/crypto: get all crypto
- Step 37 -> GET /api/v1/crypto/published: get all crypto that already published
- Step 38 -> GET /api/v1/crypto/unpublished: get a group of unpublished crypto and group all data by scraped date

### 7. Knowledge
- Step 39 -> POST /api/v1/knowledge:  scraped knowledge from urls (https://phsarhun.com/main#) after scraped data, it will into table (local_knowledge) and all the data not published yet
- Step 40 -> PUT /api/v1/knowledge?scraped_date: published scraped data to user
- Step 41 -> GET /api/v1/knowledge: get all knowledge data 
- Step 42 -> GET /api/v1/knowledge/published: get all knowledge data that already published
- Step 43 -> GET /api/v1/knowledge/unpublished: get a group of unpublished knowledge data and group all data by scraped date
- Step 43 -> GET /api/v1/knowledge/<category>: filter knowledge data that already published by category

### 8. Count Scrape
- Step 41 -> GET /api/v1/count_scrape/all: count all a mount of scraping
- Step 42 -> GET /api/v1/count_scrape/category: count all a mount of scraping base on category like a mount of scraping stock and exchange_rate
- Step 43 -> GET /api/v1/count_scrape/part: count all a mount of scraping base on part like a mount of scraping global stock and local stock


### 9. Bond
- Step 39 -> POST /api/v1/bond:  scraped bond from urls (https://kr.investing.com/rates-bonds/) after scraped data, it will into table (global_bond) and all the data not published yet
- Step 40 -> PUT /api/v1/bond?scraped_date: published scraped data to user
- Step 41 -> GET /api/v1/bond: get all bond data 
- Step 42 -> GET /api/v1/bond/published: get all bond data that already published
- Step 43 -> GET /api/v1/bond/unpublished: get a group of unpublished bond data and group all data by scraped date
- Step 43 -> GET /api/v1/bond/<category>: filter bond data that already published by category





| Parameter      | Type         | Description                      |
|:---------------|:-------------|:---------------------------------|
| `scraped_date` | `string`     | using YYYY-MM-DD HH:MM:SS format |
| `id`           | `integer`    | filter news                      |



Account for admin:
Email: pechnary15@gmail.com or UserName : admin password: 123

## What we have done

- /api/v1/auth
- /api/v1/stock
- /api/v1/raw_materials
- /api/v1/auto_scrape
- /api/v1/gdp
- /api/v1/news
- /api/v1/exchange_rate
- /api/v1/crypto
- /api/v1/knowledge
- /api/v1/count_scrape
- /api/v1/bond

