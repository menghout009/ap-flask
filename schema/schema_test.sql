create database data_vue;


CREATE TABLE app_user(
    id serial primary key unique,
    email varchar,
    username varchar,
    password varchar
);
drop table user_otp;

CREATE TABLE if not exists user_otp (
  id SERIAL PRIMARY KEY unique,
  user_id INTEGER NOT NULL REFERENCES app_user(id) ON UPDATE CASCADE ON DELETE CASCADE,
  code INTEGER NOT NULL,
  created_time TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP + INTERVAL '3 MINUTES')
);



drop function upsert_otp(sender_id INTEGER, sender_code INTEGER);

CREATE OR REPLACE FUNCTION upsert_otp(sender_id INTEGER, sender_code INTEGER)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
  result_id INTEGER;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM user_otp WHERE user_otp.user_id = sender_id) THEN
    INSERT INTO user_otp (user_id, code) VALUES (sender_id, sender_code) RETURNING id INTO result_id;
  ELSE
    UPDATE user_otp SET code = sender_code, created_time = now() WHERE user_otp.user_id = sender_id RETURNING id INTO result_id;
  END IF;

  RETURN result_id;
END;
$$;


CREATE TABLE cambodia_gdp(
    id serial primary key,
    year varchar,
    gdp_nominal numeric,
    gdp_real varchar,
    gdp_change varchar,
    gdp_per_capita varchar,
    pop_change varchar,
    population varchar,
    status varchar default 'unpublished'
);


CREATE TABLE cambodia_news(
    published varchar,
    title varchar,
    description varchar,
    image varchar,
    status varchar default 'unpublished'
);

CREATE TABLE global_news(
    category varchar,
    title varchar,
    image varchar,
    paragraph varchar,
    published varchar,
    update varchar,
    date_scrape timestamp default current_timestamp,
    status boolean default 'unpublished'
);

CREATE TABLE global_stock(
    stock varchar,
    close_price varchar,
    change varchar,
    percentage_change varchar,
    category varchar,
    date timestamp default current_timestamp,
    status varchar default 'unpublished'
);



CREATE TABLE global_stock_most_active(
    stock varchar,
    close_price varchar,
    change varchar,
    percentage_change varchar,
    date timestamp default current_timestamp,
    status varchar default 'unpublished'
);

CREATE TABLE stock_historical_data(
    date varchar,
    close_price integer,
    change integer,
    trading_volume_shr integer,
    trading_volume_khr integer,
    opening integer,
    high integer,
    low integer,
    market_cap integer,
    full_market_cap integer,
    category varchar,
    status varchar default 'unpublished'

);

CREATE TABLE stock_detail(
    symbol varchar,
    company_name varchar,
    date varchar,
    status varchar default 'unpublished'
);

CREATE TABLE raw_materials_detail(
date varchar,
close_price float,
market_price float,
high_price float,
low_price float,
category varchar,
    status varchar default 'unpublished'

);




CREATE TABLE local_raw_materials(
    id serial primary key ,
    goods varchar,
    unit varchar,
    price varchar
);






CREATE TABLE global_stock
(
    stock varchar,
    close_price varchar,
    change varchar,
    percentage_change varchar,
    category varchar,
    date timestamp default current_timestamp,
    status varchar default 'unpublished'
);

CREATE TABLE global_stock_most_active(
    stock varchar,
    close_price varchar,
    change varchar,
    percentage_change varchar,
    date timestamp default current_timestamp,
    status varchar default 'unpublished'
);

CREATE TABLE local_raw_materials(
    id serial primary key ,
    goods varchar,
    unit varchar,
    price varchar,
    status varchar default 'unpublished'
);

CREATE TABLE local_stock_historical_data(
    date varchar,
    close_price integer,
    change integer,
    change_status varchar,
    trading_volume_shr integer,
    trading_volume_khr integer,
    opening integer,
    high integer,
    low integer,
    market_cap integer,
    full_market_cap integer,
    category varchar,
    status varchar default 'unpublished'
);

CREATE TABLE local_stock_summary(
    stock varchar,
    close integer,
    change integer,
    open integer,
    high integer,
    low integer,
    volume_share integer,
    value_khr integer,
    p_e varchar,
    p_b varchar,
    change_status varchar,
    date timestamp default current_timestamp,
    status varchar default 'unpublished'
);


CREATE TABLE raw_materials_detail(
date varchar,
close_price float,
market_price float,
high_price float,
low_price float,
category varchar,
status varchar default 'unpublished'

);


CREATE TABLE stock_detail(
    symbol varchar,
    company_name varchar,
    date varchar,
    status varchar default 'unpublished'
);





create table global_exchange_rate_historical_data(
    his_id serial primary key ,
	date varchar(200),
	closing_price varchar(200),
	market_price varchar(200),
	high_price varchar(200),
	low_price varchar(200),
	variant_per varchar(200),
	category varchar(200),
	date_time TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    status varchar default 'unpublished'
);
create table global_exchange_rate(
    ex_id serial primary key ,
    event varchar(250) not null,
    buy decimal(10,4) ,
    sell decimal(10,4),
    high_price  decimal(10,4),
    low_price  decimal(10,4),
    variance varchar(250),
    variance_per varchar(250),
    time varchar(250),
    status varchar default 'unpublished'
);
create table local_knowledge(
    kn_id serial primary key,
    no varchar(250),
    name varchar(250),
    img varchar(250),
    posted_on varchar(250),
    category varchar(250),
    status varchar default 'unpublished'
);
create table local_exchange(
    id serial primary key,
    currency varchar(250),
    symbol varchar(250),
    unit varchar(250),
    buying varchar(250),
    sale varchar(250),
    medium varchar(250),
    date_time TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    status varchar default 'unpublished'
);





truncate local_exchange restart identity;
truncate local_exchange restart identity;
truncate global_news restart identity;
drop table cambodia_gdp;
truncate table global_exchange_rate_historical_data restart identity;

SELECT COUNT(*) FROM crypto_historical where date = '10/19/2023' and category = 'bitcoin_historical-data'


SELECT * FROM cambodia_news;

UPDATE cambodia_news set status = 'published', published_date = now() where DATE(scraped_date) = current_date and status = 'unpublished';




SELECT scraped_date FROM crypto_historical where status = 'published'
GROUP BY scraped_date
ORDER BY scraped_date DESC;

SELECT scraped_date FROM local_exchange where status = 'unpublished'
                    GROUP BY scraped_date
                    ORDER BY scraped_date DESC;

delete from local_exchange;

