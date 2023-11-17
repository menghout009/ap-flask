-- create database

create database data_vue;

-- create table
-- begin
CREATE TABLE app_user(
    id serial primary key unique,
    email varchar not null ,
    username varchar not null ,
    password varchar not null
);
drop table user_otp;

CREATE TABLE if not exists user_otp (
  id SERIAL PRIMARY KEY unique,
  user_id INTEGER NOT NULL REFERENCES app_user(id) ON UPDATE CASCADE ON DELETE CASCADE,
  code INTEGER NOT NULL,
  created_time TIMESTAMP NOT NULL
);


CREATE OR REPLACE FUNCTION upsert_otp(sender_id INTEGER, sender_code INTEGER, time_inserted TIMESTAMP)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
  result_id INTEGER;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM user_otp WHERE user_otp.user_id = sender_id) THEN
    INSERT INTO user_otp (user_id, code, created_time)
    VALUES (sender_id, sender_code,time_inserted)
    RETURNING id INTO result_id;
  ELSE
    UPDATE user_otp
    SET code = sender_code, created_time = time_inserted
    WHERE user_otp.user_id = sender_id
    RETURNING id INTO result_id;
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
    status varchar default 'unpublished',
    scraped_date timestamp
);


CREATE TABLE cambodia_news(
    published varchar,
    title varchar,
    description varchar,
    image varchar,
    scraped_date timestamp default current_timestamp,
    published_date timestamp,
    status varchar default 'unpublished',
    id serial primary key
);



CREATE TABLE global_news(
    category varchar,
    title varchar,
    image varchar,
    paragraph varchar,
    published varchar,
    update varchar,
    date_scrape timestamp default current_timestamp,
    published_date timestamp,
    status varchar default 'unpublished',
    id serial primary key
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
    status varchar default 'unpublished',
    scraped_date timestamp
);

CREATE TABLE stock_detail(
    symbol varchar,
    company_name varchar,
    registered_date varchar,
    status varchar default 'unpublished',
    scraped_date timestamp
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
    scraped_date timestamp default current_timestamp,
    status varchar default 'unpublished'
);

CREATE TABLE local_raw_materials(
    id serial primary key ,
    goods varchar,
    unit varchar,
    price varchar,
    status varchar default 'unpublished',
    scraped_date timestamp default current_timestamp
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
    status varchar default 'unpublished',
    scraped_date timestamp
);

CREATE TABLE local_stock_summary(
    stock varchar,
    close integer,
    change integer,
    open varchar,
    high integer,
    low integer,
    volume_share integer,
    value_khr integer,
    p_e varchar,
    p_b varchar,
    change_status varchar,
    scraped_date timestamp,
    status varchar default 'unpublished'
);


CREATE TABLE raw_materials_detail(
date varchar,
close_price float,
market_price float,
high_price float,
low_price float,
category varchar,
status varchar default 'unpublished',
scraped_date timestamp
);


CREATE TABLE global_raw_materials(
    goods varchar,
    current_price varchar,
    high_price varchar,
    low_price varchar,
    status varchar default 'unpublished',
    scraped_date timestamp
);



create table global_exchange_rate_historical_data(
    id serial primary key ,
	date varchar,
	closing_price float,
	market_price varchar(200),
	high_price varchar(200),
	low_price varchar(200),
	variant_per varchar(200),
	category varchar(200),
	scraped_date TIMESTAMP,
    status varchar default 'unpublished'
);


create table global_exchange_rate(
    id serial primary key ,
    event varchar,
    buy varchar,
    sell varchar,
    high_price varchar,
    low_price  varchar,
    variance varchar,
    variance_per varchar,
    time varchar,
    status varchar default 'unpublished',
    scraped_date timestamp
);
create table local_knowledge(
    kn_id serial primary key,
    no varchar(250),
    name varchar(250),
    img varchar(250),
    posted_on varchar(250),
    category varchar(250),
    status varchar default 'unpublished',
    published_date timestamp,
    scraped_date timestamp
);


create table local_exchange(
    id serial primary key,
    currency varchar(250),
    currency_from varchar(250),
    currency_to varchar(250),
    unit varchar(250),
    buying varchar(250),
    sale varchar(250),
    medium varchar(250),
    currency_date varchar,
    scraped_date timestamp,
    status varchar default 'unpublished',
    currency_fullName varchar default 'cambodian riel'
);

create table crypto(
    id serial primary key,
    no varchar,
    name varchar,
    symbol varchar,
    price varchar,
    market_cap varchar,
    vol varchar,
    total_vol varchar,
    chg_24 varchar,
    chg_7d varchar,
    scraped_date timestamp,
    status varchar default 'unpublished'
);

CREATE TABLE crypto_historical(
    id serial primary key ,
    date  varchar,
    price float,
    open varchar,
    high_price varchar,
    low_price varchar,
    volume varchar,
    change varchar,
    category varchar,
    status varchar default 'unpublished',
    scraped_date timestamp
);

CREATE TABLE global_bond(
event varchar,
bond_yield varchar,
before varchar,
high_price varchar,
low_price varchar,
variance varchar,
variance_percentage varchar,
hour varchar,
category varchar,
status varchar default 'unpublished',
scraped_date timestamp
);

-- end
-- create table


-- trigger for delete old data when update status
-- start trigger

CREATE OR REPLACE FUNCTION Delete_oldData()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM global_bond WHERE status = 'published' and scraped_date < NEW.scraped_date;
    DELETE FROM global_stock_most_active WHERE status = 'published' and scraped_date < NEW.scraped_date;
    DELETE FROM global_stock WHERE status = 'published' and scraped_date < NEW.scraped_date;
    DELETE FROM global_exchange_rate WHERE status = 'published' and scraped_date < NEW.scraped_date;
    DELETE FROM global_raw_materials WHERE status = 'published' and scraped_date < NEW.scraped_date;
    DELETE FROM local_raw_materials WHERE status = 'published' and scraped_date < NEW.scraped_date;
    DELETE FROM local_exchange WHERE status = 'published' and scraped_date < NEW.scraped_date;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER global_bond_trigger
AFTER UPDATE ON global_bond
FOR EACH ROW
EXECUTE FUNCTION Delete_oldData();

CREATE TRIGGER global_stock_most_active_trigger
AFTER UPDATE ON global_stock_most_active
FOR EACH ROW
EXECUTE FUNCTION Delete_oldData();


CREATE TRIGGER global_stock_trigger
AFTER UPDATE ON global_stock
FOR EACH ROW
EXECUTE FUNCTION Delete_oldData();

CREATE TRIGGER global_exchange_rate_trigger
AFTER UPDATE ON global_exchange_rate
FOR EACH ROW
EXECUTE FUNCTION Delete_oldData();

CREATE TRIGGER global_raw_materials_trigger
AFTER UPDATE ON global_raw_materials
FOR EACH ROW
EXECUTE FUNCTION Delete_oldData();


CREATE TRIGGER local_raw_materials_trigger
AFTER UPDATE ON local_raw_materials
FOR EACH ROW
EXECUTE FUNCTION Delete_oldData();


CREATE TRIGGER local_exchange_trigger
AFTER UPDATE ON local_exchange
FOR EACH ROW
EXECUTE FUNCTION Delete_oldData();

-- end trigger


UPDATE global_exchange_rate_historical_data
SET status = 'published'
WHERE scraped_date = '2023-10-24 16:01:11.332734'::timestamp returning *;


CREATE OR REPLACE FUNCTION Delete_globalStock_oldData()
RETURNS TRIGGER AS $$
BEGIN
    -- Check if the 'status' column was updated and the date is earlier than the update date
    IF NEW.status = 'published' AND NEW.scraped_date < OLD.scraped_date THEN
        -- Delete the corresponding row
        DELETE FROM global_stock WHERE status = 'published' AND scraped_date = NEW.scraped_date::timestamp;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- global stock

CREATE OR REPLACE FUNCTION Delete_globalStock_oldData()
RETURNS TRIGGER AS $$
BEGIN
        DELETE FROM global_stock WHERE status = 'published' AND scraped_date < NEW.scraped_date;
        RAISE NOTICE '%s ', NEW.scraped_date;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE TRIGGER global_stock_trigger
BEFORE UPDATE ON global_stock
FOR EACH ROW
EXECUTE FUNCTION Delete_globalStock_oldData();

-- global most active
CREATE OR REPLACE FUNCTION Delete_globalMostActive_oldData()
RETURNS TRIGGER AS $$
BEGIN
        DELETE FROM global_stock_most_active WHERE status = 'published' AND scraped_date < NEW.scraped_date;
        RAISE NOTICE '%s ', NEW.scraped_date;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER stock_most_active_trigger
BEFORE UPDATE ON global_stock_most_active
FOR EACH ROW
EXECUTE FUNCTION Delete_globalMostActive_oldData();

-- local trade summary
CREATE OR REPLACE FUNCTION Delete_LocalTradeSummary_oldData()
RETURNS TRIGGER AS $$
BEGIN
        DELETE FROM local_stock_summary WHERE status = 'published' AND scraped_date < NEW.scraped_date;
        RAISE NOTICE '%s ', NEW.scraped_date;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER local_trade_summary_trigger
BEFORE UPDATE ON local_stock_summary
FOR EACH ROW
EXECUTE FUNCTION Delete_LocalTradeSummary_oldData();

-- global bond
CREATE OR REPLACE FUNCTION Delete_GlobalBond_oldData()
RETURNS TRIGGER AS $$
BEGIN
        DELETE FROM global_bond WHERE status = 'published' AND scraped_date < NEW.scraped_date;
        RAISE NOTICE '%s ', NEW.scraped_date;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER global_bond_trigger
BEFORE UPDATE ON global_bond
FOR EACH ROW
EXECUTE FUNCTION Delete_GlobalBond_oldData();

-- global stock
CREATE OR REPLACE FUNCTION Delete_GlobalStock_oldData()
RETURNS TRIGGER AS $$
BEGIN
        DELETE FROM global_stock WHERE status = 'published' AND scraped_date < NEW.scraped_date;
        RAISE NOTICE '%s ', NEW.scraped_date;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER global_stock_trigger
BEFORE UPDATE ON global_stock
FOR EACH ROW
EXECUTE FUNCTION Delete_GlobalStock_oldData();

-- crypto
CREATE OR REPLACE FUNCTION Delete_Crypto_oldData()
RETURNS TRIGGER AS $$
BEGIN
        DELETE FROM crypto WHERE status = 'published' AND scraped_date < NEW.scraped_date;
        RAISE NOTICE '%s ', NEW.scraped_date;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER crypto_trigger
BEFORE UPDATE ON crypto
FOR EACH ROW
EXECUTE FUNCTION Delete_Crypto_oldData();

-- global exchange rate
CREATE OR REPLACE FUNCTION Delete_GlobalExchangeRate_oldData()
RETURNS TRIGGER AS $$
BEGIN
        DELETE FROM global_exchange_rate WHERE status = 'published' AND scraped_date < NEW.scraped_date;
        RAISE NOTICE '%s ', NEW.scraped_date;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER global_exchange_trigger
BEFORE UPDATE ON global_exchange_rate
FOR EACH ROW
EXECUTE FUNCTION Delete_GlobalExchangeRate_oldData();

-- global raw materials
CREATE OR REPLACE FUNCTION Delete_GlobalRawMaterials_oldData()
RETURNS TRIGGER AS $$
BEGIN
        DELETE FROM global_raw_materials WHERE status = 'published' AND scraped_date < NEW.scraped_date;
        RAISE NOTICE '%s ', NEW.scraped_date;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;



SELECT *
FROM global_exchange_rate
WHERE status = 'published'
AND scraped_date < '2023-10-24 15:32:31.821701'::timestamp;


-- WHERE TO_CHAR(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = '2023-10-24 06:21:46';


SELECT * from global_news where category = 'Stock' and status = 'published';

CREATE TABLE scrape_number(
    id serial primary key,
    news_global integer,
    news_cambodia integer,
    stock_global integer,
    stock_local integer,
    raw_material_global integer,
    raw_material_local integer,
    gdp integer,
    exchange_rate_global integer,
    exchange_rate_local integer,
    crypto integer,
    knowledge integer,
    bond integer
);

INSERT INTO scrape_number(stock_global) values(1);
-- function insert or update number of scrape
-- begin

-- global news
drop function count_scrape_news_global(input_value INT);

CREATE OR REPLACE FUNCTION count_scrape_news_global(input_value INT)
RETURNS INTEGER AS $$
DECLARE
    updated_value INT;
BEGIN
  IF EXISTS (SELECT news_global FROM scrape_number WHERE news_global IS NOT NULL) THEN
    -- If the record exists, update the value column by adding 1
    UPDATE scrape_number
    SET news_global = news_global + 1
    WHERE news_global IS NOT NULL returning news_global INTO updated_value;
    raise notice 'updated_value update: %',updated_value;
  ELSE
    -- If the record does not exist, insert a new row and return the inserted value
    INSERT INTO scrape_number (news_global)
    VALUES (input_value)
    RETURNING news_global INTO updated_value;
    raise notice 'updated_value insert: %',updated_value;
  END IF;

  RETURN updated_value;
END;
$$ LANGUAGE plpgsql;


-- cambodia news

CREATE OR REPLACE FUNCTION count_scrape_news_cambodia(input_value INT)
RETURNS INTEGER AS $$
DECLARE
    updated_value INT;
BEGIN
  IF EXISTS (SELECT news_cambodia FROM scrape_number WHERE news_cambodia IS NOT NULL) THEN
    -- If the record exists, update the value column by adding 1
    UPDATE scrape_number
    SET news_cambodia = news_cambodia + 1
    WHERE news_cambodia IS NOT NULL returning news_cambodia INTO updated_value;
    raise notice 'updated_value update: %',updated_value;
  ELSE
    -- If the record does not exist, insert a new row and return the inserted value
    INSERT INTO scrape_number (news_cambodia)
    VALUES (input_value)
    RETURNING news_cambodia INTO updated_value;
    raise notice 'updated_value insert: %',updated_value;
  END IF;

  RETURN updated_value;
END;
$$ LANGUAGE plpgsql;


-- count scrape global stock

CREATE OR REPLACE FUNCTION count_scrape_stock_global(input_value INT)
RETURNS INTEGER AS $$
DECLARE
    updated_value INT;
BEGIN
  IF EXISTS (SELECT stock_global FROM scrape_number WHERE scrape_number.stock_global IS NOT NULL) THEN
    -- If the record exists, update the value column by adding 1
    UPDATE scrape_number
    SET stock_global = stock_global + 1
    WHERE scrape_number.stock_global IS NOT NULL returning stock_global INTO updated_value;
    raise notice 'updated_value update: %',updated_value;
  ELSE
    -- If the record does not exist, insert a new row and return the inserted value
    INSERT INTO scrape_number (stock_global)
    VALUES (input_value)
    RETURNING stock_global INTO updated_value;
    raise notice 'updated_value insert: %',updated_value;
  END IF;

  RETURN updated_value;
END;
$$ LANGUAGE plpgsql;


-- count scrape local stock

CREATE OR REPLACE FUNCTION count_scrape_stock_local(input_value INT)
RETURNS INTEGER AS $$
DECLARE
    updated_value INT;
BEGIN
  IF EXISTS (SELECT stock_local FROM scrape_number WHERE stock_local IS NOT NULL) THEN
    -- If the record exists, update the value column by adding 1
    UPDATE scrape_number
    SET stock_local = stock_local + 1
    WHERE stock_local IS NOT NULL returning stock_local INTO updated_value;
    raise notice 'updated_value update: %',updated_value;
  ELSE
    -- If the record does not exist, insert a new row and return the inserted value
    INSERT INTO scrape_number (stock_local)
    VALUES (input_value)
    RETURNING stock_local INTO updated_value;
    raise notice 'updated_value insert: %',updated_value;
  END IF;

  RETURN updated_value;
END;
$$ LANGUAGE plpgsql;

-- count scrape global materials

CREATE OR REPLACE FUNCTION count_scrape_rawmaterials_global(input_value INT)
RETURNS INTEGER AS $$
DECLARE
    updated_value INT;
BEGIN
  IF EXISTS (SELECT raw_material_global FROM scrape_number WHERE raw_material_global IS NOT NULL) THEN
    -- If the record exists, update the value column by adding 1
    UPDATE scrape_number
    SET raw_material_global = raw_material_global + 1
    WHERE raw_material_global IS NOT NULL returning raw_material_global INTO updated_value;
    raise notice 'updated_value update: %',updated_value;
  ELSE
    -- If the record does not exist, insert a new row and return the inserted value
    INSERT INTO scrape_number (raw_material_global)
    VALUES (input_value)
    RETURNING raw_material_global INTO updated_value;
    raise notice 'updated_value insert: %',updated_value;
  END IF;

  RETURN updated_value;
END;
$$ LANGUAGE plpgsql;


-- count scrape local materials

CREATE OR REPLACE FUNCTION count_scrape_rawmaterials_local(input_value INT)
RETURNS INTEGER AS $$
DECLARE
    updated_value INT;
BEGIN
  IF EXISTS (SELECT raw_material_local FROM scrape_number WHERE raw_material_local IS NOT NULL) THEN
    -- If the record exists, update the value column by adding 1
    UPDATE scrape_number
    SET raw_material_local = raw_material_local + 1
    WHERE raw_material_local IS NOT NULL returning raw_material_local INTO updated_value;
    raise notice 'updated_value update: %',updated_value;
  ELSE
    -- If the record does not exist, insert a new row and return the inserted value
    INSERT INTO scrape_number (raw_material_local)
    VALUES (input_value)
    RETURNING raw_material_local INTO updated_value;
    raise notice 'updated_value insert: %',updated_value;
  END IF;

  RETURN updated_value;
END;
$$ LANGUAGE plpgsql;

-- count scrape gdp

CREATE OR REPLACE FUNCTION count_scrape_gdp(input_value INT)
RETURNS INTEGER AS $$
DECLARE
    updated_value INT;
BEGIN
  IF EXISTS (SELECT gdp FROM scrape_number WHERE gdp IS NOT NULL) THEN
    -- If the record exists, update the value column by adding 1
    UPDATE scrape_number
    SET gdp = gdp + 1
    WHERE gdp IS NOT NULL returning gdp INTO updated_value;
    raise notice 'updated_value update: %',updated_value;
  ELSE
    -- If the record does not exist, insert a new row and return the inserted value
    INSERT INTO scrape_number (gdp)
    VALUES (input_value)
    RETURNING gdp INTO updated_value;
    raise notice 'updated_value insert: %',updated_value;
  END IF;

  RETURN updated_value;
END;
$$ LANGUAGE plpgsql;


-- count scrape global exchange rate

CREATE OR REPLACE FUNCTION count_scrape_exchange_global(input_value INT)
RETURNS INTEGER AS $$
DECLARE
    updated_value INT;
BEGIN
  IF EXISTS (SELECT exchange_rate_global FROM scrape_number WHERE exchange_rate_global IS NOT NULL) THEN
    -- If the record exists, update the value column by adding 1
    UPDATE scrape_number
    SET exchange_rate_global = exchange_rate_global + 1
    WHERE exchange_rate_global IS NOT NULL returning exchange_rate_global INTO updated_value;
    raise notice 'updated_value update: %',updated_value;
  ELSE
    -- If the record does not exist, insert a new row and return the inserted value
    INSERT INTO scrape_number (exchange_rate_global)
    VALUES (input_value)
    RETURNING exchange_rate_global INTO updated_value;
    raise notice 'updated_value insert: %',updated_value;
  END IF;

  RETURN updated_value;
END;
$$ LANGUAGE plpgsql;


-- count scrape global exchange rate

CREATE OR REPLACE FUNCTION count_scrape_exchange_local(input_value INT)
RETURNS INTEGER AS $$
DECLARE
    updated_value INT;
BEGIN
  IF EXISTS (SELECT exchange_rate_local FROM scrape_number WHERE exchange_rate_local IS NOT NULL) THEN
    -- If the record exists, update the value column by adding 1
    UPDATE scrape_number
    SET exchange_rate_local = exchange_rate_local + 1
    WHERE exchange_rate_local IS NOT NULL returning exchange_rate_local INTO updated_value;
    raise notice 'updated_value update: %',updated_value;
  ELSE
    -- If the record does not exist, insert a new row and return the inserted value
    INSERT INTO scrape_number (exchange_rate_local)
    VALUES (input_value)
    RETURNING exchange_rate_local INTO updated_value;
    raise notice 'updated_value insert: %',updated_value;
  END IF;

  RETURN updated_value;
END;
$$ LANGUAGE plpgsql;


-- count scrape crypto

CREATE OR REPLACE FUNCTION count_scrape_crypto(input_value INT)
RETURNS INTEGER AS $$
DECLARE
    updated_value INT;
BEGIN
  IF EXISTS (SELECT crypto FROM scrape_number WHERE crypto IS NOT NULL) THEN
    -- If the record exists, update the value column by adding 1
    UPDATE scrape_number
    SET crypto = crypto + 1
    WHERE crypto IS NOT NULL returning crypto INTO updated_value;
    raise notice 'updated_value update: %',updated_value;
  ELSE
    -- If the record does not exist, insert a new row and return the inserted value
    INSERT INTO scrape_number (crypto)
    VALUES (input_value)
    RETURNING crypto INTO updated_value;
    raise notice 'updated_value insert: %',updated_value;
  END IF;

  RETURN updated_value;
END;
$$ LANGUAGE plpgsql;


-- count scrape knowledge

CREATE OR REPLACE FUNCTION count_scrape_knowledge(input_value INT)
RETURNS INTEGER AS $$
DECLARE
    updated_value INT;
BEGIN
  IF EXISTS (SELECT knowledge FROM scrape_number WHERE knowledge IS NOT NULL) THEN
    -- If the record exists, update the value column by adding 1
    UPDATE scrape_number
    SET knowledge = knowledge + 1
    WHERE knowledge IS NOT NULL returning knowledge INTO updated_value;
    raise notice 'updated_value update: %',updated_value;
  ELSE
    -- If the record does not exist, insert a new row and return the inserted value
    INSERT INTO scrape_number (knowledge)
    VALUES (input_value)
    RETURNING knowledge INTO updated_value;
    raise notice 'updated_value insert: %',updated_value;
  END IF;

  RETURN updated_value;
END;
$$ LANGUAGE plpgsql;


-- count scrape bond

CREATE OR REPLACE FUNCTION count_scrape_bond(input_value INT)
RETURNS INTEGER AS $$
DECLARE
    updated_value INT;
BEGIN
  IF EXISTS (SELECT bond FROM scrape_number WHERE bond IS NOT NULL) THEN
    -- If the record exists, update the value column by adding 1
    UPDATE scrape_number
    SET bond = bond + 1
    WHERE bond IS NOT NULL returning bond INTO updated_value;
    raise notice 'updated_value update: %',updated_value;
  ELSE
    -- If the record does not exist, insert a new row and return the inserted value
    INSERT INTO scrape_number (bond)
    VALUES (input_value)
    RETURNING bond INTO updated_value;
    raise notice 'updated_value insert: %',updated_value;
  END IF;

  RETURN updated_value;
END;
$$ LANGUAGE plpgsql;



SELECT sum(news_global) + sum(news_cambodia) + sum(stock_local) + sum(stock_global)
+ sum(raw_material_local) + sum(raw_material_global) + sum(gdp) + sum(exchange_rate_local)
+ sum(exchange_rate_global) + sum(crypto) + sum(knowledge) + sum(bond)
AS count FROM scrape_number;

-- end

UPDATE local_stock_historical_data
SET status = 'published'
WHERE TO_CHAR(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = '2023-10-27T15:17:05';

SELECT * From global_stock WHERE TO_CHAR(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = '2023-10-27T15:17:05';

SELECT * FROM local_exchange where status = 'published' and scraped_date = (SELECT MAX(scraped_date) FROM local_exchange);


TRUNCATE global_stock restart identity;
UPDATE local_knowledge
SET status = 'published', published_date = now()
WHERE TO_CHAR(scraped_date, 'YYYY-MM-DD HH24:MI:SS') = '2023-10-22 15:27:17';


SELECT * FROM local_exchange;


create table global_raw_material(
    goods varchar,
    current_price varchar,
    high_price varchar,
    low_price varchar,
    status varchar default 'unpublished',
    scraped_date timestamp,
    variance varchar,
    variance_per varchar
);


create trigger global_raw_materials_trigger
    before update
    on global_raw_material
    for each row
execute procedure delete_globalrawmaterials_olddata();

delete from local_raw_materials;



CREATE TABLE stock_csx_index(
    id serial primary key,
    date varchar,
    current_index varchar,
    change_per varchar,
    opening varchar,
    high_price varchar,
    low_price varchar,
    trading_volume_share varchar,
    trading_value_khr varchar,
    market_cap varchar,
    status varchar default 'unpublished',
    scraped_date timestamp
);


SELECT * FROM raw_materials_detail where category = 'gold';

SELECT * FROM global_raw_material;

UPDATE local_raw_materials set status = 'published' where scraped_date = '2023-11-07 09:58:18.538898' returning *;

SELECT * FROM global_exchange_rate_historical_data where status = 'published' and category = 'Dollar/yen';


TRUNCATE global_bond restart identity ;