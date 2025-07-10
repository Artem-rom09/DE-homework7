
CREATE KEYSPACE IF NOT EXISTS amazon_reviews 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

-- Таблиця для запиту: "всі відгуки для продукту"
CREATE TABLE IF NOT EXISTS amazon_reviews.reviews_by_product (
    product_id text,
    review_date timestamp,
    review_id text,
    customer_id int,
    star_rating int,
    review_headline text,
    review_body text,
    PRIMARY KEY ((product_id), review_date, review_id)
) WITH CLUSTERING ORDER BY (review_date DESC, review_id ASC);

-- Таблиця для запиту: "всі відгуки для продукту з певним рейтингом"
CREATE TABLE IF NOT EXISTS amazon_reviews.reviews_by_product_and_rating (
    product_id text,
    star_rating int,
    review_date timestamp,
    review_id text,
    customer_id int,
    review_headline text,
    review_body text,
    PRIMARY KEY ((product_id, star_rating), review_date, review_id)
) WITH CLUSTERING ORDER BY (review_date DESC, review_id ASC);

-- Таблиця для запиту: "всі відгуки клієнта"
CREATE TABLE IF NOT EXISTS amazon_reviews.reviews_by_customer (
    customer_id int,
    review_date timestamp,
    review_id text,
    product_id text,
    star_rating int,
    review_headline text,
    review_body text,
    PRIMARY KEY ((customer_id), review_date, review_id)
) WITH CLUSTERING ORDER BY (review_date DESC, review_id ASC);

-- Таблиця для агрегації: "N найпопулярніших продуктів за період"
CREATE TABLE IF NOT EXISTS amazon_reviews.most_reviewed_products_by_month (
    period text, -- формат 'YYYY-MM'
    total_reviews int,
    product_id text,
    product_title text,
    PRIMARY KEY ((period), total_reviews, product_id)
) WITH CLUSTERING ORDER BY (total_reviews DESC);

-- Таблиця для агрегації: "N найпродуктивніших клієнтів за період"
CREATE TABLE IF NOT EXISTS amazon_reviews.most_productive_customers_by_month (
    period text,
    total_reviews int,
    customer_id int,
    PRIMARY KEY ((period), total_reviews, customer_id)
) WITH CLUSTERING ORDER BY (total_reviews DESC);

-- Таблиця для агрегації: "N найбільших 'хейтерів' (1-2 зірки)"
CREATE TABLE IF NOT EXISTS amazon_reviews.haters_by_month (
    period text,
    hater_reviews int,
    customer_id int,
    PRIMARY KEY ((period), hater_reviews, customer_id)
) WITH CLUSTERING ORDER BY (hater_reviews DESC);

-- Таблиця для агрегації: "N найбільших 'прихильників' (4-5 зірок)"
CREATE TABLE IF NOT EXISTS amazon_reviews.backers_by_month (
    period text,
    backer_reviews int,
    customer_id int,
    PRIMARY KEY ((period), backer_reviews, customer_id)
) WITH CLUSTERING ORDER BY (backer_reviews DESC);