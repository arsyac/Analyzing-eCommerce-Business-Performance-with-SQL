-- First step = CREATE TABLE BY USING DATA RELATIONSHIP DIAGRAM

CREATE TABLE customers(
	customer_id VARCHAR,
	customer_unique_id VARCHAR,
	customer_zip_code_prefix INTEGER,
	customer_city VARCHAR,
	customer_state VARCHAR
);
CREATE TABLE geolocation(
	geo_zip_code_prefix VARCHAR,
	geo_lat VARCHAR,
	geo_lng VARCHAR,
	geo_city VARCHAR,
	geo_state VARCHAR
);
CREATE TABLE order_items(
	order_id VARCHAR,
	order_item_id INTEGER,
	product_id VARCHAR,
	seller_id VARCHAR,
	shipping_limit_date TIMESTAMP WITHOUT TIME ZONE,
	price FLOAT,
	freight_value FLOAT
);
CREATE TABLE order_payments(
	order_id VARCHAR,
	payment_sequential INT,
	payment_type VARCHAR,
	payment_installments INT,
	payment_value FLOAT
);
CREATE TABLE order_reviews(
	review_id VARCHAR,
	order_id VARCHAR,
	review_score INT,
	review_comment_title VARCHAR,
	review_comment_message TEXT,
	review_creation_date TIMESTAMP WITHOUT TIME ZONE,
	review_answer TIMESTAMP WITHOUT TIME ZONE
);
CREATE TABLE orders(
	order_id VARCHAR,
	customer_id VARCHAR,
	order_status VARCHAR,
	order_purchase_timestamp TIMESTAMP WITHOUT TIME ZONE,
	order_approved_at TIMESTAMP WITHOUT TIME ZONE,
	order_delivered_customer_date TIMESTAMP WITHOUT TIME ZONE,
	order_estimated_delivery_date TIMESTAMP WITHOUT TIME ZONE,
);
CREATE TABLE product(
	num INT,
	product_id VARCHAR,
	product_category_name VARCHAR,
	product_name_length FLOAT,
	product_description_length FLOAT,
	product_photos_qty FLOAT,
	product_weight_g FLOAT,
	product_length_cm FLOAT,
	product_height_cm FLOAT,
	product_width_cm FLOAT
);
CREATE TABLE sellers(
	seller_id VARCHAR,
	seller_zip_code_prefix VARCHAR,
	seller_city VARCHAR,
	seller_state VARCHAR
);

-- Second step = import dataset to Postgre Database

COPY customers(
	customer_id,
	customer_unique_id,
	customer_zip_code_prefix,
	customer_city,
	customer_state
)
FROM 'macos:\OneDrive\DS\Mini Project\1\Dataset(extract\customers_dataset.csv'
DELIMITER ','
CSV HEADER;

COPY geolocation(
	geo_zip_code_prefix,
	geo_lat,
	geo_lng,
	geo_city,
	geo_state
)
FROM 'macos:\OneDrive\DS\Mini Project\1\Dataset(extract\customers_dataset.csv'
DELIMITER ','
CSV HEADER;

COPY order_items(
	order_id,
	order_item_id,
	product_id,
	seller_id,
	shipping_limit_date,
	price,
	freight_value
)
FROM 'macos:\OneDrive\DS\Mini Project\1\Dataset(extract\customers_dataset.csv'
DELIMITER ','
CSV HEADER;

COPY order_payments(
	order_id,
	payment_sequential,
	payment_type,
	payment_installments,
	payment_value
)
FROM 'macos:\OneDrive\DS\Mini Project\1\Dataset(extract\customers_dataset.csv'
DELIMITER ','
CSV HEADER;

COPY order_reviews(
	review_id,
	order_id,
	review_score,
	review_comment_title,
	review_comment_message,
	review_creation_date,
	review_answer
)
FROM 'macos:\OneDrive\DS\Mini Project\1\Dataset(extract\customers_dataset.csv'
DELIMITER ','
CSV HEADER;

COPY orders(
	order_id,
	customer_id,
	order_status,
	order_purchase_timestamp,
	order_approved_at,
	order_delivered_customer_date,
	order_estimated_delivery_date
)
FROM 'macos:\OneDrive\DS\Mini Project\1\Dataset(extract\customers_dataset.csv'
DELIMITER ','
CSV HEADER;

CREATE TABLE product(
	num,
	product_id,
	product_category_name,
	product_name_length,
	product_description_length,
	product_photos_qty,
	product_weight_g,
	product_length_cm,
	product_height_cm,
	product_width_cm
)
FROM 'macos:\OneDrive\DS\Mini Project\1\Dataset(extract\customers_dataset.csv'
DELIMITER ','
CSV HEADER;

COPY sellers(
	seller_id,
	seller_zip_code_prefix,
	seller_city,
	seller_state
)
FROM 'macos:\OneDrive\DS\Mini Project\1\Dataset(extract\customers_dataset.csv'
DELIMITER ','
CSV HEADER;

--Primary Key
ALTER TABLE customers ADD CONSTRAINT customers_prikey ADD PRIMARY KEY(customer_id);
ALTER TABLE orders ADD CONSTRAINT orders_prikey ADD PRIMARY KEY(order_id);
ALTER TABLE product ADD CONSTRAINT product_prikey ADD PRIMARY KEY(product_id);
ALTER TABLE sellers ADD CONSTRAINT sellers_prikey ADD PRIMARY KEY(seller_id);

--Foreign Key
ALTER TABLE order_items ADD FOREIGN KEY(order_id) REFERENCES orders;
ALTER TABLE order_items ADD FOREIGN KEY(product_id) REFERENCES product;
ALTER TABLE order_items	ADD FOREIGN KEY(seller_id) REFERENCES sellers;
ALTER TABLE order_payments ADD FOREIGN KEY(order_id) REFERENCES orders;
ALTER TABLE order_reviews ADD FOREIGN KEY(order_id) REFERENCES orders;
ALTER TABLE orders ADD FOREIGN KEY(customer_id) REFERENCES customers;


--Tugas 2
WITH
  mau_annual AS(
  SELECT
    tahun,
    ROUND(AVG(mau),3) AS avg_mau
  FROM (
    SELECT
      date_part('year',
        od.order_purchase_timestamp) AS tahun,
      date_part('month',
        od.order_purchase_timestamp) AS bulan,
      COUNT(DISTINCT cd.customer_unique_id) AS mau
    FROM
      customers_dataset AS cd
    INNER JOIN
      orders_dataset AS od
    ON
      cd.customer_id = od.customer_id
    GROUP BY
      tahun,
      bulan) AS sub1
  GROUP BY
    tahun
  ORDER BY
    tahun),
  new_customers AS(
  SELECT
    tahun,
    SUM(mau) AS total_customer
  FROM (
    SELECT
      date_part('year',
        od.order_purchase_timestamp) AS tahun,
      date_part('month',
        od.order_purchase_timestamp) AS bulan,
      COUNT(DISTINCT cd.customer_unique_id) AS mau
    FROM
      customers_dataset AS cd
    INNER JOIN
      orders_dataset AS od
    ON
      cd.customer_id = od.customer_id
    GROUP BY
      tahun,
      bulan) AS sub2
  GROUP BY
    tahun),
  cust_repeat_order AS(
  SELECT
    tahun,
    COUNT(customer_unique_id) AS repeat_order
  FROM (
    SELECT
      date_part('year',
        od.order_purchase_timestamp) AS tahun,
      cd.customer_unique_id,
      COUNT(cd.customer_unique_id) AS jumlah
    FROM
      customers_dataset AS cd
    INNER JOIN
      orders_dataset AS od
    ON
      cd.customer_id = od.customer_id
    GROUP BY
      tahun,
      cd.customer_unique_id
    HAVING
      COUNT(cd.customer_unique_id) > 1) AS sub3
  GROUP BY
    tahun),
  avg_frekuensi_order_annual AS(
  SELECT
    tahun,
    ROUND(AVG(jumlah),3) AS avg_frekuensi_order
  FROM (
    SELECT
      date_part('year',
        od.order_purchase_timestamp) AS tahun,
      cd.customer_unique_id,
      COUNT(cd.customer_unique_id) AS jumlah
    FROM
      customers_dataset AS cd
    INNER JOIN
      orders_dataset AS od
    ON
      cd.customer_id = od.customer_id
    GROUP BY
      tahun,
      cd.customer_unique_id) AS sub4
  GROUP BY
    tahun)
SELECT
  ma.tahun,
  ma.avg_mau,
  nc.total_customer,
  rpo.repeat_order,
  afoa.avg_frekuensi_order
FROM
  mau_annual AS ma
INNER JOIN
  new_customers AS nc
ON
  ma.tahun = nc.tahun
INNER JOIN
  cust_repeat_order AS rpo
ON
  nc.tahun = rpo.tahun
INNER JOIN
  avg_frekuensi_order_annual AS afoa
ON
  rpo.tahun = afoa.tahun;
  
--Tugas 3
WITH
  annual_revenue AS(
  SELECT
    date_part('year',
      od.order_purchase_timestamp) AS tahun,
    SUM(oid.price + oid.freight_value) AS total_revenue_annual
  FROM
    orders_dataset AS od
  INNER JOIN
    order_items_dataset AS oid
  ON
    od.order_id = oid.order_id
  WHERE
    order_status = 'delivered'
  GROUP BY
    tahun
  ORDER BY
    tahun),
  annual_cancel AS(
  SELECT
    date_part('year',
      od.order_purchase_timestamp) AS tahun,
    COUNT(order_status) AS cancel_order_annual
  FROM
    orders_dataset AS od
  WHERE
    order_status = 'canceled'
  GROUP BY
    tahun),
  annual_product AS(
  SELECT
    tahun,
    product_category_name,
    revenue_product_annual
  FROM (
    SELECT
      date_part('year',
        od.order_purchase_timestamp) AS tahun,
      pd.product_category_name,
      SUM(oid.price + oid.freight_value) AS revenue_product_annual,
      RANK() OVER(PARTITION BY date_part('year', od.order_purchase_timestamp)
      ORDER BY
        SUM(oid.price + oid.freight_value) DESC) AS rank
    FROM
      orders_dataset AS od
    INNER JOIN
      order_items_dataset AS oid
    ON
      od.order_id = oid.order_id
    INNER JOIN
      products_dataset AS pd
    ON
      oid.product_id = pd.product_id
    GROUP BY
      tahun,
      pd.product_category_name) AS sub5
  WHERE
    rank = 1),
  annual_cancel_product AS(
  SELECT
    tahun,
    product_category_name,
    most_cancel_order_product
  FROM (
    SELECT
      date_part('year',
        od.order_purchase_timestamp) AS tahun,
      pd.product_category_name,
      COUNT(order_status) AS most_cancel_order_product,
      RANK() OVER(PARTITION BY date_part('year', od.order_purchase_timestamp)
      ORDER BY
        COUNT(order_status) DESC) AS rank
    FROM
      orders_dataset AS od
    INNER JOIN
      order_items_dataset AS oid
    ON
      od.order_id = oid.order_id
    INNER JOIN
      products_dataset AS pd
    ON
      oid.product_id = pd.product_id
    WHERE
      order_status = 'canceled'
    GROUP BY
      tahun,
      pd.product_category_name) AS sub6
  WHERE
    rank = 1)
SELECT
  ar.tahun,
  ar.total_revenue_annual,
  ac.cancel_order_annual,
  ap.product_category_name,
  ap.revenue_product_annual,
  acp.product_category_name,
  acp.most_cancel_order_product
FROM
  annual_revenue AS ar
INNER JOIN
  annual_cancel AS ac
ON
  ar.tahun = ac.tahun
INNER JOIN
  annual_product AS ap
ON
  ac.tahun = ap.tahun
INNER JOIN
  annual_cancel_product AS acp
ON
  ap.tahun = acp.tahun


-- Tugas 3
with tabel1 as(
select
    pd.payment_type,
    count(payment_type) as jumlah
from orders_dataset as od
inner join payments_dataset as pd
    on od.order_id = pd.order_id
group by pd.payment_type
order by jumlah desc),
tabel2 as(
select
    payment_type,
    sum(case when tahun = 2016 then 1 else 0 end) as tahun_2016,
    sum(case when tahun = 2017 then 1 else 0 end) as tahun_2017,
    sum(case when tahun = 2018 then 1 else 0 end) as tahun_2018
from(
    select
        date_part('year',od.order_purchase_timestamp) as tahun,
        pd.payment_type
    from orders_dataset as od
    inner join payments_dataset as pd
        on od.order_id = pd.order_id) as sub7
group by payment_type
order by payment_type)
select
    t1.payment_type,
    tahun_2016,
    tahun_2017,
    tahun_2018,
    jumlah
from tabel1 as t1
inner join tabel2 as t2
    on t1.payment_type = t2.payment_type
order by jumlah desc;
