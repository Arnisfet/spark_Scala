create table clients (
uid VARCHAR (40) UNIQUE NOT NULL
, gender VARCHAR(1)
, age_cat VARCHAR(5)
, shop_cat VARCHAR(200)
, web_cat VARCHAR(200));

GRANT SELECT ON TABLE clients TO labchecker2;