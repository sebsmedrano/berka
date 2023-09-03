-- Databricks notebook source
CREATE TABLE
account (
    account_id int,
    district_id int,
    frequency string,
    date DATE
)
USING csv
OPTIONS (
  'path' '/FileStore/account.asc',
  'header' 'false',
  'delimiter' ';'
);


-- COMMAND ----------

CREATE TABLE card (
    card_id int,
    disp_id int,
    type string,
    issued string
)
USING csv
OPTIONS (
  'path' '/FileStore/card.asc',
  'header' 'false',
  'delimiter' ';'
);

-- COMMAND ----------

CREATE TABLE client (
    client_id int,
    birth_number string,
    district_id int
)
USING csv
OPTIONS (
  'path' '/FileStore/client.asc',
  'header' 'false',
  'delimiter' ';'
);

-- COMMAND ----------

CREATE TABLE disp (
    disp_id int,
    client_id int,
    account_id int,
    type string
)
USING csv
OPTIONS (
  'path' '/FileStore/disp.asc',
  'header' 'false',
  'delimiter' ';'
);

-- COMMAND ----------

CREATE TABLE district (
    district_id int,
    A2 string,
    A3 string,
    A4 int,
    A5 int,
    A6 int,
    A7 int,
    A8 int,
    A9 int,
    A10 FLOAT,
    A11 int,
    A12 FLOAT,
    A13 FLOAT,
    A14 int,
    A15 int,
    A16 int
)
USING csv
OPTIONS (
  'path' '/FileStore/district.asc',
  'header' 'false',
  'delimiter' ';'
);

-- COMMAND ----------

CREATE TABLE loan (
    loan_id int,
    account_id int,
    date DATE,
    amount int,
    duration int,
    payments FLOAT,
    status string
)
USING csv
OPTIONS (
  'path' '/FileStore/loan.asc',
  'header' 'false',
  'delimiter' ';'
);

-- COMMAND ----------

CREATE TABLE order (
    order_id int,
    account_id int,
    bank_to string,
    account_to string,
    amount FLOAT,
    k_symbol string
)
USING csv
OPTIONS (
  'path' '/FileStore/order.asc',
  'header' 'false',
  'delimiter' ';'
);

-- COMMAND ----------

CREATE TABLE trans (
    trans_id int,
    account_id int,
    date DATE,
    type string,
    operation string,
    amount float,
    balance float,
    k_symbol string,
    bank string,
    account string
)
USING csv
OPTIONS (
  'path' '/FileStore/trans.asc',
  'header' 'false',
  'delimiter' ';'
);

-- COMMAND ----------

SELECT d.disp_id, d.client_id, d.account_id, d.type as disp_type,a.district_id, a.frequency, a.date,
t.trans_id, t.type, t.operation, t.amount, t.balance, t.k_symbol, t.bank
FROM disp d
JOIN account a ON d.account_id = a.account_id
JOIN trans t ON d.account_id = t.account_id

-- COMMAND ----------

SELECT d.client_id, count(t.trans_id) as num_transactions
FROM disp d
JOIN account a ON d.account_id = a.account_id
join trans t ON d.account_id = t.account_id
group by d.client_id

-- COMMAND ----------

SELECT d.account_id, c.client_id
FROM disp d, client c
JOIN account a ON d.account_id = a.account_id
WHERE 
    d.client_id = c.client_id and
    d.type not in ('OWNER') and
    d.account_id in
   	 (
   		 SELECT l.account_id
   		 FROM loan l, account a
   		 WHERE l.account_id = a.account_id and l.status = 'A'
   	 )
