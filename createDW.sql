create dwh_project;
use dwh_project;

drop table if exists fact;
drop table if exists product;
drop table if exists customer;
drop table if exists store;
drop table if exists trandate;
drop table if exists supplier;

CREATE TABLE `product` (
  `PRODUCT_ID` VARCHAR(6) NOT NULL, 
  `PRODUCT_NAME` VARCHAR(30) NOT NULL,  
  CONSTRAINT product_pk PRIMARY KEY (PRODUCT_ID));
  
CREATE TABLE `customer` (
  `CUSTOMER_ID` VARCHAR(6) NOT NULL, 
  `CUSTOMER_NAME` VARCHAR(30) NOT NULL,  
  CONSTRAINT cust_pk PRIMARY KEY (CUSTOMER_ID));

CREATE TABLE `store` (
  `STORE_ID` VARCHAR(6) NOT NULL, 
  `STORE_NAME` VARCHAR(30) NOT NULL,  
  CONSTRAINT store_pk PRIMARY KEY (STORE_ID));
  
  
CREATE TABLE `supplier` (
  `SUPPLIER_ID` VARCHAR(6) NOT NULL, 
  `SUPPLIER_NAME` VARCHAR(30) NOT NULL,  
  CONSTRAINT supp_pk PRIMARY KEY (SUPPLIER_ID));
  
  
CREATE TABLE `trandate` (
  `TIME_ID` VARCHAR(8) NOT NULL,
  `FDATE` DATE NOT NULL,
  `DAYS` Numeric(4) NOT NULL,   
  `MONTHS` Numeric(4) NOT NULL,
  `QUARTERS` Numeric(4) NOT NULL,
  `YEARS` Numeric(4) NOT NULL,
  CONSTRAINT date_pk PRIMARY KEY (TIME_ID));  
  
CREATE TABLE `fact` (
`prod` VARCHAR(6) NOT NULL,
`supp` VARCHAR(6) NOT NULL,
`date_id` VARCHAR(8) NOT NULL,
`store` VARCHAR(6) NOT NULL,
`cust` VARCHAR(6) NOT NULL,
FOREIGN KEY (`prod`) REFERENCES product(PRODUCT_ID),
FOREIGN KEY (`supp`) REFERENCES supplier(SUPPLIER_ID),
FOREIGN KEY (`date_id`) REFERENCES trandate(TIME_ID),
FOREIGN KEY (`store`) REFERENCES store(STORE_ID),
FOREIGN KEY (`cust`) REFERENCES customer(CUSTOMER_ID),
`quant` numeric(30) NOT NULL,
`price` numeric(30) NOT NULL,
`totsale` FLOAT(30) NOT NULL,
CONSTRAINT fact_pk PRIMARY KEY (prod, supp, date_id, store, cust)); 
  

