-- CREATE DATABASE IF IT DOES NOT EXIST
CREATE DATABASE IF NOT EXISTS superstore_dw;

-- USE THE DATABASE
USE superstore_dw;

-- DELETE ALL TABLES IF THEY EXIST

-- FACT TABLES

DROP TABLE IF EXISTS ShippingBehaviorS;
DROP TABLE IF EXISTS ShippingBehavior;
DROP TABLE IF EXISTS ProductPerformance;
DROP TABLE IF EXISTS OrderM;
DROP TABLE IF EXISTS Orders;
DROP TABLE IF EXISTS Item;


-- DIMENSION TABLES

DROP TABLE IF EXISTS Shipping;
DROP TABLE IF EXISTS Product;
DROP TABLE IF EXISTS Category;
DROP TABLE IF EXISTS Location;
DROP TABLE IF EXISTS State;
DROP TABLE IF EXISTS Region;
DROP TABLE IF EXISTS Customer;
DROP TABLE IF EXISTS CalendarMonth;
DROP TABLE IF EXISTS Calendar;

-- CREATE DIMENSION TABLES
CREATE TABLE Calendar (
    calendar_id INT AUTO_INCREMENT PRIMARY KEY,
    full_date DATE,
    year_id INT,
    year_number INT,
    month_id INT,
    month_number INT,
    month_name VARCHAR(15),
    day_id INT,
    day_number INT
);

CREATE TABLE CalendarMonth (
    calendar_month_id INT AUTO_INCREMENT PRIMARY KEY,
    calendar_month_number INT,
    calendar_month_name VARCHAR(15),
    year_id INT,
    year_number INT 
);

CREATE TABLE Customer (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_code VARCHAR(50),
    customer_name VARCHAR(100),
    segment VARCHAR(20)
);

CREATE TABLE Region (
    region_id INT AUTO_INCREMENT PRIMARY KEY,
    region_name VARCHAR(50),
    country_id INT,
    country_name VARCHAR(50)
);

CREATE TABLE State (
    state_id INT AUTO_INCREMENT PRIMARY KEY,
    state_name VARCHAR(50),
    region_id INT,
    region_name VARCHAR(50),
    country_id INT,
    country_name VARCHAR(50)
);

CREATE TABLE Location (
    location_id INT AUTO_INCREMENT PRIMARY KEY,
    location_code VARCHAR(50),
    country_id INT,
    country_name VARCHAR(50),
    region_id INT,
    region_name VARCHAR(50),
    state_id INT,
    state_name VARCHAR(50),
    city_id INT,
    city_name VARCHAR(50),
    postal_code VARCHAR(15)
);

CREATE TABLE Category (
    category_id INT AUTO_INCREMENT PRIMARY KEY,
    category_name VARCHAR(50)
);

CREATE TABLE Product (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    product_code VARCHAR(50),
    product_name VARCHAR(150),
    category_id INT,
    category_name VARCHAR(50),
    sub_category_id INT,
    sub_category_name VARCHAR(50)
);

CREATE TABLE Shipping (
    shipping_id INT AUTO_INCREMENT PRIMARY KEY,
    ship_mode VARCHAR(50)
);

-- CREATE FACT TABLES

CREATE TABLE Item (
    customer_id INT,
    location_id INT,
    calendar_id INT,
    product_id INT,
    order_code VARCHAR(50),
    quantity INT,
    sales DECIMAL(10, 2),
    discount DECIMAL(3, 2),
    lost_value DECIMAL(10, 2),
    profit DECIMAL(10, 2),
    CONSTRAINT fk_item_customer FOREIGN KEY (customer_id) REFERENCES Customer(customer_id),
    CONSTRAINT fk_item_location FOREIGN KEY (location_id) REFERENCES Location(location_id),
    CONSTRAINT fk_item_calendar FOREIGN KEY (calendar_id) REFERENCES Calendar(calendar_id),
    CONSTRAINT fk_item_product FOREIGN KEY (product_id) REFERENCES Product(product_id),
    PRIMARY KEY (customer_id, location_id, calendar_id, product_id)
);

CREATE TABLE Orders (
    order_calendar_id INT,
    shipping_calendar_id INT,
    customer_id INT,
    location_id INT,
    shipping_id INT,
    order_code VARCHAR(50),
    sales_order DECIMAL(10, 2),
    quantity_order DECIMAL(10, 2),
    lost_value_order DECIMAL(10, 2),
    profit_order DECIMAL(10, 2),
    CONSTRAINT fk_orders_order_calendar FOREIGN KEY (order_calendar_id) REFERENCES Calendar(calendar_id),
    CONSTRAINT fk_orders_shipping_calendar FOREIGN KEY (shipping_calendar_id) REFERENCES Calendar(calendar_id),
    CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES Customer(customer_id),
    CONSTRAINT fk_orders_location FOREIGN KEY (location_id) REFERENCES Location(location_id),
    CONSTRAINT fk_orders_shipping FOREIGN KEY (shipping_id) REFERENCES Shipping(shipping_id),
    PRIMARY KEY (order_calendar_id, shipping_calendar_id, location_id, shipping_id)
);

CREATE TABLE OrderM (
    calendar_month_id INT,
    state_id INT,
    sales_month DECIMAL(10, 2),
    quantity_month DECIMAL(10, 2),
    lost_value_month DECIMAL(10, 2),
    profit_month DECIMAL(10, 2),
    CONSTRAINT fk_order_m_calendar_month FOREIGN KEY (calendar_month_id) REFERENCES CalendarMonth(calendar_month_id),
    CONSTRAINT fk_order_m_state FOREIGN KEY (state_id) REFERENCES State(state_id),
    PRIMARY KEY (calendar_month_id, state_id)
);

CREATE TABLE ProductPerformance (
    category_id INT,
    state_id INT,
    calendar_month_id INT,
    total_sales DECIMAL(10, 2),
    total_profit DECIMAL(10, 2),
    cumulative_profit DECIMAL(10, 2),
    total_quantity INT,
    CONSTRAINT fk_product_performance_category FOREIGN KEY (category_id) REFERENCES Category(category_id),
    CONSTRAINT fk_product_performance_state FOREIGN KEY (state_id) REFERENCES State(state_id),
    CONSTRAINT fk_product_performance_calendar_month FOREIGN KEY (calendar_month_id) REFERENCES CalendarMonth(calendar_month_id),
    PRIMARY KEY (category_id, state_id, calendar_month_id)
);

CREATE TABLE ShippingBehavior (
    shipping_id INT,
    category_id INT,
    region_id INT,
    shipping_delay INT,
    method_freq INT,
    CONSTRAINT fk_shipping_behavior_shipping FOREIGN KEY (shipping_id) REFERENCES Shipping(shipping_id),
    CONSTRAINT fk_shipping_behavior_category FOREIGN KEY (category_id) REFERENCES Category(category_id),
    CONSTRAINT fk_shipping_behavior_region FOREIGN KEY (region_id) REFERENCES Region(region_id),
    PRIMARY KEY (shipping_id, category_id, region_id)
);

CREATE TABLE ShippingBehaviorS (
    shipping_id INT,
    category_id INT,
    state_id INT,
    shipping_delay INT,
    method_freq INT,
    CONSTRAINT fk_shipping_behavior_s_shipping FOREIGN KEY (shipping_id) REFERENCES Shipping(shipping_id),
    CONSTRAINT fk_shipping_behavior_s_category FOREIGN KEY (category_id) REFERENCES Category(category_id),
    CONSTRAINT fk_shipping_behavior_s_state FOREIGN KEY (state_id) REFERENCES State(state_id),
    PRIMARY KEY (shipping_id, category_id, state_id)
);
