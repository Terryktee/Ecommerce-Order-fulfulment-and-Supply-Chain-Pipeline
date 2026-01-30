CREATE TABLE Orders (
    order_id INT PRIMARY KEY,
    order_date VARCHAR(255) ,
    customer_id INT NOT NULL,
    order_city VARCHAR(255) ,
    order_country VARCHAR(255),
    order_region VARCHAR(255),
    order_zipcode INT,
    order_status VARCHAR(255),
    market VARCHAR(255)

    FOREIGN KEY (customer_id) REFERENCES Customer(customer_id)
)