CREATE TABLE Shipping(
    shipping_id INT PRIMARY KEY,
    order_id INT NOT NULL,
    shipping_mode varchar(255),
    shipping_date VARCHAR(255),
    days_for_shipping_real VARCHAR(255),
    days_for_shipment_schedules VARCHAR(255),

    FOREIGN KEY (order_id) REFERENCES Order(order_id)

)