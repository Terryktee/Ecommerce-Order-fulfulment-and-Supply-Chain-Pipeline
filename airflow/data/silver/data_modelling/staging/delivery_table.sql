CREATE TABLE Delivery (
    delivey_id INT PRIMARY KEY,
    order_id INT NOT NULL,
    delivery_status VARCHAR(255),
    late_delivery_risk INT ,

    FOREIGN KEY (order_id) REFERENCES Order(order_id)
)