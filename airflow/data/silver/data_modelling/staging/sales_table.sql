CREATE TABLE SALES(
    sales_id INT PRIMARY KEY,
    order_id INT NOT NULL,
    order_item_id INT NOT NULL ,
    sales_amount Decimal(10,2),
    profit_per_order Decimal(10,2),
    benefit_per_order Decimal(10,2),
    profit_ratio Decimal(10,2),

    FOREIGN KEY (order_id) REFERENCES Order(order_id),
    FOREIGN KEY (order_item_id) REFERENCES OrderItem(order_item_id)
)