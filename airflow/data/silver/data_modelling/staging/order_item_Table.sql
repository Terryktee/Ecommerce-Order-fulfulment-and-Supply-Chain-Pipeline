CREATE TABLE OrderItem(
    order_item_id INT PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT not null,
    order_item_quantity VARCHAR(255),
    order_item_product_price Decimal(10,2),
    order_item_discount Decimal(10,2),
    order_item_discount_rate Decimal(10,2),

    FOREIGN KEY (order_id) REFERENCES Order(order_id),
    FOREIGN KEY (product_id) REFERENCES Product(product_id)
)