CREATE TABLE Product(
    product_id INT PRIMARY KEY,
    product_name varchar(255),
    product_description varchar(255),
    product_image VARCHAR(255),
    product_price decimal(10,2),
    product_status varchar(255)

    FOREIGN KEY (category_id) REFERENCES Category(category_id)
)