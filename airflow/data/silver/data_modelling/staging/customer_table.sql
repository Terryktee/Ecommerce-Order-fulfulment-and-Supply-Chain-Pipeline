CREATE TABLE Customer (
    customer_id INT NOT NULL PRIMARY KEY,
    customer_fname varchar(255) ,
    customer_lname varchar(255) ,
    customer_password varchar(255) , 
    customer_segment varchar(255) ,
    customer_state varchar(255) ,
    customer_street varchar(255) ,
    customer_zipcode INT ,
    customer_city varchar (255) ,
    customer_country varchar(255)
)