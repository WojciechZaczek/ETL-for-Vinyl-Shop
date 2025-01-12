CREATE TABLE artist(
	artist_id INT IDENTITY (1, 1) PRIMARY KEY,
	first_name VARCHAR (50) NULL,
	profile VARCHAR (255) NULL,
	last_name VARCHAR (50) NULL,
	band_name_ VARCHAR (100) NULL
);
CREATE TABLE album_information(
	album_information_id INT IDENTITY (1, 1) PRIMARY KEY,
	style VARCHAR (50) NULL,
	audio_format VARCHAR (50) NULL,
	album_format VARCHAR (50) NULL,
);
CREATE TABLE album(
	album_id INT IDENTITY (1, 1) PRIMARY KEY,
	title VARCHAR (100) NULL,
	release_year INT NULL,
	artist_id INT NULL,
	album_information_id INT NULL,
	FOREIGN KEY (artist_id) REFERENCES artist (artist_id) ON DELETE NO ACTION ON UPDATE NO ACTION,
	FOREIGN KEY (album_information_id) REFERENCES album_information (album_information_id) ON DELETE NO ACTION ON UPDATE NO ACTION,
);

CREATE TABLE label(
	label_id INT IDENTITY (1, 1) PRIMARY KEY,
	label_name VARCHAR (50) NULL,
	address VARCHAR (250) NULL
);
CREATE TABLE record(
	record_id INT IDENTITY (1, 1) PRIMARY KEY,
	serial_no VARCHAR(50) NULL,
	price DECIMAL (10, 2) NULL,
	release_year INT NULL,
	label_id INT NULL,
	country_id VARCHAR (50) NULL,
	album_id INT NULL,
	quantity INT NULL,
	FOREIGN KEY (label_id) REFERENCES label (label_id) ON DELETE NO ACTION ON UPDATE NO ACTION,
	FOREIGN KEY (album_id) REFERENCES album (album_id) ON DELETE NO ACTION ON UPDATE NO ACTION,
);

CREATE TABLE customer(
	customer_id INT IDENTITY (1, 1) PRIMARY KEY,
	first_name VARCHAR (50) NOT NULL,
	last_name VARCHAR (50) NOT NULL,
	address VARCHAR (250) NOT NULL,
	email VARCHAR (100),
	phone_number VARCHAR (50),
);

CREATE TABLE orders(
	order_id INT IDENTITY (1, 1) PRIMARY KEY,
	order_no VARCHAR(50) NOT NULL,
	order_data DATETIME NOT NULL,
	customer_id INT NOT NULL,
	FOREIGN KEY (customer_id) REFERENCES customer (customer_id) ON DELETE NO ACTION ON UPDATE NO ACTION,
);



CREATE TABLE order_item(
	order_item INT IDENTITY (1, 1) PRIMARY KEY,
	record_id INT NOT NULL,
	quantity INT NOT NULL,
	order_id INT NOT NULL,
	FOREIGN KEY (record_id) REFERENCES record (record_id) ON DELETE NO ACTION ON UPDATE NO ACTION,
	FOREIGN KEY (order_id) REFERENCES orders (order_id) ON DELETE NO ACTION ON UPDATE NO ACTION,
);