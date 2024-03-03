CREATE TABLE reviews (
	rating int4 NOT NULL,
	verified boolean NOT NULL,
	reviewer_id varchar NOT NULL,
	product_id varchar NOT NULL,
	"date" timestamp NULL,
	vote float8 NULL
);