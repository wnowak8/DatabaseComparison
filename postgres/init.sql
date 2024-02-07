CREATE TABLE reviews (
	rating int4 NOT NULL,
	verified boolean NOT NULL,
	reviewerID varchar NOT NULL,
	product_id varchar NOT NULL,
	"date" timestamp(0) NOT NULL,
	vote float8 NULL
);