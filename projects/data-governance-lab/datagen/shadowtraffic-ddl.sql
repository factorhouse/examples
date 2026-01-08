DROP TABLE IF EXISTS demo.heartbeat;
DROP TABLE IF EXISTS demo.users;
DROP TABLE IF EXISTS demo.orders;

CREATE TABLE IF NOT EXISTS demo.heartbeat (
		id integer PRIMARY KEY,
		ts timestamp without time zone NOT NULL DEFAULT NOW()
);

CREATE TABLE demo.users (
	user_id uuid NOT NULL,
	first_name text NOT NULL,
	last_name text NOT NULL,
	email text NOT NULL,
	country text NOT NULL,
	postal_code text NOT NULL,
	created_at timestamp NOT NULL,
	CONSTRAINT users_pkey PRIMARY KEY (user_id)
);

CREATE TABLE demo.orders (
	order_id uuid NOT NULL,
	user_id uuid NOT NULL,
	product_name text NOT NULL,
	category text NOT NULL,
	quantity integer NOT NULL,
	unit_price decimal(5,2) NOT NULL,
	created_at timestamp NOT NULL,
	CONSTRAINT orders_pkey PRIMARY KEY (order_id)
);
