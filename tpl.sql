DROP TABLE IF EXISTS items;
DROP TABLE IF EXISTS delivery;
DROP TABLE IF EXISTS payment;
DROP TABLE IF EXISTS orders;

CREATE TABLE public.delivery (
	"name" text NOT NULL,
	phone varchar NOT NULL,
	zip varchar NULL,
	city text NOT NULL,
	address text NOT NULL,
	region text NOT NULL,
	email text NOT NULL,
	id serial4 NOT NULL
);

CREATE TABLE public.payment (
	"transaction" varchar NOT NULL ,
	request_id varchar NULL,
	currency varchar NOT NULL,
	provider varchar NOT NULL,
	amount numeric NOT NULL,
	payment_dt numeric NOT NULL,
	bank varchar NOT NULL,
	delivery_cost numeric NOT NULL DEFAULT 0,
	goods_total numeric NOT NULL DEFAULT 0,
	custom_fee numeric NOT NULL DEFAULT 0,
	id serial4 NOT NULL
);

CREATE TABLE public.items (
	chrt_id numeric NOT NULL,
	track_number varchar NOT NULL,
	price numeric NOT NULL,
	rid varchar NOT NULL,
	"name" text NOT NULL,
	sale int2 NULL DEFAULT 0,
	"size" varchar NULL DEFAULT 0,
	total_price numeric NOT NULL,
	nm_id numeric NULL,
	brand text NULL,
	status int4 NULL,
	id int4 NOT NULL DEFAULT nextval('seq'::regclass)
);

CREATE TABLE public.orders (
	order_uid varchar NOT NULL,
	track_number varchar NOT NULL,
	entry varchar NULL,
	items_id _int4 NOT NULL,
	locale varchar NULL DEFAULT 'en'::character varying,
	internal_signature varchar NULL,
	customer_id varchar NULL,
	delivery_service varchar NULL,
	shardkey varchar NULL,
	sm_id int4 NULL,
	date_created date NOT NULL,
	oof_shard varchar NULL,
	delivery_id int4 NOT NULL,
	payment_id int4 NOT NULL,
	id serial4 NOT NULL
);