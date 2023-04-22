DROP TABLE IF EXISTS dds.dm_deliveries;

CREATE TABLE dds.dm_deliveries (
	id serial4 NOT NULL,
	delivery_id varchar NOT NULL,
	order_id int4 NOT NULL,
	courier_id int4 NOT NULL,
	delivery_ts timestamp NOT NULL,
	address varchar NOT NULL,
	rate numeric(14, 2) NOT NULL,
	tip_sum numeric(14, 2) NOT NULL,
	CONSTRAINT dm_deliveries_delivery_id_key UNIQUE (delivery_id),
	CONSTRAINT dm_deliveries_pkey PRIMARY KEY (id),
	CONSTRAINT dm_deliveries_rate_check CHECK (((rate >= (0)::numeric) AND (rate <= (5)::numeric))),
	CONSTRAINT dm_deliveries_tip_sum_check CHECK ((tip_sum >= (0)::numeric))
);

ALTER TABLE dds.dm_deliveries ADD CONSTRAINT fk_courier_id FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id);
ALTER TABLE dds.dm_deliveries ADD CONSTRAINT fk_order_id FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id);