DROP TABLE IF EXISTS dds.dm_couriers;

CREATE TABLE dds.dm_couriers (
	id serial4 NOT NULL,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	CONSTRAINT dm_couriers_courier_id_key UNIQUE (courier_id),
	CONSTRAINT dm_couriers_pkey PRIMARY KEY (id)
);