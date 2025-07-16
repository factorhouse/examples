CREATE SCHEMA ecommerce;
GRANT ALL ON SCHEMA ecommerce TO develop;

-- change search_path on a connection-level
SET search_path TO ecommerce;

-- change search_path on a database-level
ALTER database "develop" SET search_path TO ecommerce;

-- create a publication for all tables in the ecommerce schema
CREATE PUBLICATION cdc_publication FOR TABLES IN SCHEMA ecommerce;
