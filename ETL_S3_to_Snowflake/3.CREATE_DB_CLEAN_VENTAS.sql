-- Crear una base de datos si no existe
CREATE DATABASE IF NOT EXISTS staging;

-- Utilizar la base de datos staging
USE DATABASE staging;

-- Crear una tabla en Snowflake
CREATE OR REPLACE TABLE ventas (
    order_id INTEGER,
    product STRING,
    quantity_ordered INTEGER,
    price_each NUMERIC(10, 2),
    order_date TIMESTAMP,
    purchase_address STRING,
    street_number STRING,
    street_name STRING,
    city STRING,
    state STRING,
    postal_code STRING
);

-- Cargar datos en la tabla desde una ubicación externa (por ejemplo, un archivo CSV)
COPY INTO ventas
FROM @mi_ubicacion_externa/archivo.csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Nota: Debes reemplazar @mi_ubicacion_externa/archivo.csv con la ubicación real de tu archivo de datos.

-- Ahora, puedes realizar cualquier consulta adicional o manipulación de datos en la tabla 'ventas'.
SELECT 
    v.ORDER_ID,
    v.PRODUCT,
    v.QUANTITY_ORDERED,
    v.PRICE_EACH,
    SPLIT_PART(v.ORDER_DATE, ' ', 1) AS ORDER_DATE,
    v.PURCHASE_ADDRESS,
    SPLIT_PART(v.PURCHASE_ADDRESS, ' ', 1) AS street_number,
    SUBSTRING(SPLIT_PART(v.PURCHASE_ADDRESS, ',', 1), 5) AS street_name,
    SPLIT_PART(v.PURCHASE_ADDRESS, ',', 2) AS city,
    SPLIT_PART(SPLIT_PART(v.PURCHASE_ADDRESS, ',', 3), ' ', 2) AS state,
    SPLIT_PART(SPLIT_PART(v.PURCHASE_ADDRESS, ',', 3), ' ', 3) AS postal_code
FROM EPHIFANY.PUBLIC.VENTAS v
WHERE v.ORDER_ID IS NOT NULL
