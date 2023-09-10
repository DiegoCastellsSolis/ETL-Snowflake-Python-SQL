-- Crear la base de datos Ephifany
CREATE OR REPLACE DATABASE Ephifany;

-- Cambiar al contexto de la base de datos Ephifany
USE DATABASE Ephifany;

-- Crear el esquema public
CREATE OR REPLACE SCHEMA public;

-- Crear la tabla ventas en el esquema public
CREATE OR REPLACE TABLE public.ventas (
  "Order ID" STRING,
  "Product" STRING,
  "Quantity Ordered" INT,
  "Price Each" DECIMAL(10, 2),
  "Order Date" TIMESTAMP,
  "Purchase Address" STRING
);