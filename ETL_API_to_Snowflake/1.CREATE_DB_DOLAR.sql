-- Crear la base de datos Ephifany si no existe
CREATE DATABASE IF NOT EXISTS Ephifany;

-- Cambiar al contexto de la base de datos Ephifany
USE Ephifany;

-- Crear el esquema public si no existe
CREATE SCHEMA IF NOT EXISTS public;

-- Crear la tabla ventas en el esquema public si no existe
CREATE TABLE IF NOT EXISTS dolar (
  "date_operation" DATE,
  "blue_buy" DECIMAL(10, 2),
  "oficial_buy" DECIMAL(10, 2),
  "blue_sell" DECIMAL(10, 2),
  "oficial_sell" DECIMAL(10, 2),
  "country" STRING
);
