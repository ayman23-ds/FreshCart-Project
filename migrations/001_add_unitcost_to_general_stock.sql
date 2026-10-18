-- Migration: 001_add_unitcost_to_general_stock.sql
-- Database: freshcart_warehouse
-- Schema: bronze_suppliers
-- Purpose: Support GENERAL supplier schema drift.
--
-- Old CSV:
-- Supplier;Product;Store;Date;Qty;MinQty
--
-- New CSV:
-- Supplier;Product;Store;Date;QtyOnHand;MinQty;UnitCost
--
-- UnitCost is nullable because historical files
-- do not contain this column.
-- TEXT preserves the raw supplier value in Bronze.


ALTER TABLE bronze_suppliers.general_stock
ADD COLUMN IF NOT EXISTS "UnitCost" TEXT;

COMMENT ON COLUMN bronze_suppliers.general_stock."UnitCost"
IS 'Raw unit cost from GENERAL supplier CSV; NULL when not provided.';

