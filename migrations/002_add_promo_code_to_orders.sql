-- Migration: 002_add_promo_code_to_orders.sql
-- Purpose: Support promo_code in source orders

ALTER TABLE bronze_app.orders
ADD COLUMN IF NOT EXISTS promo_code TEXT;