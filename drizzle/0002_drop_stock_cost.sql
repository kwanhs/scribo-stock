-- Safe if column was never created (fresh installs use 0001 without stock_cost).
ALTER TABLE "stock_rows" DROP COLUMN IF EXISTS "stock_cost";
