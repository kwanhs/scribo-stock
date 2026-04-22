CREATE TABLE IF NOT EXISTS "stock_rows" (
	"id" serial PRIMARY KEY NOT NULL,
	"period_from" varchar(64) NOT NULL,
	"period_to" varchar(64) NOT NULL,
	"store_code" text NOT NULL,
	"store_name" text DEFAULT '' NOT NULL,
	"group_code" text DEFAULT '' NOT NULL,
	"group_name" text DEFAULT '' NOT NULL,
	"div_code" text DEFAULT '' NOT NULL,
	"div_name" text DEFAULT '' NOT NULL,
	"dpt_code" text DEFAULT '' NOT NULL,
	"dpt_name" text DEFAULT '' NOT NULL,
	"line_code" text DEFAULT '' NOT NULL,
	"line_name" text DEFAULT '' NOT NULL,
	"class_code" text DEFAULT '' NOT NULL,
	"class_name" text DEFAULT '' NOT NULL,
	"barcode" text NOT NULL,
	"product_name" text DEFAULT '' NOT NULL,
	"product_name_ja" text DEFAULT '' NOT NULL,
	"stock_qty" double precision DEFAULT 0 NOT NULL,
	"uploaded_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "stock_rows_period_store_barcode_uidx" ON "stock_rows" ("period_from","period_to","store_code","barcode");
