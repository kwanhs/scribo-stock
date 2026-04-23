import {
  doublePrecision,
  pgTable,
  serial,
  text,
  timestamp,
  uniqueIndex,
  varchar,
} from 'drizzle-orm/pg-core'

export const todos = pgTable('todos', {
  id: serial().primaryKey(),
  title: text().notNull(),
  createdAt: timestamp('created_at').defaultNow(),
})

export type Todo = typeof todos.$inferSelect
export type TodoInsert = typeof todos.$inferInsert

/** Stock snapshot rows aligned with CSV import / client `StockRow` shape. */
export const stockRows = pgTable(
  'stock_rows',
  {
    id: serial('id').primaryKey(),
    periodFrom: varchar('period_from', { length: 64 }).notNull(),
    periodTo: varchar('period_to', { length: 64 }).notNull(),
    storeCode: text('store_code').notNull(),
    storeName: text('store_name').notNull().default(''),
    groupCode: text('group_code').notNull().default(''),
    groupName: text('group_name').notNull().default(''),
    divCode: text('div_code').notNull().default(''),
    divName: text('div_name').notNull().default(''),
    dptCode: text('dpt_code').notNull().default(''),
    dptName: text('dpt_name').notNull().default(''),
    lineCode: text('line_code').notNull().default(''),
    lineName: text('line_name').notNull().default(''),
    classCode: text('class_code').notNull().default(''),
    className: text('class_name').notNull().default(''),
    barcode: text('barcode').notNull(),
    productName: text('product_name').notNull().default(''),
    productNameJa: text('product_name_ja').notNull().default(''),
    stockQty: doublePrecision('stock_qty').notNull().default(0),
    uploadedAt: timestamp('uploaded_at', { withTimezone: true }).defaultNow().notNull(),
  },
  (t) => [
    uniqueIndex('stock_rows_period_store_barcode_uidx').on(
      t.periodFrom,
      t.periodTo,
      t.storeCode,
      t.barcode,
    ),
  ],
)

/** Row as stored in Postgres (Drizzle-inferred). */
export type StockRowSelect = typeof stockRows.$inferSelect
export type StockRowInsert = typeof stockRows.$inferInsert

/**
 * Client / IndexedDB / CSV-mapped row: composite key string and epoch ms for `uploadedAt`
 * (Drizzle select shape otherwise).
 */
export type StockRow = Omit<StockRowSelect, 'id' | 'uploadedAt'> & {
  id: string
  uploadedAt: number
}
