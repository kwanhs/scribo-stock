import { and, desc, eq, or } from 'drizzle-orm'

import { db } from '#/db/index'
import { stockRows, type StockRow, type StockRowInsert, type StockRowSelect } from '#/db/schema'

export type StockRowsInsertResult =
  | { ok: true; rowsInserted: number }
  | { ok: false; reason: 'duplicate_in_file'; duplicateKey: string }
  | { ok: false; reason: 'duplicate_in_db'; duplicateKey: string }

export function naturalKey(row: {
  periodFrom: string
  periodTo: string
  storeCode: string
  barcode: string
}) {
  return `${row.periodFrom}|${row.periodTo}|${row.storeCode}|${row.barcode}`
}

async function findExistingNaturalKeyInDb(
  rows: Pick<StockRowInsert, 'periodFrom' | 'periodTo' | 'storeCode' | 'barcode'>[],
): Promise<string | undefined> {
  const OR_CHUNK = 80
  for (let i = 0; i < rows.length; i += OR_CHUNK) {
    const batch = rows.slice(i, i + OR_CHUNK)
    const condition = or(
      ...batch.map((r) =>
        and(
          eq(stockRows.periodFrom, r.periodFrom),
          eq(stockRows.periodTo, r.periodTo),
          eq(stockRows.storeCode, r.storeCode),
          eq(stockRows.barcode, r.barcode),
        ),
      ),
    )
    if (!condition) continue
    const hit = await db
      .select({
        periodFrom: stockRows.periodFrom,
        periodTo: stockRows.periodTo,
        storeCode: stockRows.storeCode,
        barcode: stockRows.barcode,
      })
      .from(stockRows)
      .where(condition)
      .limit(1)
    if (hit.length > 0) {
      const r = hit[0]!
      return naturalKey(r)
    }
  }
  return undefined
}

async function insertChunks(values: StockRowInsert[]) {
  if (!values.length) return 0
  const CHUNK = 500
  let inserted = 0
  for (let i = 0; i < values.length; i += CHUNK) {
    const batch = values.slice(i, i + CHUNK)
    const res = await db.insert(stockRows).values(batch).returning({ id: stockRows.id })
    inserted += res.length
  }
  return inserted
}

/**
 * Insert rows into Postgres. Rejects if the file payload repeats a natural key,
 * or if any key already exists (no duplicate uploads vs DB).
 */
export async function insertStockRowsStrict(
  rows: StockRowInsert[],
): Promise<StockRowsInsertResult> {
  if (!rows.length) return { ok: true, rowsInserted: 0 }

  const seen = new Set<string>()
  for (const r of rows) {
    const k = naturalKey(r)
    if (seen.has(k)) {
      return { ok: false, reason: 'duplicate_in_file', duplicateKey: k }
    }
    seen.add(k)
  }

  const existingKey = await findExistingNaturalKeyInDb(rows)
  if (existingKey !== undefined) {
    return { ok: false, reason: 'duplicate_in_db', duplicateKey: existingKey }
  }

  const rowsInserted = await insertChunks(rows)
  return { ok: true, rowsInserted }
}

export function clientStockRowToInsert(row: StockRow): StockRowInsert {
  return {
    periodFrom: row.periodFrom,
    periodTo: row.periodTo,
    storeCode: row.storeCode,
    storeName: row.storeName,
    groupCode: row.groupCode,
    groupName: row.groupName,
    divCode: row.divCode,
    divName: row.divName,
    dptCode: row.dptCode,
    dptName: row.dptName,
    lineCode: row.lineCode,
    lineName: row.lineName,
    classCode: row.classCode,
    className: row.className,
    barcode: row.barcode,
    productName: row.productName,
    productNameJa: row.productNameJa,
    stockQty: row.stockQty,
    uploadedAt: new Date(row.uploadedAt),
  }
}

export async function insertClientStockRows(rows: StockRow[]): Promise<StockRowsInsertResult> {
  const inserts = rows.map(clientStockRowToInsert)
  return insertStockRowsStrict(inserts)
}

export type ListStockRowsParams = {
  limit?: number
  offset?: number
  storeCode?: string
  periodFrom?: string
  periodTo?: string
}

const MAX_LIMIT = 20_000

export async function listStockRows(params: ListStockRowsParams = {}): Promise<StockRowSelect[]> {
  const limit = Math.min(Math.max(Number(params.limit) || 5000, 1), MAX_LIMIT)
  const offset = Math.max(Number(params.offset) || 0, 0)

  const conditions = []
  if (params.storeCode) conditions.push(eq(stockRows.storeCode, params.storeCode))
  if (params.periodFrom) conditions.push(eq(stockRows.periodFrom, params.periodFrom))
  if (params.periodTo) conditions.push(eq(stockRows.periodTo, params.periodTo))

  const where =
    conditions.length === 0
      ? undefined
      : conditions.length === 1
        ? conditions[0]
        : and(...conditions)

  if (where) {
    return await db
      .select()
      .from(stockRows)
      .where(where)
      .orderBy(desc(stockRows.id))
      .limit(limit)
      .offset(offset)
  }
  return await db
    .select()
    .from(stockRows)
    .orderBy(desc(stockRows.id))
    .limit(limit)
    .offset(offset)
}

/** Single row by DB id (if you need it). */
export async function getStockRowById(id: number): Promise<StockRowSelect | undefined> {
  const rows = await db.select().from(stockRows).where(eq(stockRows.id, id)).limit(1)
  return rows[0]
}
