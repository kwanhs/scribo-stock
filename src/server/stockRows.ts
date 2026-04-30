import { and, asc, desc, eq, or, sql } from 'drizzle-orm'

import { getDb } from '#/db/index'
import { stockRows, type StockRow, type StockRowInsert, type StockRowSelect } from '#/db/schema'

export type StockRowsInsertResult =
  | { ok: true; rowsInserted: number }
  | { ok: false; reason: 'duplicate_in_file'; duplicateKey: string }
  | { ok: false; reason: 'duplicate_in_db'; duplicateKey: string }

export type LatestStockSnapshot = {
  periodFrom: string
  periodTo: string
}

export type StockSummary = {
  stores: number
  products: number
  qty: number
  rows: number
  periodFrom: string | null
  periodTo: string | null
}

export type ProductSearchResult = {
  barcode: string
  productName: string
  productNameJa: string
  totalQty: number
  byStore: {
    storeCode: string
    storeName: string
    qty: number
  }[]
}

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
  const db = getDb()
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
  const db = getDb()
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
 * Insert rows into D1. Rejects if the payload repeats a natural key,
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

export async function deleteAllStockRows(): Promise<number> {
  const db = getDb()
  const deleted = await db.delete(stockRows).returning({ id: stockRows.id })
  return deleted.length
}

export type ListStockRowsParams = {
  limit?: number
  offset?: number
  storeCode?: string
  periodFrom?: string
  periodTo?: string
}

const MAX_LIMIT = 20_000

async function getLatestSnapshot(): Promise<LatestStockSnapshot | null> {
  const db = getDb()
  const latest = await db
    .select({
      periodFrom: stockRows.periodFrom,
      periodTo: stockRows.periodTo,
    })
    .from(stockRows)
    .orderBy(desc(stockRows.uploadedAt), desc(stockRows.id))
    .limit(1)

  if (latest.length === 0) return null
  return latest[0]!
}

function buildSnapshotWhere(snapshot: LatestStockSnapshot) {
  return and(
    eq(stockRows.periodFrom, snapshot.periodFrom),
    eq(stockRows.periodTo, snapshot.periodTo),
  )
}

export async function listStockRows(params: ListStockRowsParams = {}): Promise<StockRowSelect[]> {
  const db = getDb()
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

export async function getStockSummary(): Promise<StockSummary> {
  const db = getDb()
  const latestSnapshot = await getLatestSnapshot()

  if (!latestSnapshot) {
    return {
      stores: 0,
      products: 0,
      qty: 0,
      rows: 0,
      periodFrom: null,
      periodTo: null,
    }
  }

  const where = buildSnapshotWhere(latestSnapshot)
  const summary = await db
    .select({
      stores: sql<number>`count(distinct ${stockRows.storeCode})`,
      products: sql<number>`count(distinct ${stockRows.barcode})`,
      qty: sql<number>`coalesce(sum(${stockRows.stockQty}), 0)`,
      rows: sql<number>`count(*)`,
    })
    .from(stockRows)
    .where(where)

  const row = summary[0]
  return {
    stores: Number(row?.stores ?? 0),
    products: Number(row?.products ?? 0),
    qty: Number(row?.qty ?? 0),
    rows: Number(row?.rows ?? 0),
    periodFrom: latestSnapshot.periodFrom,
    periodTo: latestSnapshot.periodTo,
  }
}

function escapeLike(input: string) {
  return input.replaceAll('\\', '\\\\').replaceAll('%', '\\%').replaceAll('_', '\\_')
}

export async function searchProducts(params: {
  query?: string
  limit?: number
}): Promise<{
  query: string
  periodFrom: string | null
  periodTo: string | null
  products: ProductSearchResult[]
}> {
  const db = getDb()
  const latestSnapshot = await getLatestSnapshot()
  const query = params.query?.trim() ?? ''
  const limit = Math.min(Math.max(Number(params.limit) || 50, 1), 200)

  if (!latestSnapshot) {
    return {
      query,
      periodFrom: null,
      periodTo: null,
      products: [],
    }
  }

  if (!query) {
    return {
      query,
      periodFrom: latestSnapshot.periodFrom,
      periodTo: latestSnapshot.periodTo,
      products: [],
    }
  }

  const like = `%${escapeLike(query.toLowerCase())}%`
  const where = and(
    buildSnapshotWhere(latestSnapshot),
    or(
      sql`lower(${stockRows.barcode}) like ${like} escape '\\'`,
      sql`lower(${stockRows.productName}) like ${like} escape '\\'`,
      sql`lower(${stockRows.productNameJa}) like ${like} escape '\\'`,
    ),
  )

  const productRows = await db
    .select({
      barcode: stockRows.barcode,
      productName: sql<string>`max(${stockRows.productName})`,
      productNameJa: sql<string>`max(${stockRows.productNameJa})`,
      totalQty: sql<number>`coalesce(sum(${stockRows.stockQty}), 0)`,
    })
    .from(stockRows)
    .where(where)
    .groupBy(stockRows.barcode)
    .orderBy(asc(stockRows.barcode))
    .limit(limit)

  if (productRows.length === 0) {
    return {
      query,
      periodFrom: latestSnapshot.periodFrom,
      periodTo: latestSnapshot.periodTo,
      products: [],
    }
  }

  const barcodes = productRows.map((row) => row.barcode)
  const storeRows = await db
    .select({
      barcode: stockRows.barcode,
      storeCode: stockRows.storeCode,
      storeName: sql<string>`max(${stockRows.storeName})`,
      qty: sql<number>`coalesce(sum(${stockRows.stockQty}), 0)`,
    })
    .from(stockRows)
    .where(
      and(
        buildSnapshotWhere(latestSnapshot),
        or(...barcodes.map((barcode) => eq(stockRows.barcode, barcode))),
      ),
    )
    .groupBy(stockRows.barcode, stockRows.storeCode)
    .orderBy(asc(stockRows.barcode), asc(stockRows.storeCode))

  const storesByBarcode = new Map<string, ProductSearchResult['byStore']>()
  for (const row of storeRows) {
    const stores = storesByBarcode.get(row.barcode) ?? []
    stores.push({
      storeCode: row.storeCode,
      storeName: row.storeName,
      qty: Number(row.qty ?? 0),
    })
    storesByBarcode.set(row.barcode, stores)
  }

  return {
    query,
    periodFrom: latestSnapshot.periodFrom,
    periodTo: latestSnapshot.periodTo,
    products: productRows.map((row) => ({
      barcode: row.barcode,
      productName: row.productName,
      productNameJa: row.productNameJa,
      totalQty: Number(row.totalQty ?? 0),
      byStore: storesByBarcode.get(row.barcode) ?? [],
    })),
  }
}

export async function getStockRowById(id: number): Promise<StockRowSelect | undefined> {
  const db = getDb()
  const rows = await db.select().from(stockRows).where(eq(stockRows.id, id)).limit(1)
  return rows[0]
}
