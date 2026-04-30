import type { StockRowInsert, StockRowSelect } from '@/db/schema'

export type StockSummaryResponse = {
  ok: true
  stores: number
  products: number
  qty: number
  rows: number
  periodFrom: string | null
  periodTo: string | null
}

export type ProductSearchResponse = {
  ok: true
  query: string
  periodFrom: string | null
  periodTo: string | null
  products: {
    barcode: string
    productName: string
    productNameJa: string
    totalQty: number
    byStore: {
      storeCode: string
      storeName: string
      qty: number
    }[]
  }[]
}

export type ListStockRowsQuery = {
  limit?: number
  offset?: number
  storeCode?: string
  periodFrom?: string
  periodTo?: string
}

export async function fetchStockRows(
  query: ListStockRowsQuery = {},
): Promise<{ ok: true; rows: StockRowSelect[]; count: number }> {
  const sp = new URLSearchParams()
  if (query.limit != null) sp.set('limit', String(query.limit))
  if (query.offset != null) sp.set('offset', String(query.offset))
  if (query.storeCode) sp.set('storeCode', query.storeCode)
  if (query.periodFrom) sp.set('periodFrom', query.periodFrom)
  if (query.periodTo) sp.set('periodTo', query.periodTo)
  const qs = sp.toString()
  const res = await fetch(`/api/stock/rows${qs ? `?${qs}` : ''}`)
  const body = (await res.json()) as Record<string, unknown>
  if (!res.ok) {
    const msg = typeof body.error === 'string' ? body.error : `Request failed (${res.status})`
    throw new Error(msg)
  }
  return {
    ok: true,
    rows: (body.rows as StockRowSelect[]) ?? [],
    count: Number(body.count) || 0,
  }
}

export async function fetchStockSummary(): Promise<StockSummaryResponse> {
  const res = await fetch('/api/stock/rows?mode=summary')
  const body = (await res.json()) as Record<string, unknown>
  if (!res.ok) {
    const msg = typeof body.error === 'string' ? body.error : `Request failed (${res.status})`
    throw new Error(msg)
  }
  return {
    ok: true,
    stores: Number(body.stores) || 0,
    products: Number(body.products) || 0,
    qty: Number(body.qty) || 0,
    rows: Number(body.rows) || 0,
    periodFrom: typeof body.periodFrom === 'string' ? body.periodFrom : null,
    periodTo: typeof body.periodTo === 'string' ? body.periodTo : null,
  }
}

export async function fetchProductSearch(query: {
  query?: string
  limit?: number
} = {}): Promise<ProductSearchResponse> {
  const sp = new URLSearchParams({ mode: 'search' })
  if (query.query) sp.set('query', query.query)
  if (query.limit != null) sp.set('limit', String(query.limit))
  const res = await fetch(`/api/stock/rows?${sp.toString()}`)
  const body = (await res.json()) as Record<string, unknown>
  if (!res.ok) {
    const msg = typeof body.error === 'string' ? body.error : `Request failed (${res.status})`
    throw new Error(msg)
  }

  const products = Array.isArray(body.products) ? body.products : []
  return {
    ok: true,
    query: typeof body.query === 'string' ? body.query : '',
    periodFrom: typeof body.periodFrom === 'string' ? body.periodFrom : null,
    periodTo: typeof body.periodTo === 'string' ? body.periodTo : null,
    products: products.map((product) => {
      const p = product as Record<string, unknown>
      const byStore = Array.isArray(p.byStore) ? p.byStore : []
      return {
        barcode: typeof p.barcode === 'string' ? p.barcode : '',
        productName: typeof p.productName === 'string' ? p.productName : '',
        productNameJa: typeof p.productNameJa === 'string' ? p.productNameJa : '',
        totalQty: Number(p.totalQty) || 0,
        byStore: byStore.map((store) => {
          const s = store as Record<string, unknown>
          return {
            storeCode: typeof s.storeCode === 'string' ? s.storeCode : '',
            storeName: typeof s.storeName === 'string' ? s.storeName : '',
            qty: Number(s.qty) || 0,
          }
        }),
      }
    }),
  }
}

export async function postStockRows(rows: StockRowInsert[]): Promise<{
  ok: true
  rowsInserted: number
}> {
  const res = await fetch('/api/stock/rows', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ rows }),
  })
  const body = (await res.json()) as Record<string, unknown>
  if (!res.ok) {
    const msg =
      typeof body.error === 'string'
        ? body.error
        : `Insert failed (${res.status})`
    const detail = typeof body.detail === 'string' ? `: ${body.detail}` : ''
    const key =
      typeof body.duplicateKey === 'string' ? ` (${body.duplicateKey})` : ''
    throw new Error(`${msg}${detail}${key}`)
  }
  return { ok: true, rowsInserted: Number(body.rowsInserted) || 0 }
}

export async function deleteStockRows(): Promise<{ ok: true; deleted: number }> {
  const res = await fetch('/api/stock/rows', {
    method: 'DELETE',
  })
  const body = (await res.json()) as Record<string, unknown>
  if (!res.ok) {
    const msg = typeof body.error === 'string' ? body.error : `Delete failed (${res.status})`
    throw new Error(msg)
  }
  return { ok: true, deleted: Number(body.deleted) || 0 }
}
