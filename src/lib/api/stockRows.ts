import type { StockRowInsert, StockRowSelect } from '@/db/schema'

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
