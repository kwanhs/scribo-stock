/* eslint-disable react-refresh/only-export-components -- route registers HTTP handlers and server RPC exports */
import { createFileRoute } from '@tanstack/react-router'
import { json } from '@tanstack/react-router/ssr/client'
import { createServerFn } from '@tanstack/react-start'
import { z } from 'zod'

import type { StockRowInsert } from '#/db/schema'
import { insertStockRowSchema } from '#/db/schema.zod'
import { insertStockRowsStrict, listStockRows } from '#/server/stockRows'

const listQuerySchema = z.object({
  limit: z.coerce.number().int().min(1).max(20_000).optional(),
  offset: z.coerce.number().int().min(0).optional(),
  storeCode: z.string().min(1).optional(),
  periodFrom: z.string().min(1).optional(),
  periodTo: z.string().min(1).optional(),
})

const insertBodySchema = z.object({
  rows: z.array(z.record(z.unknown())).min(1).max(10_000),
})

function parseStockRowInserts(rows: Record<string, unknown>[]): StockRowInsert[] {
  return rows.map((raw) => {
    const copy = { ...raw }
    if (copy.uploadedAt != null && copy.uploadedAt !== '') {
      copy.uploadedAt = new Date(String(copy.uploadedAt))
    } else {
      copy.uploadedAt = new Date()
    }
    return insertStockRowSchema.parse(copy) as StockRowInsert
  })
}

const listFnInputSchema = z
  .object({
    limit: z.number().int().min(1).max(20_000).optional(),
    offset: z.number().int().min(0).optional(),
    storeCode: z.string().min(1).optional(),
    periodFrom: z.string().min(1).optional(),
    periodTo: z.string().min(1).optional(),
  })
  .optional()

/** Typed RPC: list stock rows from Postgres (newest first). */
export const listStockRowsFn = createServerFn({
  method: 'POST',
})
  .inputValidator((data: unknown) => listFnInputSchema.parse(data ?? {}))
  .handler(async ({ data }) => {
    const rows = await listStockRows(data ?? {})
    return { ok: true as const, rows, count: rows.length }
  })

/** Typed RPC: insert rows; rejects duplicate keys in payload or DB. */
export const insertStockRowsFn = createServerFn({
  method: 'POST',
})
  .inputValidator((data: unknown) => insertBodySchema.parse(data))
  .handler(async ({ data }) => {
    const rows = parseStockRowInserts(data.rows)
    const result = await insertStockRowsStrict(rows)
    if (!result.ok) {
      return {
        ok: false as const,
        reason: result.reason,
        duplicateKey: result.duplicateKey,
      }
    }
    return { ok: true as const, rowsInserted: result.rowsInserted }
  })

export const Route = createFileRoute('/api/stock/rows')({
  component: () => null,
  server: {
    handlers: {
      GET: async ({ request }) => {
        const url = new URL(request.url)
        const parsed = listQuerySchema.safeParse({
          limit: url.searchParams.get('limit') ?? undefined,
          offset: url.searchParams.get('offset') ?? undefined,
          storeCode: url.searchParams.get('storeCode') ?? undefined,
          periodFrom: url.searchParams.get('periodFrom') ?? undefined,
          periodTo: url.searchParams.get('periodTo') ?? undefined,
        })
        if (!parsed.success) {
          return json(
            { error: 'Invalid query parameters.', detail: parsed.error.flatten() },
            { status: 400 },
          )
        }
        const rows = await listStockRows(parsed.data)
        return json({ ok: true, rows, count: rows.length })
      },

      POST: async ({ request }) => {
        let body: unknown
        try {
          body = await request.json()
        } catch {
          return json({ error: 'Invalid JSON body.' }, { status: 400 })
        }

        const parsed = insertBodySchema.safeParse(body)
        if (!parsed.success) {
          return json(
            { error: 'Invalid request body.', detail: parsed.error.flatten() },
            { status: 400 },
          )
        }

        const rows = parseStockRowInserts(parsed.data.rows)

        const result = await insertStockRowsStrict(rows)
        if (!result.ok) {
          if (result.reason === 'duplicate_in_file') {
            return json(
              {
                error: 'Duplicate rows in payload.',
                detail:
                  'The same period, store, and barcode appears more than once. Send each key only once.',
                duplicateKey: result.duplicateKey,
              },
              { status: 409 },
            )
          }
          return json(
            {
              error: 'Data already uploaded.',
              detail:
                'One or more rows match existing data for the same period, store, and barcode.',
              duplicateKey: result.duplicateKey,
            },
            { status: 409 },
          )
        }

        return json({
          ok: true,
          rowsInserted: result.rowsInserted,
        })
      },
    },
  },
})
