import { createFileRoute } from '@tanstack/react-router'
import { json } from '@tanstack/react-router/ssr/client'
import Papa from 'papaparse'
import { and, eq, or } from 'drizzle-orm'

import { db } from '#/db/index'
import { stockRows } from '#/db/schema'
import { insertStockRowSchema } from '#/db/schema.zod'
import { mapCsvRow, type StockRow } from '#/lib/stockDb'
import { redactStockCsvWithPython } from '#/server/redactStockCsv'

function stockRowToInsert(row: StockRow) {
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

function naturalKey(row: ReturnType<typeof stockRowToInsert>) {
  return `${row.periodFrom}|${row.periodTo}|${row.storeCode}|${row.barcode}`
}

/** Reject upload if any row already exists (no partial inserts, no silent skips vs DB). */
async function findExistingNaturalKeyInDb(
  rows: ReturnType<typeof stockRowToInsert>[],
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
      return `${r.periodFrom}|${r.periodTo}|${r.storeCode}|${r.barcode}`
    }
  }
  return undefined
}

/** Insert rows after caller verified keys are absent from DB. */
async function insertStockRows(rows: ReturnType<typeof stockRowToInsert>[]) {
  if (!rows.length) return 0
  const CHUNK = 500
  let inserted = 0
  for (let i = 0; i < rows.length; i += CHUNK) {
    const batch = rows.slice(i, i + CHUNK)
    const res = await db.insert(stockRows).values(batch).returning({ id: stockRows.id })
    inserted += res.length
  }
  return inserted
}

export const Route = createFileRoute('/api/csv/redact-upload')({
  component: () => null,
  server: {
    handlers: {
      POST: async ({ request }) => {
        const ct = request.headers.get('content-type') || ''
        if (!ct.includes('multipart/form-data')) {
          return json({ error: 'Expected multipart/form-data with a file field.' }, { status: 415 })
        }

        let form: FormData
        try {
          form = await request.formData()
        } catch {
          return json({ error: 'Invalid form data.' }, { status: 400 })
        }

        const file = form.get('file')
        if (!(file instanceof File)) {
          return json({ error: 'Missing file field.' }, { status: 400 })
        }

        const rawText = await file.text()
        const redacted = redactStockCsvWithPython(rawText)
        if (!redacted.ok) {
          return json(
            { error: 'Redaction script failed.', detail: redacted.error },
            { status: 500 },
          )
        }

        const parsed = Papa.parse<Record<string, unknown>>(redacted.csv, {
          header: true,
          skipEmptyLines: true,
        })
        if (parsed.errors.length) {
          return json(
            {
              error: 'CSV parse error after redaction.',
              detail: parsed.errors.map((e) => e.message).join('; '),
            },
            { status: 400 },
          )
        }

        const byNaturalKey = new Map<string, ReturnType<typeof stockRowToInsert>>()
        for (const r of parsed.data) {
          const row = mapCsvRow(r)
          if (!row) continue
          const insert = stockRowToInsert(row)
          const check = insertStockRowSchema.safeParse(insert)
          if (!check.success) continue
          const k = naturalKey(insert)
          if (byNaturalKey.has(k)) {
            return json(
              {
                error: 'Duplicate rows in file.',
                detail:
                  'The same period, store, and barcode appears more than once. Remove duplicates before uploading.',
                duplicateKey: k,
              },
              { status: 409 },
            )
          }
          byNaturalKey.set(k, insert)
        }

        const mapped = [...byNaturalKey.values()]

        if (!mapped.length) {
          return json(
            {
              error: 'No valid stock rows after redaction.',
              rowsParsed: parsed.data.length,
              rowsInserted: 0,
            },
            { status: 422 },
          )
        }

        const existingKey = await findExistingNaturalKeyInDb(mapped)
        if (existingKey !== undefined) {
          return json(
            {
              error: 'Data already uploaded.',
              detail:
                'One or more rows match existing data for the same period, store, and barcode. Remove overlapping rows or use a different file.',
              duplicateKey: existingKey,
            },
            { status: 409 },
          )
        }

        const rowsInserted = await insertStockRows(mapped)

        return json({
          ok: true,
          rowsParsed: parsed.data.length,
          rowsUniqueInFile: mapped.length,
          rowsInserted,
        })
      },
    },
  },
})
