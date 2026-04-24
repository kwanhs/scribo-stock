import { createFileRoute } from '@tanstack/react-router'
import { json } from '@tanstack/react-router/ssr/client'
import Papa from 'papaparse'

import { insertStockRowSchema } from '#/db/schema.zod'
import { mapCsvRow } from '#/lib/stockDb'
import { redactStockCsvWithPython } from '#/server/redactStockCsv'
import { clientStockRowToInsert, insertStockRowsStrict } from '#/server/stockRows'

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

        const inserts = []
        for (const r of parsed.data) {
          const row = mapCsvRow(r)
          if (!row) continue
          const insert = clientStockRowToInsert(row)
          const check = insertStockRowSchema.safeParse(insert)
          if (!check.success) continue
          inserts.push(insert)
        }

        if (!inserts.length) {
          return json(
            {
              error: 'No valid stock rows after redaction.',
              rowsParsed: parsed.data.length,
              rowsInserted: 0,
            },
            { status: 422 },
          )
        }

        const result = await insertStockRowsStrict(inserts)
        if (!result.ok) {
          if (result.reason === 'duplicate_in_file') {
            return json(
              {
                error: 'Duplicate rows in file.',
                detail:
                  'The same period, store, and barcode appears more than once. Remove duplicates before uploading.',
                duplicateKey: result.duplicateKey,
              },
              { status: 409 },
            )
          }
          return json(
            {
              error: 'Data already uploaded.',
              detail:
                'One or more rows match existing data for the same period, store, and barcode. Remove overlapping rows or use a different file.',
              duplicateKey: result.duplicateKey,
            },
            { status: 409 },
          )
        }

        return json({
          ok: true,
          rowsParsed: parsed.data.length,
          rowsUniqueInFile: inserts.length,
          rowsInserted: result.rowsInserted,
        })
      },
    },
  },
})
