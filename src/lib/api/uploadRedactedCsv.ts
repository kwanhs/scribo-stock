/**
 * POST the raw CSV to the server: Python redacts cost/price columns, then rows are inserted into Postgres (rejects duplicates in file or vs DB).
 */
export async function uploadRedactedStockCsv(file: File): Promise<{
  ok: true
  rowsParsed: number
  rowsUniqueInFile: number
  rowsInserted: number
}> {
  const form = new FormData()
  form.set('file', file)
  const res = await fetch('/api/csv/redact-upload', {
    method: 'POST',
    body: form,
  })
  const body = (await res.json()) as Record<string, unknown>
  if (!res.ok) {
    const msg =
      typeof body.error === 'string'
        ? body.error
        : `Upload failed (${res.status})`
    const detail = typeof body.detail === 'string' ? `: ${body.detail}` : ''
    throw new Error(`${msg}${detail}`)
  }
  return {
    ok: true,
    rowsParsed: Number(body.rowsParsed) || 0,
    rowsUniqueInFile: Number(body.rowsUniqueInFile) || 0,
    rowsInserted: Number(body.rowsInserted) || 0,
  }
}
