import { openDB, type IDBPDatabase } from "idb";

import type { StockRow } from "#/db/schema";

export type { StockRow } from "#/db/schema";

const DB_NAME = "stock-portal";
const STORE = "rows";
const META = "meta";

let dbPromise: Promise<IDBPDatabase> | null = null;
function getDb() {
  if (!dbPromise) {
    dbPromise = openDB(DB_NAME, 1, {
      upgrade(db) {
        const s = db.createObjectStore(STORE, { keyPath: "id" });
        s.createIndex("storeCode", "storeCode");
        s.createIndex("barcode", "barcode");
        s.createIndex("groupCode", "groupCode");
        s.createIndex("dptCode", "dptCode");
        db.createObjectStore(META);
      },
    });
  }
  return dbPromise;
}

const num = (v: unknown): number => {
  if (v === null || v === undefined || v === "") return 0;
  const n = Number(String(v).replace(/,/g, ""));
  return Number.isFinite(n) ? n : 0;
};
const str = (v: unknown): string =>
  v === null || v === undefined ? "" : String(v).trim();

// Barcodes can arrive as scientific notation strings (e.g. "4.89104E+12") when
// Excel autoconverted them. Normalize back to a plain integer string when possible.
const normalizeBarcode = (v: unknown): string => {
  const raw = str(v);
  if (!raw) return "";
  if (/^[0-9]+(\.[0-9]+)?[eE][+-]?[0-9]+$/.test(raw)) {
    const n = Number(raw);
    if (Number.isFinite(n)) return BigInt(Math.round(n)).toString();
  }
  return raw;
};

// CSV header lookup that tolerates BOM, whitespace, and duplicate columns.
// PapaParse renames duplicate headers (e.g. "店鋪編號" → "店鋪編號_1"); when the
// first occurrence is empty, we still want the value from the renamed sibling.
function pick(r: Record<string, unknown>, key: string): unknown {
  let fallback: unknown = "";
  for (const k of Object.keys(r)) {
    const norm = k
      .replace(/^\uFEFF/, "")
      .trim()
      .replace(/_\d+$/, "");
    if (norm === key) {
      const v = r[k];
      if (v !== null && v !== undefined && String(v).trim() !== "") return v;
      fallback = v ?? fallback;
    }
  }
  return fallback;
}

export function mapCsvRow(r: Record<string, unknown>): StockRow | null {
  const periodFrom = str(pick(r, "期間From"));
  const periodTo = str(pick(r, "期間To"));
  const storeCode = str(pick(r, "店鋪編號"));
  const barcode = normalizeBarcode(pick(r, "條形碼"));
  const productName = str(pick(r, "商品名"));
  if (!storeCode || !barcode || !productName) return null;
  return {
    id: `${periodFrom}|${periodTo}|${storeCode}|${barcode}|${productName}`,
    periodFrom,
    periodTo,
    storeCode,
    storeName: str(pick(r, "店鋪名")),
    groupCode: str(pick(r, "組")),
    groupName: str(pick(r, "組名")),
    divCode: str(pick(r, "DIV")),
    divName: str(pick(r, "DIV名")),
    dptCode: str(pick(r, "DPT")),
    dptName: str(pick(r, "DPT名")),
    lineCode: str(pick(r, "Line")),
    lineName: str(pick(r, "Line名")),
    classCode: str(pick(r, "Class")),
    className: str(pick(r, "Class編號名")),
    barcode,
    productName,
    productNameJa: str(pick(r, "商品名(日本名)")),
    stockQty: num(pick(r, "庫存數量")),
    uploadedAt: Date.now(),
  };
}

export async function upsertRows(rows: StockRow[]) {
  const db = await getDb();
  const tx = db.transaction(STORE, "readwrite");
  for (const row of rows) tx.store.put(row);
  await tx.done;
}

export async function getAllRows(): Promise<StockRow[]> {
  const db = await getDb();
  return db.getAll(STORE) as Promise<StockRow[]>;
}

export async function clearAll() {
  const db = await getDb();
  await db.clear(STORE);
  await db.clear(META);
}

export async function setMeta(key: string, value: unknown) {
  const db = await getDb();
  await db.put(META, value, key);
}

export async function getMeta<T = unknown>(
  key: string
): Promise<T | undefined> {
  const db = await getDb();
  return db.get(META, key) as Promise<T | undefined>;
}
