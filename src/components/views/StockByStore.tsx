import { useMemo, useState } from "react";
import { Search } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { fmtInt } from "@/lib/format";
import type { StockRow } from "@/lib/stockDb";

interface Props { rows: StockRow[]; }

interface AggKey {
  storeCode: string; storeName: string; barcode: string; productName: string;
  qty: number;
}

export const StockByStore = ({ rows }: Props) => {
  const [query, setQuery] = useState("");
  const [store, setStore] = useState<string>("all");

  const stores = useMemo(() => {
    const map = new Map<string, string>();
    rows.forEach((r) => map.set(r.storeCode, r.storeName));
    return Array.from(map.entries()).sort();
  }, [rows]);

  const aggregated = useMemo(() => {
    const map = new Map<string, AggKey>();
    for (const r of rows) {
      if (store !== "all" && r.storeCode !== store) continue;
      const k = `${r.storeCode}|${r.barcode}`;
      const cur = map.get(k);
      if (cur) {
        cur.qty += r.stockQty;
      } else {
        map.set(k, {
          storeCode: r.storeCode, storeName: r.storeName,
          barcode: r.barcode, productName: r.productName,
          qty: r.stockQty,
        });
      }
    }
    let arr = Array.from(map.values());
    if (query.trim()) {
      const q = query.trim().toLowerCase();
      arr = arr.filter((r) =>
        r.productName.toLowerCase().includes(q) ||
        r.barcode.toLowerCase().includes(q) ||
        r.storeName.toLowerCase().includes(q)
      );
    }
    arr.sort((a, b) =>
      a.barcode.localeCompare(b.barcode) ||
      a.productName.localeCompare(b.productName) ||
      a.storeCode.localeCompare(b.storeCode) ||
      a.storeName.localeCompare(b.storeName)
    );
    return arr;
  }, [rows, store, query]);

  const totals = useMemo(
    () => aggregated.reduce(
      (acc, r) => ({ qty: acc.qty + r.qty }),
      { qty: 0 }
    ),
    [aggregated]
  );

  const visible = aggregated.slice(0, 500);

  return (
    <div className="space-y-4">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center">
        <div className="relative flex-1">
          <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            placeholder="Search product, barcode or store…"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            className="pl-9"
          />
        </div>
        <Select value={store} onValueChange={setStore}>
          <SelectTrigger className="sm:w-64"><SelectValue /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All stores ({stores.length})</SelectItem>
            {stores.map(([code, name]) => (
              <SelectItem key={code} value={code}>{code} — {name}</SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <Card className="overflow-hidden border-border/60 shadow-card">
        <div className="flex flex-wrap items-center justify-between gap-2 border-b bg-secondary/40 px-4 py-3 text-sm">
          <span className="font-medium">{fmtInt(aggregated.length)} items</span>
          <div className="flex flex-wrap gap-4 text-muted-foreground">
            <span>Qty: <span className="font-semibold text-foreground">{fmtInt(totals.qty)}</span></span>
          </div>
        </div>
        <div className="max-h-[60vh] overflow-auto">
          <Table>
            <TableHeader className="sticky top-0 bg-card">
              <TableRow>
                <TableHead>Store</TableHead>
                <TableHead>Barcode</TableHead>
                <TableHead>Product</TableHead>
                <TableHead className="text-right">Qty</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {visible.map((r) => (
                <TableRow key={`${r.storeCode}-${r.barcode}`}>
                  <TableCell><Badge variant="secondary">{r.storeCode}</Badge> <span className="text-muted-foreground">{r.storeName}</span></TableCell>
                  <TableCell className="font-mono text-xs">{r.barcode}</TableCell>
                  <TableCell className="max-w-[320px] truncate">{r.productName}</TableCell>
                  <TableCell className="text-right tabular-nums">{fmtInt(r.qty)}</TableCell>
                </TableRow>
              ))}
              {visible.length === 0 && (
                <TableRow><TableCell colSpan={4} className="py-10 text-center text-muted-foreground">No matching rows.</TableCell></TableRow>
              )}
            </TableBody>
          </Table>
        </div>
        {aggregated.length > visible.length && (
          <div className="border-t bg-secondary/30 px-4 py-2 text-center text-xs text-muted-foreground">
            Showing first 500 of {fmtInt(aggregated.length)}. Refine your search to narrow results.
          </div>
        )}
      </Card>
    </div>
  );
};
