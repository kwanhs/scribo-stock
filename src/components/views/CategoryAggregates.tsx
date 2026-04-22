import { useMemo, useState } from "react";
import { Card } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { fmtInt } from "@/lib/format";
import type { StockRow } from "@/lib/stockDb";

interface Props { rows: StockRow[]; }

type Level = "groupCode" | "divCode" | "dptCode" | "lineCode" | "classCode";

const LEVELS: { value: Level; label: string; nameKey: keyof StockRow }[] = [
  { value: "groupCode", label: "Group", nameKey: "groupName" },
  { value: "divCode", label: "Division", nameKey: "divName" },
  { value: "dptCode", label: "Department", nameKey: "dptName" },
  { value: "lineCode", label: "Line", nameKey: "lineName" },
  { value: "classCode", label: "Class", nameKey: "className" },
];

interface Agg {
  code: string; name: string;
  stockQty: number;
  productCount: Set<string>;
}

export const CategoryAggregates = ({ rows }: Props) => {
  const [level, setLevel] = useState<Level>("dptCode");
  const [store, setStore] = useState<string>("all");

  const stores = useMemo(() => {
    const m = new Map<string, string>();
    rows.forEach((r) => m.set(r.storeCode, r.storeName));
    return Array.from(m.entries()).sort();
  }, [rows]);

  const cfg = LEVELS.find((l) => l.value === level)!;

  const data = useMemo(() => {
    const map = new Map<string, Agg>();
    for (const r of rows) {
      if (store !== "all" && r.storeCode !== store) continue;
      const code = String(r[level] || "");
      if (!code) continue;
      let a = map.get(code);
      if (!a) {
        a = {
          code,
          name: String(r[cfg.nameKey] || ""),
          stockQty: 0,
          productCount: new Set(),
        };
        map.set(code, a);
      }
      a.stockQty += r.stockQty;
      a.productCount.add(r.barcode);
    }
    return Array.from(map.values()).sort((a, b) => b.stockQty - a.stockQty);
  }, [rows, level, store, cfg.nameKey]);

  return (
    <div className="space-y-4">
      <div className="flex flex-col gap-3 sm:flex-row">
        <Select value={level} onValueChange={(v) => setLevel(v as Level)}>
          <SelectTrigger className="sm:w-56"><SelectValue /></SelectTrigger>
          <SelectContent>
            {LEVELS.map((l) => <SelectItem key={l.value} value={l.value}>Group by {l.label}</SelectItem>)}
          </SelectContent>
        </Select>
        <Select value={store} onValueChange={setStore}>
          <SelectTrigger className="sm:w-72"><SelectValue /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All stores</SelectItem>
            {stores.map(([code, name]) => (
              <SelectItem key={code} value={code}>{code} — {name}</SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <Card className="overflow-hidden border-border/60 shadow-card">
        <div className="border-b bg-secondary/40 px-4 py-3 text-sm font-medium">
          {fmtInt(data.length)} {cfg.label.toLowerCase()} categories
        </div>
        <div className="max-h-[60vh] overflow-auto">
          <Table>
            <TableHeader className="sticky top-0 bg-card">
              <TableRow>
                <TableHead>{cfg.label}</TableHead>
                <TableHead className="text-right">Products</TableHead>
                <TableHead className="text-right">Stock qty</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {data.map((a) => (
                <TableRow key={a.code}>
                  <TableCell>
                    <div className="flex items-center gap-2">
                      <Badge variant="secondary" className="font-mono">{a.code}</Badge>
                      <span className="max-w-[320px] truncate">{a.name}</span>
                    </div>
                  </TableCell>
                  <TableCell className="text-right tabular-nums">{fmtInt(a.productCount.size)}</TableCell>
                  <TableCell className="text-right tabular-nums">{fmtInt(a.stockQty)}</TableCell>
                </TableRow>
              ))}
              {data.length === 0 && (
                <TableRow><TableCell colSpan={3} className="py-10 text-center text-muted-foreground">No data.</TableCell></TableRow>
              )}
            </TableBody>
          </Table>
        </div>
      </Card>
    </div>
  );
};
