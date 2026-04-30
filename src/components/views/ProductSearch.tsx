import { useEffect, useMemo, useState } from "react";
import { Search } from "lucide-react";
import useSWR from "swr";
import { Input } from "@/components/ui/input";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { fmtInt } from "@/lib/format";
import { fetchProductSearch } from "@/lib/api/stockRows";

interface Props {
  hasData: boolean;
  periodFrom: string | null;
  periodTo: string | null;
}

const SEARCH_LIMIT = 50;

function formatStockDate(periodFrom: string | null, periodTo: string | null) {
  if (periodFrom && periodTo) {
    return periodFrom === periodTo ? periodTo : `${periodFrom} to ${periodTo}`;
  }
  return "—";
}

export const ProductSearch = ({ hasData, periodFrom, periodTo }: Props) => {
  const [query, setQuery] = useState("");
  const [debouncedQuery, setDebouncedQuery] = useState("");

  useEffect(() => {
    const timeout = window.setTimeout(() => {
      setDebouncedQuery(query.trim());
    }, 250);
    return () => window.clearTimeout(timeout);
  }, [query]);

  const searchKey = debouncedQuery ? ["product-search", debouncedQuery, SEARCH_LIMIT] : null;
  const { data, error, isLoading } = useSWR(
    searchKey,
    ([, searchQuery, limit]: [string, string, number]) =>
      fetchProductSearch({ query: searchQuery, limit }),
  );

  const stockDate = useMemo(
    () =>
      formatStockDate(
        data?.periodFrom ?? periodFrom ?? null,
        data?.periodTo ?? periodTo ?? null,
      ),
    [data?.periodFrom, data?.periodTo, periodFrom, periodTo],
  );
  const hits = data?.products ?? [];
  const hasQuery = debouncedQuery.length > 0;

  return (
    <div className="space-y-4">
      <div className="relative">
        <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
        <Input
          placeholder="Search by barcode, Chinese or Japanese product name…"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          className="pl-9 h-11 text-base"
        />
      </div>

      <div className="flex flex-wrap items-center justify-between gap-2 rounded-lg border border-border/60 bg-card/70 px-4 py-3 text-sm text-muted-foreground">
        <span>Searches run on the backend against the latest uploaded stock snapshot.</span>
        <span>
          Stock date: <span className="font-medium text-foreground">{stockDate}</span>
        </span>
      </div>

      {!hasData && (
        <Card className="border-dashed bg-card/50 p-10 text-center text-muted-foreground">
          Upload a stock CSV to start searching products.
        </Card>
      )}

      {hasData && !query.trim() && (
        <Card className="border-dashed bg-card/50 p-10 text-center text-muted-foreground">
          Start typing to search products from the backend.
        </Card>
      )}

      {error && (
        <Card className="border-destructive/40 bg-destructive/5 p-6 text-sm text-destructive">
          Failed to load product search results: {error instanceof Error ? error.message : "Unknown error"}
        </Card>
      )}

      {isLoading && hasQuery && (
        <div className="space-y-3">
          {Array.from({ length: 2 }).map((_, index) => (
            <Card key={index} className="overflow-hidden border-border/60 shadow-card">
              <div className="space-y-2 border-b px-5 py-4">
                <Skeleton className="h-5 w-48" />
                <Skeleton className="h-4 w-72" />
              </div>
              <div className="space-y-2 px-5 py-4">
                <Skeleton className="h-10 w-full" />
                <Skeleton className="h-10 w-full" />
              </div>
            </Card>
          ))}
        </div>
      )}

      <div className="space-y-3">
        {hits.map((p) => (
          <Card key={p.barcode} className="overflow-hidden border-border/60 shadow-card">
            <div className="flex flex-col gap-2 border-b bg-gradient-subtle px-5 py-4 sm:flex-row sm:items-center sm:justify-between">
              <div>
                <div className="flex items-center gap-2">
                  <Badge variant="secondary" className="font-mono">{p.barcode}</Badge>
                  <h3 className="font-semibold">{p.productName || "—"}</h3>
                </div>
                {p.productNameJa && p.productNameJa !== p.productName && (
                  <p className="mt-1 text-sm text-muted-foreground">{p.productNameJa}</p>
                )}
              </div>
              <div className="flex gap-4 text-sm">
                <div className="text-right">
                  <p className="text-xs text-muted-foreground">Total qty</p>
                  <p className="font-semibold tabular-nums">{fmtInt(p.totalQty)}</p>
                </div>
              </div>
            </div>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Store</TableHead>
                  <TableHead className="text-right">Qty</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {p.byStore
                  .slice()
                  .sort((a, b) => a.storeCode.localeCompare(b.storeCode))
                  .map((s) => (
                    <TableRow key={s.storeCode}>
                      <TableCell><Badge variant="outline">{s.storeCode}</Badge> <span className="text-muted-foreground">{s.storeName}</span></TableCell>
                      <TableCell className="text-right tabular-nums">{fmtInt(s.qty)}</TableCell>
                    </TableRow>
                  ))}
              </TableBody>
            </Table>
          </Card>
        ))}

        {hasQuery && !isLoading && hits.length === 0 && (
          <Card className="border-dashed p-10 text-center text-muted-foreground">
            No products match "{debouncedQuery}".
          </Card>
        )}
      </div>
    </div>
  );
};
