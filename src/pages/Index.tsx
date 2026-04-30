import { Boxes, CalendarDays, Layers, PackageSearch, Store, Trash2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription,
  AlertDialogFooter, AlertDialogHeader, AlertDialogTitle, AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import { CsvUploader } from "@/components/CsvUploader";
import { StatCard } from "@/components/StatCard";
import { ProductSearch } from "@/components/views/ProductSearch";
import { useStockSummary } from "@/hooks/useStockData";
import { deleteStockRows } from "@/lib/api/stockRows";
import { fmtInt } from "@/lib/format";
import { useToast } from "@/hooks/use-toast";

const formatStockDate = (periodFrom: string | null, periodTo: string | null) => {
  if (periodFrom && periodTo) {
    return periodFrom === periodTo ? periodTo : `${periodFrom} to ${periodTo}`;
  }
  return "—";
};

const Index = () => {
  const { summary, loading, refresh } = useStockSummary();
  const { toast } = useToast();
  const hasData = (summary?.rows ?? 0) > 0;

  const handleClear = async () => {
    await deleteStockRows();
    await refresh();
    toast({ title: "Data cleared", description: "All uploaded rows removed from the backend database." });
  };

  return (
    <div className="min-h-screen bg-gradient-subtle">
      <header className="border-b border-border/60 bg-card/70 backdrop-blur">
        <div className="container flex flex-wrap items-center justify-between gap-3 py-5">
          <div className="flex items-center gap-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-gradient-primary text-primary-foreground shadow-card">
              <Boxes className="h-5 w-5" />
            </div>
            <div>
              <h1 className="text-lg font-semibold tracking-tight">Stock Portal</h1>
              <p className="text-xs text-muted-foreground">Multi-store inventory overview</p>
            </div>
          </div>
          {hasData && (
            <AlertDialog>
              <AlertDialogTrigger asChild>
                <Button variant="ghost" size="sm" className="text-muted-foreground">
                  <Trash2 className="mr-2 h-4 w-4" /> Clear data
                </Button>
              </AlertDialogTrigger>
              <AlertDialogContent>
                <AlertDialogHeader>
                  <AlertDialogTitle>Clear all uploaded data?</AlertDialogTitle>
                  <AlertDialogDescription>
                    This removes all {fmtInt(summary?.rows ?? 0)} rows from the backend database. You can re-upload at any time.
                  </AlertDialogDescription>
                </AlertDialogHeader>
                <AlertDialogFooter>
                  <AlertDialogCancel>Cancel</AlertDialogCancel>
                  <AlertDialogAction onClick={handleClear}>Clear</AlertDialogAction>
                </AlertDialogFooter>
              </AlertDialogContent>
            </AlertDialog>
          )}
        </div>
      </header>

      <main className="container space-y-6 py-8">
        <CsvUploader onUploaded={refresh} />

        <section className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-4">
          <StatCard label="Stores" value={fmtInt(summary?.stores ?? 0)} icon={Store} />
          <StatCard label="Products (SKUs)" value={fmtInt(summary?.products ?? 0)} icon={PackageSearch} />
          <StatCard
            label="Stock units"
            value={fmtInt(summary?.qty ?? 0)}
            hint={`${fmtInt(summary?.rows ?? 0)} rows`}
            icon={Layers}
          />
          <StatCard
            label="Stock data date"
            value={formatStockDate(summary?.periodFrom ?? null, summary?.periodTo ?? null)}
            hint="Latest uploaded snapshot"
            icon={CalendarDays}
          />
        </section>

        {loading ? (
          <div className="rounded-xl border border-border/60 bg-card p-10 text-center text-muted-foreground shadow-card">
            Loading…
          </div>
        ) : !hasData ? (
          <div className="rounded-xl border border-dashed border-border bg-card/60 p-12 text-center">
            <h2 className="mb-2 text-lg font-semibold">No data yet</h2>
            <p className="text-sm text-muted-foreground">
              Upload a stock CSV above to start exploring inventory across your stores.
            </p>
          </div>
        ) : (
          <ProductSearch
            hasData={hasData}
            periodFrom={summary?.periodFrom ?? null}
            periodTo={summary?.periodTo ?? null}
          />
        )}
      </main>
    </div>
  );
};

export default Index;
