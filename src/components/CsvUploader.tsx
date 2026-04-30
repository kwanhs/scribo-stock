import { useRef, useState } from "react";
import Papa from "papaparse";
import { Upload, Loader2, FileSpreadsheet } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useToast } from "@/hooks/use-toast";
import { mapCsvRow, upsertRows, type StockRow } from "@/lib/stockDb";

interface Props {
  onUploaded: () => void;
}

const CHUNK = 2000;

export const CsvUploader = ({ onUploaded }: Props) => {
  const inputRef = useRef<HTMLInputElement>(null);
  const [busy, setBusy] = useState(false);
  const [progress, setProgress] = useState(0);
  const { toast } = useToast();

  const handleFile = async (file: File) => {
    setBusy(true);
    setProgress(0);
    let total = 0;
    let kept = 0;
    let buffer: StockRow[] = [];
    const pending: Promise<void>[] = [];

    try {
      await new Promise<void>((resolve, reject) => {
        Papa.parse<Record<string, unknown>>(file, {
          header: true,
          skipEmptyLines: true,
          // NOTE: do NOT use `worker: true` — workers don't support async chunk callbacks
          // and throw "Not implemented." when parser.pause()/resume() is called.
          chunk: (results, parser) => {
            for (const r of results.data) {
              total++;
              const mapped = mapCsvRow(r);
              if (mapped) {
                buffer.push(mapped);
              }
            }
            if (buffer.length >= CHUNK) {
              const batch = buffer;
              buffer = [];
              parser.pause();
              pending.push(
                upsertRows(batch)
                  .then(() => {
                    kept = kept + batch.length;
                    parser.resume();
                  })
                  .catch((e) => {
                    parser.abort();
                    reject(e);
                  })
              );
            }
            setProgress(total);
          },
          complete: () => {
            (async () => {
              try {
                await Promise.all(pending);
                if (buffer.length) await upsertRows(buffer);
                resolve();
              } catch (e) {
                reject(e);
              }
            })();
          },
          error: (err) => reject(err),
        });
      });

      toast({
        title: "Upload complete",
        description: `Parsed ${total.toLocaleString()} rows, stored ${kept.toLocaleString()}.`,
      });
      onUploaded();
    } catch (err) {
      console.error("CSV upload failed", err);
      toast({
        title: "Upload failed",
        description:
          err instanceof Error ? err.message : "Unknown error parsing CSV.",
        variant: "destructive",
      });
    } finally {
      setBusy(false);
    }
  };

  return (
    <div
      onDragOver={(e) => e.preventDefault()}
      onDrop={(e) => {
        e.preventDefault();
        const f = e.dataTransfer.files?.[0];
        if (f) handleFile(f);
      }}
      className="rounded-xl border-2 border-dashed border-border bg-card p-8 text-center transition-colors hover:border-primary/50 hover:bg-secondary/30"
    >
      <div className="mx-auto mb-4 flex h-12 w-12 items-center justify-center rounded-full bg-gradient-primary text-primary-foreground shadow-card">
        {busy ? (
          <Loader2 className="h-6 w-6 animate-spin" />
        ) : (
          <FileSpreadsheet className="h-6 w-6" />
        )}
      </div>
      <h3 className="mb-1 text-base font-semibold">Upload stock CSV</h3>
      <p className="mb-4 text-sm text-muted-foreground">
        {busy
          ? `Processing… ${progress.toLocaleString()} rows`
          : "Drag & drop or pick a file. Existing rows are upserted by period + store + barcode."}
      </p>
      <input
        ref={inputRef}
        type="file"
        accept=".csv,text/csv"
        className="hidden"
        onChange={(e) => {
          const f = e.target.files?.[0];
          if (f) handleFile(f);
          e.target.value = "";
        }}
      />
      <Button
        onClick={() => inputRef.current?.click()}
        disabled={busy}
        className="bg-gradient-primary hover:opacity-90"
      >
        <Upload className="mr-2 h-4 w-4" />
        Choose CSV
      </Button>
    </div>
  );
};
