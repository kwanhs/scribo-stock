import useSWR from "swr";

import { fetchStockSummary } from "@/lib/api/stockRows";

export function useStockSummary() {
  const swr = useSWR("stock-summary", fetchStockSummary);

  return {
    summary: swr.data,
    loading: swr.isLoading,
    error: swr.error,
    refresh: swr.mutate,
  };
}
