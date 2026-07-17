import { MetricsSnapshot } from "../types";

const STRATEGY_LABEL: Record<string, string> = {
  "cooperative-sticky": "cooperative-sticky",
  range: "eager (range)",
  roundrobin: "eager (round-robin)",
};

/**
 * Näkyvä JA aria-live="polite" -yhteensopiva tilabanneri. Elementti pysyy
 * aina DOM:ssa (vain sisältö tyhjenee) — jos koko live-region liitettäisiin
 * ehdollisesti vasta rebalancen alkaessa, osa ruudunlukijoista ei luotettavasti
 * huomaisi sitä, koska ARIA-live-region-malli seuraa MUUTOKSIA jo olemassa
 * olevaan alueeseen, ei uuden alueen ilmestymistä.
 */
export function RebalanceBanner({ snapshot }: { snapshot: MetricsSnapshot }) {
  const active = snapshot.rebalancing;
  const label = STRATEGY_LABEL[snapshot.assignment_strategy] ?? snapshot.assignment_strategy;
  const scope =
    snapshot.assignment_strategy === "cooperative-sticky"
      ? `vain partitiot ${snapshot.transitioning_partitions.map((p) => `P${p}`).join(", ") || "…"}`
      : "kaikki partitiot";

  // Elementti pysyy aina DOM:ssa ja tyhjänä tekstisisältönä piiloutuu
  // visuaalisesti CSS:n :empty-valitsimella (ks. styles.css) — ei koskaan
  // display:none tai hidden, jotka poistaisivat sen saavutettavuuspuusta.
  return (
    <div className="rebalance-banner" role="status" aria-live="polite">
      {active ? `⟳ Rebalancing partition assignments… (${label} — ${scope} pysähtyy hetkeksi)` : ""}
    </div>
  );
}
