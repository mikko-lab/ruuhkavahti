import { MetricsSnapshot } from "../types";

/**
 * Näyttää guardrail_consumer.py:n (partition, offset)-pohjaisen dedup-cachen
 * suodattamien duplikaattien määrän — todiste siitä, että at-least-once-
 * toimitus näkyy oikeasti, ei vain teoriassa (ks. README "Idempotenssi").
 */
export function DuplicateCounter({ snapshot }: { snapshot: MetricsSnapshot }) {
  return (
    <div className="duplicate-counter">
      <span className="duplicate-counter-label">Duplikaatteja suodatettu</span>
      <span className="duplicate-counter-value">
        {snapshot.duplicates_filtered.toLocaleString("fi-FI")}
      </span>
    </div>
  );
}
