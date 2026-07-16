import { MetricsSnapshot } from "../types";

const ROWS: Array<{ key: keyof MetricsSnapshot["decisions"]; label: string; color: string }> = [
  { key: "PASS", label: "PASS", color: "#22c55e" },
  { key: "ESCALATE", label: "ESCALATE", color: "#eab308" },
  { key: "BLOCK", label: "BLOCK", color: "#ef4444" },
];

export function DecisionBarChart({ snapshot }: { snapshot: MetricsSnapshot }) {
  const total = snapshot.decisions.PASS + snapshot.decisions.ESCALATE + snapshot.decisions.BLOCK || 1;

  return (
    <div className="decision-chart">
      {ROWS.map((row) => {
        const count = snapshot.decisions[row.key];
        const pct = (count / total) * 100;
        return (
          <div key={row.key} className="decision-row">
            <span className="decision-label" style={{ color: row.color }}>
              {row.label}
            </span>
            <div className="decision-bar-track">
              <div
                className="decision-bar-fill"
                style={{ width: `${pct}%`, background: row.color }}
              />
            </div>
            <span className="decision-count">{count.toLocaleString("fi-FI")}</span>
          </div>
        );
      })}
      <div className="latency-row">
        <span>p50: {snapshot.latency_p50_ms} ms</span>
        <span>p95: {snapshot.latency_p95_ms} ms</span>
      </div>
    </div>
  );
}
