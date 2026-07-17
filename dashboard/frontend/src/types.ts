export type AssignmentStrategy = "cooperative-sticky" | "range" | "roundrobin";

export interface MetricsSnapshot {
  lag: Record<string, number>;
  total_lag: number;
  active_consumers: number;
  decisions: { PASS: number; ESCALATE: number; BLOCK: number };
  latency_p50_ms: number;
  latency_p95_ms: number;
  producer_mode: "baseline" | "spike";
  producer_rate: number;
  rebalancing: boolean;
  transitioning_partitions: number[];
  assignment_strategy: AssignmentStrategy;
  duplicates_filtered: number;
}

export const EMPTY_SNAPSHOT: MetricsSnapshot = {
  lag: { "0": 0, "1": 0, "2": 0, "3": 0 },
  total_lag: 0,
  active_consumers: 0,
  decisions: { PASS: 0, ESCALATE: 0, BLOCK: 0 },
  latency_p50_ms: 0,
  latency_p95_ms: 0,
  producer_mode: "baseline",
  producer_rate: 0,
  rebalancing: false,
  transitioning_partitions: [],
  assignment_strategy: "cooperative-sticky",
  duplicates_filtered: 0,
};
