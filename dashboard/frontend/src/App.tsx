import { ParticleFlow3D } from "./components/ParticleFlow3D";
import { SinglePartitionGauge, TotalLagGauge } from "./components/LagGauge";
import { DecisionBarChart } from "./components/DecisionBarChart";
import { Controls } from "./components/Controls";
import { LiveAnnouncer } from "./components/LiveAnnouncer";
import { AccessibleDataTable } from "./components/AccessibleDataTable";
import { RebalanceBanner } from "./components/RebalanceBanner";
import { DuplicateCounter } from "./components/DuplicateCounter";
import { useMetricsSocket } from "./useMetricsSocket";
import { usePrefersReducedMotion } from "./usePrefersReducedMotion";

export default function App() {
  const snapshot = useMetricsSocket();
  const reducedMotion = usePrefersReducedMotion();
  const partitions = Object.keys(snapshot.lag)
    .map(Number)
    .sort((a, b) => a - b);

  return (
    <div className="app">
      <header className="app-header">
        <h1>Ruuhkavahti</h1>
        <p>Kafka-pohjainen reaaliaikainen guardrail-demo</p>
      </header>

      <LiveAnnouncer snapshot={snapshot} />
      <RebalanceBanner snapshot={snapshot} />

      <main className="app-main">
        {!reducedMotion && <ParticleFlow3D snapshot={snapshot} />}

        <aside className={reducedMotion ? "sidebar sidebar--full" : "sidebar"}>
          {reducedMotion && (
            <p className="reduced-motion-note">
              Järjestelmäasetuksesi pyytää vähennettyä liikettä — 3D-partikkelivirta on korvattu
              samalla datalla ilman jatkuvaa animaatiota.
            </p>
          )}
          <TotalLagGauge value={snapshot.total_lag} />
          <div className="gauge-grid">
            {partitions.map((p) => (
              <SinglePartitionGauge key={p} partition={p} value={snapshot.lag[p]} />
            ))}
          </div>
          <DecisionBarChart snapshot={snapshot} />
          <DuplicateCounter snapshot={snapshot} />
          <Controls snapshot={snapshot} />
          <AccessibleDataTable snapshot={snapshot} />
        </aside>
      </main>
    </div>
  );
}
