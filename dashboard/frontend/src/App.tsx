import { ParticleFlow3D } from "./components/ParticleFlow3D";
import { SinglePartitionGauge, TotalLagGauge } from "./components/LagGauge";
import { DecisionBarChart } from "./components/DecisionBarChart";
import { Controls } from "./components/Controls";
import { LiveAnnouncer } from "./components/LiveAnnouncer";
import { AccessibleDataTable } from "./components/AccessibleDataTable";
import { RebalanceBanner } from "./components/RebalanceBanner";
import { DuplicateCounter } from "./components/DuplicateCounter";
import { AnalyticsConsumerPanel } from "./components/AnalyticsConsumerPanel";
import { DemoCaption } from "./components/DemoCaption";
import { useMetricsSocket } from "./useMetricsSocket";
import { usePlatformMetrics } from "./usePlatformMetrics";
import { usePrefersReducedMotion } from "./usePrefersReducedMotion";
import { useDemoMode } from "./useDemoMode";

export default function App() {
  const snapshot = useMetricsSocket();
  const platformMetrics = usePlatformMetrics();
  const reducedMotion = usePrefersReducedMotion();
  const demo = useDemoMode();
  const partitions = Object.keys(snapshot.lag)
    .map(Number)
    .sort((a, b) => a - b);

  return (
    <div className={`app${demo.active ? " demo-mode" : ""}`}>
      <header className="app-header">
        <h1>Ruuhkavahti</h1>
        <p>Kafka-pohjainen reaaliaikainen guardrail-demo</p>
      </header>

      <LiveAnnouncer snapshot={snapshot} />
      <RebalanceBanner snapshot={snapshot} />

      <main className="app-main">
        {!reducedMotion && <ParticleFlow3D snapshot={snapshot} demoMode={demo.active} />}

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
          <AnalyticsConsumerPanel metrics={platformMetrics} />
          <Controls snapshot={snapshot} demoMode={demo.active} />
          {!demo.active && <AccessibleDataTable snapshot={snapshot} />}
        </aside>
      </main>

      {demo.active && <DemoCaption text={demo.caption} />}
    </div>
  );
}
