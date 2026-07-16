const GAUGE_MAX = 6000;

export function ratioColor(ratio: number): string {
  if (ratio < 0.33) return "#22c55e";
  if (ratio < 0.66) return "#eab308";
  return "#ef4444";
}

export function severityLabel(ratio: number): string {
  if (ratio < 0.33) return "matala";
  if (ratio < 0.66) return "kohtalainen";
  return "korkea";
}

function arcPath(startDeg: number, endDeg: number, r: number): string {
  const toRad = (d: number) => ((d - 180) * Math.PI) / 180;
  const x1 = 60 + r * Math.cos(toRad(startDeg));
  const y1 = 60 + r * Math.sin(toRad(startDeg));
  const x2 = 60 + r * Math.cos(toRad(endDeg));
  const y2 = 60 + r * Math.sin(toRad(endDeg));
  return `M ${x1} ${y1} A ${r} ${r} 0 0 1 ${x2} ${y2}`;
}

function GaugeDefs({ id, color }: { id: string; color: string }) {
  return (
    <defs>
      <linearGradient id={`grad-${id}`} x1="0" y1="0" x2="1" y2="0">
        <stop offset="0%" stopColor={color} stopOpacity="0.55" />
        <stop offset="100%" stopColor={color} stopOpacity="1" />
      </linearGradient>
      <filter id={`shadow-${id}`} x="-50%" y="-50%" width="200%" height="200%">
        <feDropShadow dx="0" dy="1" stdDeviation="1.4" floodColor={color} floodOpacity="0.6" />
      </filter>
    </defs>
  );
}

function Needle({ id, value, max }: { id: string; value: number; max: number }) {
  const ratio = Math.min(1, value / max);
  const angleDeg = -90 + ratio * 180;
  const color = ratioColor(ratio);
  return (
    <g className="gauge-needle" transform={`rotate(${angleDeg} 60 60)`} filter={`url(#shadow-${id})`}>
      <line x1="60" y1="60" x2="60" y2="14" stroke={color} strokeWidth="3" strokeLinecap="round" />
      <circle cx="60" cy="60" r="4" fill={color} />
    </g>
  );
}

export function SinglePartitionGauge({ partition, value }: { partition: number; value: number }) {
  const ratio = Math.min(1, value / GAUGE_MAX);
  const id = `p${partition}`;
  return (
    <div className="gauge-card">
      <svg viewBox="0 0 120 70" className="gauge-svg" role="img" aria-label={`Partitio ${partition}: ${value} viestiä jonossa, taso ${severityLabel(ratio)}`}>
        <GaugeDefs id={id} color={ratioColor(ratio)} />
        <path className="gauge-track" d={arcPath(0, 180, 50)} strokeWidth="8" fill="none" />
        <path
          className="gauge-progress"
          d={arcPath(0, 180 * ratio, 50)}
          stroke={`url(#grad-${id})`}
          strokeWidth="8"
          strokeLinecap="round"
          fill="none"
        />
        <Needle id={id} value={value} max={GAUGE_MAX} />
      </svg>
      <div className="gauge-label">
        <span className="gauge-partition">P{partition}</span>
        <span className="gauge-value">{value}</span>
      </div>
    </div>
  );
}

export function TotalLagGauge({ value }: { value: number }) {
  const ratio = Math.min(1, value / (GAUGE_MAX * 2));
  return (
    <div className="total-lag" style={{ color: ratioColor(ratio) }}>
      <div className="total-lag-value">{value.toLocaleString("fi-FI")}</div>
      <div className="total-lag-label">viestiä jonossa (yhteensä) — taso: {severityLabel(ratio)}</div>
    </div>
  );
}
