import { PlatformMetricsState } from "../usePlatformMetrics";

/**
 * Näyttää analytics-consumerin (ks. DEEP_DIVE.md "Core platform -laajennus")
 * liukuvan ikkunan lukuja: block-rate ja kokonaismäärä koko sen elinajalta.
 *
 * Tarkoituksella ERI komponentti kuin DecisionBarChart: tämä data tulee
 * kokonaan eri palvelusta (oma consumer group, oma HTTP-rajapinta, oma
 * allekirjoitettu auth) kuin /ws:n live-snapshot — pointti on juuri se, että
 * kaksi riippumatonta kuluttajaa lukevat samaa Kafka-topicia eri tarkoituksiin.
 * Jos tämä sekoitettaisiin DecisionBarChartiin, koko "toinen tiimi voi liittyä
 * tapahtumavirtaan koskematta alkuperäiseen putkeen" -väite katoaisi näkyvistä.
 *
 * "unreachable"-tila ei piilota koko paneelia — näyttää sen sijaan viimeisimmän
 * tunnetun arvon ja tilan sanallisesti, saman "liputa älä piilota" -periaatteen
 * mukaisesti kuin muualla dashboardissa (ks. Limitations-osio pääREADME:ssä).
 */
export function AnalyticsConsumerPanel({ metrics }: { metrics: PlatformMetricsState }) {
  const { data, status } = metrics;

  return (
    <div className="analytics-consumer-panel" role="status">
      <span className="analytics-consumer-panel-label">
        Analytics-consumer (riippumaton 3. kuluttaja)
      </span>

      {data ? (
        <>
          <div className="analytics-consumer-panel-row">
            <span>Block-rate (1h ikkuna)</span>
            <strong>{(data.block_rate_in_window * 100).toFixed(2)} %</strong>
          </div>
          <div className="analytics-consumer-panel-row">
            <span>Käsitelty yhteensä</span>
            <strong>{data.total_consumed_lifetime.toLocaleString("fi-FI")}</strong>
          </div>
        </>
      ) : (
        <span className="analytics-consumer-panel-empty">
          {status === "loading" ? "Haetaan…" : "Ei vielä dataa"}
        </span>
      )}

      {status === "unreachable" && (
        <span className="analytics-consumer-panel-warning">
          ⚠ analytics-consumer ei vastaa juuri nyt — näytetään viimeisin tunnettu arvo.
        </span>
      )}
    </div>
  );
}
