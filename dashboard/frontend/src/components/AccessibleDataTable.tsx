import { useId, useState } from "react";
import { MetricsSnapshot } from "../types";

/**
 * Sama data kuin partikkelivirrassa ja mittareissa, mutta oikeana
 * semanttisena <table>:na. Piilotettu oletuksena (ettei se tuputa itseään
 * kaikille), mutta yhden painikkeen takana — ei koskaan ainoastaan
 * visuaalisen tulkinnan varassa (ks. README "Liputa, älä piilota").
 */
export function AccessibleDataTable({ snapshot }: { snapshot: MetricsSnapshot }) {
  const [open, setOpen] = useState(false);
  const tableId = useId();
  const partitions = Object.keys(snapshot.lag)
    .map(Number)
    .sort((a, b) => a - b);

  return (
    <div className="data-table-section">
      <button
        type="button"
        className="data-table-toggle"
        aria-expanded={open}
        aria-controls={tableId}
        onClick={() => setOpen((o) => !o)}
      >
        {open ? "Piilota data taulukkona" : "Näytä data taulukkona"}
      </button>
      <div id={tableId} hidden={!open}>
        <div className="data-table-wrapper">
          <table className="data-table">
            <caption>Jono partitioittain (lag)</caption>
            <thead>
              <tr>
                <th scope="col">Partitio</th>
                <th scope="col">Viestiä jonossa</th>
              </tr>
            </thead>
            <tbody>
              {partitions.map((p) => (
                <tr key={p}>
                  <td>P{p}</td>
                  <td>{snapshot.lag[p]}</td>
                </tr>
              ))}
            </tbody>
            <tfoot>
              <tr>
                <td>Yhteensä</td>
                <td>{snapshot.total_lag}</td>
              </tr>
            </tfoot>
          </table>
        </div>
        <div className="data-table-wrapper" style={{ marginTop: "0.75rem" }}>
          <table className="data-table">
            <caption>Päätösjakauma ja läpimenoaika</caption>
            <thead>
              <tr>
                <th scope="col">Mittari</th>
                <th scope="col">Arvo</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>PASS</td>
                <td>{snapshot.decisions.PASS}</td>
              </tr>
              <tr>
                <td>ESCALATE</td>
                <td>{snapshot.decisions.ESCALATE}</td>
              </tr>
              <tr>
                <td>BLOCK</td>
                <td>{snapshot.decisions.BLOCK}</td>
              </tr>
              <tr>
                <td>Läpimenoaika p50</td>
                <td>{snapshot.latency_p50_ms} ms</td>
              </tr>
              <tr>
                <td>Läpimenoaika p95</td>
                <td>{snapshot.latency_p95_ms} ms</td>
              </tr>
              <tr>
                <td>Duplikaatteja suodatettu</td>
                <td>{snapshot.duplicates_filtered}</td>
              </tr>
              <tr>
                <td>Rebalance-strategia</td>
                <td>{snapshot.assignment_strategy}</td>
              </tr>
              <tr>
                <td>Rebalancoi juuri nyt</td>
                <td>{snapshot.rebalancing ? "kyllä" : "ei"}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
