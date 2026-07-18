/**
 * Demo Moden tekstitysoverlay. Puhtaasti visuaalinen kerros nauhoitusta
 * varten — dashboard on jo saavutettava ilman tätä, joten teksti on
 * aria-hidden (ei toisteta ruudunlukijalle päällekkäisenä ilmoituksena
 * LiveAnnouncerin/RebalanceBannerin kanssa).
 */
export function DemoCaption({ text }: { text: string | null }) {
  return (
    <div className="demo-caption" aria-hidden="true">
      <span className={`demo-caption-text${text ? " demo-caption-text--visible" : ""}`}>
        {text ?? ""}
      </span>
    </div>
  );
}
