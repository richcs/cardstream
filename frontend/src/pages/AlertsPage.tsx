import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { getAlerts } from '../api/client';
import type { Alert, AlertType, Severity } from '../api/types';
import { SeverityBadge } from '../components/SeverityBadge';
import { useAlertsFeed } from '../hooks/useAlertsFeed';

const SEVERITIES: Severity[] = ['LOW', 'MED', 'HIGH'];
const TYPES: AlertType[] = ['SPIKE', 'ARBITRAGE'];

export function AlertsPage() {
  const [severity, setSeverity] = useState<Severity | ''>('');
  const [type, setType] = useState<AlertType | ''>('');
  const [watchlistOnly, setWatchlistOnly] = useState(false);
  const [initial, setInitial] = useState<Alert[]>([]);

  const { alerts: live, connected } = useAlertsFeed(watchlistOnly);

  useEffect(() => {
    getAlerts({ severity: severity || undefined, type: type || undefined, limit: 50 }).then(setInitial);
  }, [severity, type]);

  const matchesFilter = (a: Alert) => (!severity || a.severity === severity) && (!type || a.type === type);
  const liveIds = new Set(live.map((a) => a.alertId));
  const combined = [...live.filter(matchesFilter), ...initial.filter((a) => !liveIds.has(a.alertId) && matchesFilter(a))];

  return (
    <div>
      <h1>Alerts</h1>
      <div className="filters">
        <select value={severity} onChange={(e) => setSeverity(e.target.value as Severity | '')}>
          <option value="">All severities</option>
          {SEVERITIES.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
        <select value={type} onChange={(e) => setType(e.target.value as AlertType | '')}>
          <option value="">All types</option>
          {TYPES.map((t) => (
            <option key={t} value={t}>
              {t}
            </option>
          ))}
        </select>
        <label className="checkbox">
          <input type="checkbox" checked={watchlistOnly} onChange={(e) => setWatchlistOnly(e.target.checked)} />
          Watchlist only
        </label>
        <span className={connected ? 'conn-dot live' : 'conn-dot'} title={connected ? 'Live' : 'Disconnected'} />
      </div>

      {combined.length === 0 && <p className="empty">No alerts match.</p>}
      <ul className="alert-list">
        {combined.map((a) => (
          <li key={a.alertId} className="alert-row">
            <SeverityBadge severity={a.severity} />
            <span className="alert-type">{a.type}</span>
            <Link to={`/cards/${encodeURIComponent(a.cardId)}`}>{a.name ?? a.cardId}</Link>
            <span className="muted">{new Date(a.ts).toLocaleString()}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}
