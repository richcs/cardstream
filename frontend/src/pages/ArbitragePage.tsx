import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { getArbitrage } from '../api/client';
import type { Alert } from '../api/types';
import { useAlertsFeed } from '../hooks/useAlertsFeed';

function num(detail: Record<string, unknown>, key: string): number | undefined {
  const v = detail[key];
  return typeof v === 'number' ? v : undefined;
}

export function ArbitragePage() {
  const [flags, setFlags] = useState<Alert[]>([]);
  const { alerts: live } = useAlertsFeed(false);

  useEffect(() => {
    getArbitrage(50).then(setFlags);
  }, []);

  useEffect(() => {
    const newArb = live.filter((a) => a.type === 'ARBITRAGE');
    if (newArb.length === 0) return;
    setFlags((prev) => {
      const seen = new Set(prev.map((a) => a.alertId));
      const additions = newArb.filter((a) => !seen.has(a.alertId));
      return additions.length === 0 ? prev : [...additions, ...prev].slice(0, 100);
    });
  }, [live]);

  return (
    <div>
      <h1>Arbitrage</h1>
      <p className="muted">Listings priced below the rolling market average.</p>
      {flags.length === 0 && <p className="empty">No arbitrage flags yet.</p>}
      <table className="table">
        <thead>
          <tr>
            <th>Card</th>
            <th>Listing</th>
            <th>Ref. avg</th>
            <th>Discount</th>
            <th>Seller</th>
            <th>When</th>
          </tr>
        </thead>
        <tbody>
          {flags.map((a) => {
            const listingPrice = num(a.detail, 'listingPrice');
            const referenceAvg = num(a.detail, 'referenceAvg');
            const discountPct = num(a.detail, 'discountPct');
            return (
              <tr key={a.alertId}>
                <td>
                  <Link to={`/cards/${encodeURIComponent(a.cardId)}`}>{a.name ?? a.cardId}</Link>
                </td>
                <td>{listingPrice !== undefined ? `$${listingPrice.toFixed(2)}` : '—'}</td>
                <td>{referenceAvg !== undefined ? `$${referenceAvg.toFixed(2)}` : '—'}</td>
                <td className="positive">{discountPct !== undefined ? `${(discountPct * 100).toFixed(1)}%` : '—'}</td>
                <td className="muted">{String(a.detail.sellerId ?? '—')}</td>
                <td className="muted">{new Date(a.ts).toLocaleString()}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
