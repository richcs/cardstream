import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { getTopMovers } from '../api/client';
import type { TopMover } from '../api/types';

function tickerSuffix(marketKey: string): string {
  const parts = marketKey.split('|');
  return parts.length === 3 ? `${parts[1]} / ${parts[2]}` : marketKey;
}

export function TopMoversPage() {
  const [windowType, setWindowType] = useState<'hourly' | 'daily'>('daily');
  const [dir, setDir] = useState<'gainers' | 'losers'>('gainers');
  const [movers, setMovers] = useState<TopMover[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    getTopMovers({ window: windowType, dir, limit: 25 })
      .then((res) => {
        if (!cancelled) setMovers(res);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [windowType, dir]);

  return (
    <div>
      <h1>Top Movers</h1>
      <div className="filters">
        <div className="toggle-group">
          <button className={windowType === 'hourly' ? 'active' : ''} onClick={() => setWindowType('hourly')}>
            Hourly
          </button>
          <button className={windowType === 'daily' ? 'active' : ''} onClick={() => setWindowType('daily')}>
            Daily
          </button>
        </div>
        <div className="toggle-group">
          <button className={dir === 'gainers' ? 'active' : ''} onClick={() => setDir('gainers')}>
            Gainers
          </button>
          <button className={dir === 'losers' ? 'active' : ''} onClick={() => setDir('losers')}>
            Losers
          </button>
        </div>
      </div>

      {loading && <p className="empty">Loading…</p>}
      {!loading && movers.length === 0 && <p className="empty">No settled windows yet.</p>}

      <table className="table">
        <thead>
          <tr>
            <th>Card</th>
            <th>Ticker</th>
            <th>Avg</th>
            <th>Prev avg</th>
            <th>% change</th>
          </tr>
        </thead>
        <tbody>
          {movers.map((m) => (
            <tr key={m.marketKey}>
              <td>
                <Link to={`/cards/${encodeURIComponent(m.cardId)}`}>{m.cardName}</Link>
              </td>
              <td className="muted">{tickerSuffix(m.marketKey)}</td>
              <td>${m.avgPrice.toFixed(2)}</td>
              <td>${m.previousAvgPrice.toFixed(2)}</td>
              <td className={m.pctChange >= 0 ? 'positive' : 'negative'}>
                {(m.pctChange * 100).toFixed(1)}%
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
