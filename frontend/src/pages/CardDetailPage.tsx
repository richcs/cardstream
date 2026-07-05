import { useEffect, useMemo, useState } from 'react';
import { useParams } from 'react-router-dom';
import { addToWatchlist, getCardDetail, getCardHistory, getWatchlist, removeFromWatchlist } from '../api/client';
import type { CardDetailResponse, MarketSnapshot, WindowedAggregate } from '../api/types';
import { PriceChart } from '../components/PriceChart';
import { getUserId } from '../hooks/useUserId';
import { usePriceStream } from '../hooks/usePriceStream';

type HistoryWindow = 'hourly' | 'daily';

export function CardDetailPage() {
  const { cardId = '' } = useParams();
  const userId = useMemo(() => getUserId(), []);

  const [detail, setDetail] = useState<CardDetailResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selectedMarketKey, setSelectedMarketKey] = useState<string | null>(null);
  const [window, setWindowType] = useState<HistoryWindow>('hourly');
  const [history, setHistory] = useState<WindowedAggregate[]>([]);
  const [watching, setWatching] = useState(false);

  useEffect(() => {
    let cancelled = false;
    setDetail(null);
    setError(null);
    setSelectedMarketKey(null);
    getCardDetail(cardId)
      .then((res) => {
        if (cancelled) return;
        setDetail(res);
        if (res.markets.length > 0) setSelectedMarketKey(res.markets[0].marketKey);
      })
      .catch(() => {
        if (!cancelled) setError('Card not found.');
      });
    return () => {
      cancelled = true;
    };
  }, [cardId]);

  useEffect(() => {
    let cancelled = false;
    getWatchlist(userId)
      .then((cards) => {
        if (!cancelled) setWatching(cards.some((c) => c.cardId === cardId));
      })
      .catch(() => {
        /* ignore — watchlist state is a nice-to-have */
      });
    return () => {
      cancelled = true;
    };
  }, [cardId, userId]);

  useEffect(() => {
    let cancelled = false;
    getCardHistory(cardId, window).then((rows) => {
      if (!cancelled) setHistory(rows);
    });
    return () => {
      cancelled = true;
    };
  }, [cardId, window]);

  usePriceStream((agg) => {
    if (agg.cardId !== cardId) return;
    const wantedType = window === 'hourly' ? 'HOURLY' : 'DAILY';
    if (agg.windowType !== wantedType) return;
    setHistory((prev) => [...prev.filter((h) => h.marketKey !== agg.marketKey || h.windowStart !== agg.windowStart), agg]);
  });

  async function toggleWatch() {
    if (watching) {
      await removeFromWatchlist(userId, cardId);
    } else {
      await addToWatchlist(userId, cardId);
    }
    setWatching((w) => !w);
  }

  if (error) return <p className="error">{error}</p>;
  if (!detail) return <p className="empty">Loading…</p>;

  const { card, markets } = detail;
  const chartData = selectedMarketKey ? history.filter((h) => h.marketKey === selectedMarketKey) : [];

  return (
    <div>
      <div className="card-header">
        {card.imageLarge && <img src={card.imageLarge} alt={card.name} className="card-hero" />}
        <div>
          <h1>{card.name}</h1>
          <p className="muted">
            {card.setName} · {card.rarity} · #{card.number}
          </p>
          <button className={watching ? 'watch-btn watching' : 'watch-btn'} onClick={toggleWatch}>
            {watching ? '★ Watching' : '☆ Watch'}
          </button>
        </div>
      </div>

      <h2>Markets</h2>
      <table className="table">
        <thead>
          <tr>
            <th>Finish</th>
            <th>Condition</th>
            <th>Last</th>
            <th>Avg</th>
            <th>Volatility</th>
            <th>Volume</th>
            <th>Samples</th>
          </tr>
        </thead>
        <tbody>
          {markets.map((m: MarketSnapshot) => (
            <tr
              key={m.marketKey}
              className={m.marketKey === selectedMarketKey ? 'selected' : ''}
              onClick={() => setSelectedMarketKey(m.marketKey)}
            >
              <td>{m.finish}</td>
              <td>{m.condition}</td>
              <td>${m.lastPrice?.toFixed(2)}</td>
              <td>${m.avgPrice?.toFixed(2)}</td>
              <td>{m.volatility.toFixed(2)}</td>
              <td>{m.volume}</td>
              <td>{m.sampleCount}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {markets.length === 0 && <p className="empty">No live market data yet for this card.</p>}

      <div className="section-header">
        <h2>Price history</h2>
        <div className="toggle-group">
          <button className={window === 'hourly' ? 'active' : ''} onClick={() => setWindowType('hourly')}>
            Hourly
          </button>
          <button className={window === 'daily' ? 'active' : ''} onClick={() => setWindowType('daily')}>
            Daily
          </button>
        </div>
      </div>
      <PriceChart data={chartData} />
    </div>
  );
}
