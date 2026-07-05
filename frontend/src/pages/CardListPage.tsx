import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { searchCards } from '../api/client';
import type { CardView, Page } from '../api/types';

const PAGE_SIZE = 24;

export function CardListPage() {
  const [q, setQ] = useState('');
  const [set, setSet] = useState('');
  const [rarity, setRarity] = useState('');
  const [page, setPage] = useState(0);
  const [result, setResult] = useState<Page<CardView> | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    searchCards({ q: q || undefined, set: set || undefined, rarity: rarity || undefined, page, pageSize: PAGE_SIZE })
      .then((res) => {
        if (!cancelled) setResult(res);
      })
      .catch(() => {
        if (!cancelled) setError('Failed to load cards.');
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [q, set, rarity, page]);

  const totalPages = result ? Math.max(1, Math.ceil(result.total / PAGE_SIZE)) : 1;

  return (
    <div>
      <h1>Cards</h1>
      <div className="filters">
        <input
          placeholder="Search name…"
          value={q}
          onChange={(e) => {
            setPage(0);
            setQ(e.target.value);
          }}
        />
        <input
          placeholder="Set id…"
          value={set}
          onChange={(e) => {
            setPage(0);
            setSet(e.target.value);
          }}
        />
        <input
          placeholder="Rarity…"
          value={rarity}
          onChange={(e) => {
            setPage(0);
            setRarity(e.target.value);
          }}
        />
      </div>

      {error && <p className="error">{error}</p>}
      {loading && !result && <p className="empty">Loading…</p>}

      {result && (
        <>
          <div className="card-grid">
            {result.items.map((card) => (
              <Link key={card.cardId} to={`/cards/${encodeURIComponent(card.cardId)}`} className="card-tile">
                {card.imageSmall && <img src={card.imageSmall} alt={card.name} loading="lazy" />}
                <div className="card-tile-name">{card.name}</div>
                <div className="card-tile-meta">
                  {card.setName} · {card.rarity}
                </div>
              </Link>
            ))}
          </div>
          {result.items.length === 0 && <p className="empty">No cards match.</p>}

          <div className="pager">
            <button disabled={page === 0} onClick={() => setPage((p) => p - 1)}>
              ← Prev
            </button>
            <span>
              Page {page + 1} of {totalPages} ({result.total} cards)
            </span>
            <button disabled={page + 1 >= totalPages} onClick={() => setPage((p) => p + 1)}>
              Next →
            </button>
          </div>
        </>
      )}
    </div>
  );
}
