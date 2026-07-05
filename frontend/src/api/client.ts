import type {
  Alert,
  AlertType,
  CardDetailResponse,
  CardView,
  Game,
  MarketSnapshot,
  Page,
  Severity,
  TopMover,
  WindowedAggregate,
} from './types';

class ApiError extends Error {
  status: number;

  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, init);
  if (!res.ok) {
    throw new ApiError(res.status, `${path} -> ${res.status}`);
  }
  if (res.status === 204 || res.headers.get('content-length') === '0') {
    return undefined as T;
  }
  return res.json() as Promise<T>;
}

function get<T>(path: string): Promise<T> {
  return request(path);
}

function qs(params: Record<string, string | number | undefined>): string {
  const usp = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== '') {
      usp.set(k, String(v));
    }
  }
  const s = usp.toString();
  return s ? `?${s}` : '';
}

export function searchCards(params: {
  game?: Game;
  set?: string;
  rarity?: string;
  q?: string;
  page?: number;
  pageSize?: number;
}): Promise<Page<CardView>> {
  return get(`/api/cards${qs(params)}`);
}

export function getCardDetail(cardId: string): Promise<CardDetailResponse> {
  return get(`/api/cards/${encodeURIComponent(cardId)}`);
}

export function getCardHistory(
  cardId: string,
  window: 'hourly' | 'daily',
  from?: string,
  to?: string,
): Promise<WindowedAggregate[]> {
  return get(`/api/cards/${encodeURIComponent(cardId)}/history${qs({ window, from, to })}`);
}

export function getMarketSnapshot(marketKey: string): Promise<MarketSnapshot> {
  return get(`/api/market/${encodeURIComponent(marketKey)}`);
}

export function getTopMovers(params: {
  window?: 'hourly' | 'daily';
  dir?: 'gainers' | 'losers';
  limit?: number;
}): Promise<TopMover[]> {
  return get(`/api/top-movers${qs(params)}`);
}

export function getArbitrage(limit = 50): Promise<Alert[]> {
  return get(`/api/arbitrage${qs({ limit })}`);
}

export function getAlerts(params: { severity?: Severity; type?: AlertType; limit?: number }): Promise<Alert[]> {
  return get(`/api/alerts${qs(params)}`);
}

export function getWatchlist(userId: string): Promise<CardView[]> {
  return request('/api/watchlist', { headers: { 'X-User-Id': userId } });
}

export function addToWatchlist(userId: string, cardId: string): Promise<void> {
  return request('/api/watchlist', {
    method: 'POST',
    headers: { 'X-User-Id': userId, 'Content-Type': 'application/json' },
    body: JSON.stringify({ cardId }),
  });
}

export function removeFromWatchlist(userId: string, cardId: string): Promise<void> {
  return request(`/api/watchlist/${encodeURIComponent(cardId)}`, {
    method: 'DELETE',
    headers: { 'X-User-Id': userId },
  });
}
