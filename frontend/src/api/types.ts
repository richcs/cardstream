export type Game = 'POKEMON';
export type Finish = 'NORMAL' | 'HOLOFOIL' | 'REVERSE_HOLOFOIL';
export type Condition = 'NM' | 'LP' | 'MP' | 'HP' | 'DMG';
export type AlertType = 'SPIKE' | 'ARBITRAGE';
export type Severity = 'LOW' | 'MED' | 'HIGH';
export type WindowType = 'HOURLY' | 'DAILY' | 'MA_24H';

export interface CardView {
  cardId: string;
  name: string;
  number: string;
  rarity: string;
  supertype: string;
  game: Game;
  setId: string;
  setName: string;
  imageSmall: string;
  imageLarge: string;
}

export interface Page<T> {
  items: T[];
  page: number;
  pageSize: number;
  total: number;
}

export interface MarketSnapshot {
  marketKey: string;
  cardId: string;
  finish: Finish;
  condition: Condition;
  lastPrice: number;
  lastTradeAt: string;
  avgPrice: number;
  volatility: number;
  volume: number;
  sampleCount: number;
}

export interface CardDetailResponse {
  card: CardView;
  markets: MarketSnapshot[];
}

export interface WindowedAggregate {
  marketKey: string;
  cardId: string;
  windowType: WindowType;
  windowStart: string;
  windowEnd: string;
  avgPrice: number;
  volatility: number;
  volume: number;
  sampleCount: number;
}

export interface TopMover {
  marketKey: string;
  cardId: string;
  cardName: string;
  avgPrice: number;
  previousAvgPrice: number;
  pctChange: number;
}

export interface Alert {
  alertId: string;
  type: AlertType;
  severity: Severity;
  cardId: string;
  marketKey: string;
  name: string;
  detail: Record<string, unknown>;
  ts: string;
}
