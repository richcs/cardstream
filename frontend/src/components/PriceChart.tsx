import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import type { WindowedAggregate } from '../api/types';

function formatTick(iso: string): string {
  const d = new Date(iso);
  return `${d.getMonth() + 1}/${d.getDate()} ${d.getHours()}:00`;
}

export function PriceChart({ data }: { data: WindowedAggregate[] }) {
  const points = data
    .slice()
    .sort((a, b) => a.windowStart.localeCompare(b.windowStart))
    .map((w) => ({ windowStart: w.windowStart, avgPrice: w.avgPrice, volatility: w.volatility }));

  if (points.length === 0) {
    return <p className="empty">No settled price history yet for this window.</p>;
  }

  return (
    <ResponsiveContainer width="100%" height={280}>
      <LineChart data={points} margin={{ top: 8, right: 16, bottom: 8, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" opacity={0.25} />
        <XAxis dataKey="windowStart" tickFormatter={formatTick} minTickGap={40} />
        <YAxis domain={['auto', 'auto']} width={70} tickFormatter={(v: number) => `$${v.toFixed(2)}`} />
        <Tooltip
          labelFormatter={(v) => (typeof v === 'string' ? new Date(v).toLocaleString() : String(v ?? ''))}
          formatter={(value) => [`$${Number(value).toFixed(2)}`, 'avgPrice']}
        />
        <Line type="monotone" dataKey="avgPrice" stroke="#4f8ef7" dot={false} strokeWidth={2} isAnimationActive={false} />
      </LineChart>
    </ResponsiveContainer>
  );
}
