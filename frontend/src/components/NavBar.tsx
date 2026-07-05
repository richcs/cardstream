import { NavLink } from 'react-router-dom';

const links = [
  { to: '/', label: 'Cards', end: true },
  { to: '/top-movers', label: 'Top Movers' },
  { to: '/arbitrage', label: 'Arbitrage' },
  { to: '/alerts', label: 'Alerts' },
];

export function NavBar() {
  return (
    <header className="nav">
      <span className="nav-brand">Cardstream</span>
      <nav>
        {links.map((l) => (
          <NavLink key={l.to} to={l.to} end={l.end} className={({ isActive }) => (isActive ? 'active' : '')}>
            {l.label}
          </NavLink>
        ))}
      </nav>
    </header>
  );
}
