import { Route, Routes } from 'react-router-dom';
import { NavBar } from './components/NavBar';
import { CardListPage } from './pages/CardListPage';
import { CardDetailPage } from './pages/CardDetailPage';
import { TopMoversPage } from './pages/TopMoversPage';
import { ArbitragePage } from './pages/ArbitragePage';
import { AlertsPage } from './pages/AlertsPage';

export function App() {
  return (
    <>
      <NavBar />
      <main className="container">
        <Routes>
          <Route path="/" element={<CardListPage />} />
          <Route path="/cards/:cardId" element={<CardDetailPage />} />
          <Route path="/top-movers" element={<TopMoversPage />} />
          <Route path="/arbitrage" element={<ArbitragePage />} />
          <Route path="/alerts" element={<AlertsPage />} />
        </Routes>
      </main>
    </>
  );
}
