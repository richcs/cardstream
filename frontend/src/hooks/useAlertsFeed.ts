import { useEffect, useRef, useState } from 'react';
import type { Alert } from '../api/types';
import { getUserId } from './useUserId';

const MAX_ITEMS = 100;
const RECONNECT_DELAY_MS = 3000;

function wsUrl(path: string): string {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  return `${protocol}//${window.location.host}${path}`;
}

/** Live `/ws/alerts` feed, newest first. `watchlistOnly` scopes the connection to the anon user's watchlist. */
export function useAlertsFeed(watchlistOnly: boolean): { alerts: Alert[]; connected: boolean } {
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [connected, setConnected] = useState(false);
  const closedByEffect = useRef(false);

  useEffect(() => {
    closedByEffect.current = false;
    setAlerts([]);
    let socket: WebSocket;
    let reconnectTimer: ReturnType<typeof setTimeout>;

    const connect = () => {
      const path = watchlistOnly ? `/ws/alerts?userId=${encodeURIComponent(getUserId())}` : '/ws/alerts';
      socket = new WebSocket(wsUrl(path));
      socket.onopen = () => setConnected(true);
      socket.onclose = () => {
        setConnected(false);
        if (!closedByEffect.current) {
          reconnectTimer = setTimeout(connect, RECONNECT_DELAY_MS);
        }
      };
      socket.onerror = () => socket.close();
      socket.onmessage = (event) => {
        try {
          const alert = JSON.parse(event.data) as Alert;
          setAlerts((prev) => [alert, ...prev].slice(0, MAX_ITEMS));
        } catch {
          // ignore malformed frame
        }
      };
    };
    connect();

    return () => {
      closedByEffect.current = true;
      clearTimeout(reconnectTimer);
      socket?.close();
    };
  }, [watchlistOnly]);

  return { alerts, connected };
}
