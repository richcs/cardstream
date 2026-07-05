import { useEffect, useRef } from 'react';
import type { WindowedAggregate } from '../api/types';

/** Live `/sse/prices` feed — pushes every settled windowed aggregate as the sink consumes it. */
export function usePriceStream(onAggregate: (agg: WindowedAggregate) => void): void {
  const callback = useRef(onAggregate);
  callback.current = onAggregate;

  useEffect(() => {
    const source = new EventSource('/sse/prices');
    source.onmessage = (event) => {
      try {
        callback.current(JSON.parse(event.data) as WindowedAggregate);
      } catch {
        // ignore malformed frame
      }
    };
    return () => source.close();
  }, []);
}
