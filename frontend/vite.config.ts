import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Dev server proxies REST/WS/SSE to the backend so the browser only ever talks
// to one origin — no CORS config needed on the backend for local dev.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': 'http://localhost:8080',
      '/ws': { target: 'ws://localhost:8080', ws: true },
      '/sse': 'http://localhost:8080',
    },
  },
})
