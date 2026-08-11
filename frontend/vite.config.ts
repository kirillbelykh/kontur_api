import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const pkg = JSON.parse(fs.readFileSync(path.resolve(__dirname, 'package.json'), 'utf-8'))

export default defineConfig({
  base: './',
  define: {
    __APP_VERSION__: JSON.stringify(pkg.version ?? '0.0.0'),
  },
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    // LAN + ngrok: bind all interfaces; allow any Host (Vite DNS-rebinding guard → 403)
    host: true,
    port: 5173,
    strictPort: true,
    allowedHosts: true,
  },
  build: {
    // Вендор отдельными чанками: быстрее парсинг при старте и кэш между обновлениями
    rollupOptions: {
      output: {
        manualChunks: {
          react: ['react', 'react-dom', 'react-router-dom'],
          heroui: ['@heroui/react'],
          motion: ['framer-motion'],
        },
      },
    },
  },
})

