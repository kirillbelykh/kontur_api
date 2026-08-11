import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import '@fontsource-variable/inter'
import App from './App'
import './index.css'

async function bootstrap() {
  // QA-прогон без desktop shell: dev-only мок моста, в прод-сборку не попадает
  if (import.meta.env.DEV) {
    const qa = new URLSearchParams(window.location.search).get('qa')
    if (qa !== null) {
      const { installQaMock } = await import('./lib/qa-mock')
      installQaMock(qa)
    }
  }

  createRoot(document.getElementById('root')!).render(
    <StrictMode>
      <App />
    </StrictMode>,
  )
}

void bootstrap()
