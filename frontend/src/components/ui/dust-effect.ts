/**
 * «Distintegration into dust»: элемент размывается и уплывает, а из его
 * площади поднимается облако пылинок, которое уносит вправо-вверх.
 * Частицы рисуются на одном полноэкранном canvas — сотни точек без DOM-нод.
 */

import { getAppSetting } from '@/lib/app-settings'

type DustParticle = {
  x: number
  y: number
  vx: number
  vy: number
  radius: number
  delay: number
  life: number
  age: number
  shade: number
}

const ELEMENT_FADE_MS = 280
const PARTICLE_BUDGET = 1400

let activeCanvas: HTMLCanvasElement | null = null
let activeParticles: DustParticle[] = []
let rafHandle = 0
let lastFrameAt = 0

function ensureCanvas(): HTMLCanvasElement {
  if (activeCanvas && document.body.contains(activeCanvas)) return activeCanvas
  const canvas = document.createElement('canvas')
  canvas.width = window.innerWidth * devicePixelRatio
  canvas.height = window.innerHeight * devicePixelRatio
  canvas.setAttribute(
    'style',
    'position:fixed; inset:0; width:100%; height:100%; pointer-events:none; z-index:2147483647',
  )
  document.body.appendChild(canvas)
  activeCanvas = canvas
  return canvas
}

function frame(now: number) {
  const canvas = activeCanvas
  if (!canvas) return
  const ctx = canvas.getContext('2d')
  if (!ctx) return

  const dt = Math.min(0.05, (now - lastFrameAt) / 1000 || 0.016)
  lastFrameAt = now
  ctx.clearRect(0, 0, canvas.width, canvas.height)

  const scale = devicePixelRatio
  let alive = 0
  for (const p of activeParticles) {
    p.delay -= dt
    if (p.delay > 0) {
      alive++
      continue
    }
    p.age += dt
    if (p.age >= p.life) continue
    alive++

    // Ветер: разгон вправо + лёгкая турбулентность и подъём
    p.vx += 60 * dt
    p.vy += (Math.sin((p.x + p.age * 120) / 24) * 14 - 8) * dt
    p.x += p.vx * dt
    p.y += p.vy * dt

    const progress = p.age / p.life
    const opacity = progress < 0.55 ? 0.85 : 0.85 * (1 - (progress - 0.55) / 0.45)
    ctx.beginPath()
    ctx.fillStyle = `rgba(${p.shade}, ${p.shade}, ${Math.round(p.shade * 1.05)}, ${opacity.toFixed(3)})`
    ctx.arc(p.x * scale, p.y * scale, p.radius * scale * (1 - progress * 0.4), 0, Math.PI * 2)
    ctx.fill()
  }

  if (alive > 0) {
    rafHandle = requestAnimationFrame(frame)
  } else {
    activeParticles = []
    canvas.remove()
    activeCanvas = null
    rafHandle = 0
  }
}

function spawnParticlesForRect(rect: DOMRect, perElementBudget: number) {
  // Сетка с джиттером по площади элемента; распад идёт слева направо
  const area = rect.width * rect.height
  const step = Math.max(6, Math.sqrt(area / Math.max(40, perElementBudget)))
  for (let x = rect.left; x < rect.right; x += step) {
    for (let y = rect.top; y < rect.bottom; y += step) {
      const jitterX = x + (Math.random() - 0.5) * step
      const jitterY = y + (Math.random() - 0.5) * step
      const columnProgress = (x - rect.left) / Math.max(1, rect.width)
      activeParticles.push({
        x: jitterX,
        y: jitterY,
        vx: 30 + Math.random() * 90,
        vy: -20 - Math.random() * 45,
        radius: 1.4 + Math.random() * 2.6,
        delay: columnProgress * 0.15 + Math.random() * 0.06,
        life: 0.28 + Math.random() * 0.28,
        age: 0,
        shade: 130 + Math.floor(Math.random() * 60),
      })
    }
  }
}

/** Запускает распад элементов; резолвится, когда пыль улетела. */
export function dissolveToDust(elements: HTMLElement[]): Promise<void> {
  const targets = elements.filter(Boolean)
  if (targets.length === 0 || typeof document === 'undefined' || !getAppSetting('animations')) {
    return Promise.resolve()
  }

  ensureCanvas()
  const perElement = Math.max(60, Math.floor(PARTICLE_BUDGET / targets.length))
  let maxDurationMs = ELEMENT_FADE_MS

  for (const el of targets) {
    const rect = el.getBoundingClientRect()
    spawnParticlesForRect(rect, perElement)
    maxDurationMs = Math.max(maxDurationMs, (0.15 + 0.06 + 0.56) * 1000)

    el.style.transition = `opacity ${ELEMENT_FADE_MS}ms ease-out, filter ${ELEMENT_FADE_MS}ms ease-out, transform ${ELEMENT_FADE_MS}ms ease-in`
    el.style.pointerEvents = 'none'
    // Кадр спустя, чтобы transition применился
    requestAnimationFrame(() => {
      el.style.opacity = '0'
      el.style.filter = 'blur(5px)'
      el.style.transform = 'translateX(48px)'
    })
    // После фейда строка не должна занимать место — иначе в таблице остаётся серая дыра
    window.setTimeout(() => {
      el.style.display = 'none'
      el.style.height = '0'
      el.style.minHeight = '0'
      el.style.overflow = 'hidden'
      el.style.padding = '0'
      el.style.borderWidth = '0'
    }, ELEMENT_FADE_MS)
  }

  if (!rafHandle) {
    lastFrameAt = performance.now()
    rafHandle = requestAnimationFrame(frame)
  }

  return new Promise((resolve) => {
    window.setTimeout(resolve, maxDurationMs)
  })
}

/** Возвращает элементу исходный вид, если операция сорвалась. */
export function restoreDissolved(element: HTMLElement) {
  for (const prop of [
    'transition',
    'opacity',
    'filter',
    'transform',
    'pointer-events',
    'display',
    'height',
    'min-height',
    'overflow',
    'padding',
    'border-width',
  ]) {
    element.style.removeProperty(prop)
  }
}

if (import.meta.env.DEV && typeof window !== 'undefined') {
  ;(window as unknown as Record<string, unknown>).__dissolveToDust = dissolveToDust
}
