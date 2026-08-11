import confetti from 'canvas-confetti'
import { getAppSetting } from '@/lib/app-settings'

let turn = 0

const CHECK_SVG =
  '<svg viewBox="0 0 48 48" fill="none" width="72" height="72">' +
  '<circle cx="24" cy="24" r="22" stroke="#10b981" stroke-width="3" opacity="0.35"/>' +
  '<path d="M15 24.5 21.5 31 33 18" stroke="#10b981" stroke-width="4" stroke-linecap="round" stroke-linejoin="round"/>' +
  '</svg>'

function fireConfetti() {
  void confetti({
    particleCount: 110,
    spread: 75,
    startVelocity: 42,
    origin: { x: 0.5, y: 0.75 },
    zIndex: 2147483646,
  })
}

function showSuccessCheck() {
  const host = document.createElement('div')
  host.style.cssText =
    'position:fixed;inset:0;display:flex;align-items:center;justify-content:center;pointer-events:none;z-index:2147483646'
  host.innerHTML = `<span class="t-success-check" data-state="out" aria-hidden="true">${CHECK_SVG}</span>`
  document.body.appendChild(host)
  const check = host.firstElementChild as HTMLElement
  requestAnimationFrame(() => check.setAttribute('data-state', 'in'))
  window.setTimeout(() => {
    check.style.transition = 'opacity 250ms ease-out'
    check.style.opacity = '0'
    window.setTimeout(() => host.remove(), 300)
  }, 1200)
}

/** После создания заказа: чередуем конфетти и анимированную галочку. */
export function celebrateOrderCreated() {
  if (!getAppSetting('animations')) return
  if (turn++ % 2 === 0) fireConfetti()
  else showSuccessCheck()
}
