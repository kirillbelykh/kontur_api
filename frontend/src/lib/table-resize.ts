export function resizeMapToWidths(sizes: Map<unknown, unknown>): Record<string, number> {
  const widths: Record<string, number> = {}
  for (const [key, size] of sizes) {
    if (typeof size === 'number' && Number.isFinite(size)) {
      widths[String(key)] = Math.round(size)
    }
  }
  return widths
}

/** Стабильный ключ без pathname: HashRouter и file:// иначе сбрасывали настройки. */
export function tableStorageKey(ariaLabel: string, columnLabels: string[]) {
  return `kontur_table_prefs_v4_${ariaLabel}_${columnLabels.join('|')}`
}
