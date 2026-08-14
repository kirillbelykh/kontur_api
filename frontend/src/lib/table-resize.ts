export function resizeMapToWidths(sizes: Map<unknown, unknown>): Record<string, number> {
  const widths: Record<string, number> = {}
  for (const [key, size] of sizes) {
    if (typeof size === 'number' && Number.isFinite(size)) {
      widths[String(key)] = Math.round(size)
    }
  }
  return widths
}

export function widthsEqual(left: Record<string, number>, right: Record<string, number>) {
  const keys = Object.keys(left)
  if (keys.length !== Object.keys(right).length) return false
  return keys.every((key) => left[key] === right[key])
}

export function columnLayoutKey(columns: Array<{ id: string; label: string }>) {
  return columns.map((column) => `${column.id}:${column.label}`).join('|')
}

/** Стабильный ключ без pathname: HashRouter и file:// иначе сбрасывали настройки. */
export function tableStorageKey(ariaLabel: string, columnLabels: string[]) {
  return `kontur_table_prefs_v4_${ariaLabel}_${columnLabels.join('|')}`
}
