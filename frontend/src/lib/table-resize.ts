export function resizeMapToWidths(sizes: Map<unknown, unknown>): Record<string, number> {
  const widths: Record<string, number> = {}
  for (const [key, size] of sizes) {
    if (typeof size === 'number' && Number.isFinite(size)) {
      widths[String(key)] = Math.round(size)
    }
  }
  return widths
}
