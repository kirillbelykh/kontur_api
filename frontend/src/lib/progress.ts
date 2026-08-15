export type DownloadProgressPayload = {
  documentId?: string
  documentIds?: string[]
  progress?: number
}

declare global {
  interface Window {
    __konturDownloadProgress?: (payload: DownloadProgressPayload) => void
  }
}

export function parseDownloadProgress(
  payload: DownloadProgressPayload | null | undefined,
): { ids: string[]; progress: number } | null {
  if (!payload || typeof payload !== 'object') return null
  const ids = [
    ...(Array.isArray(payload.documentIds) ? payload.documentIds : []),
    ...(payload.documentId ? [payload.documentId] : []),
  ]
    .map((id) => String(id || '').trim())
    .filter(Boolean)
  const progress = Number(payload.progress)
  if (!ids.length || !Number.isFinite(progress)) return null
  return { ids, progress: Math.min(1, Math.max(0, progress)) }
}

export function subscribeDownloadProgress(
  onProgress: (ids: string[], progress: number) => void,
): () => void {
  window.__konturDownloadProgress = (payload) => {
    const parsed = parseDownloadProgress(payload)
    if (!parsed) return
    onProgress(parsed.ids, parsed.progress)
  }
  return () => {
    delete window.__konturDownloadProgress
  }
}
