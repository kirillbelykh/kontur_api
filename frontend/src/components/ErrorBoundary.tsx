import { Component, type ErrorInfo, type ReactNode } from 'react'

type ErrorBoundaryState = { error: Error | null }

/**
 * Крэш в любом экране больше не оставляет пустой WebView:
 * показываем сообщение и кнопку перезагрузки.
 */
export class ErrorBoundary extends Component<{ children: ReactNode }, ErrorBoundaryState> {
  state: ErrorBoundaryState = { error: null }

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { error }
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('UI crash:', error, info.componentStack)
  }

  render() {
    if (!this.state.error) return this.props.children

    return (
      <div className="flex min-h-screen items-center justify-center bg-background px-6">
        <div className="w-full max-w-md rounded-lg border border-border bg-card p-6 text-center shadow-panel">
          <h1 className="text-lg font-semibold text-foreground">Что-то пошло не так</h1>
          <p className="mt-2 text-sm text-muted-foreground">
            Интерфейс столкнулся с ошибкой. Перезагрузите приложение — данные на сервере не пострадали.
          </p>
          <p className="mt-2 break-words font-mono text-xs text-muted-foreground">
            {this.state.error.message}
          </p>
          <button
            type="button"
            onClick={() => window.location.reload()}
            className="mt-4 inline-flex h-9 items-center justify-center rounded-md border border-border bg-background px-4 text-sm font-medium text-foreground hover:bg-muted"
          >
            Перезагрузить
          </button>
        </div>
      </div>
    )
  }
}
