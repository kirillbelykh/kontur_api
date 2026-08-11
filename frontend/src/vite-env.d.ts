/// <reference types="vite/client" />

/** Версия из package.json — define в vite.config.ts */
declare const __APP_VERSION__: string

declare module '*.png' {
  const src: string
  export default src
}

declare module '*.jpg' {
  const src: string
  export default src
}

declare module '*.svg' {
  const src: string
  export default src
}
