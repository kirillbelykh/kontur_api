import {
  Children,
  isValidElement,
  type ComponentProps,
  type ReactNode,
} from 'react'
import { Modal, useOverlayState } from '@heroui/react'

import { cn } from '@/lib/utils'

type ModalSize = NonNullable<ComponentProps<typeof Modal.Container>['size']>

function resolveLayout(className?: string): {
  size: ModalSize
  dialogClassName?: string
} {
  if (!className) return { size: 'md' }

  const tokens = className.split(/\s+/).filter(Boolean)
  const maxWidth = tokens.find((token) => token.startsWith('max-w-'))

  let size: ModalSize = 'md'
  if (maxWidth === 'max-w-xs') size = 'xs'
  else if (maxWidth === 'max-w-sm') size = 'sm'
  else if (maxWidth === 'max-w-md') size = 'md'
  else if (maxWidth === 'max-w-lg') size = 'lg'
  else if (
    maxWidth === 'max-w-xl' ||
    maxWidth === 'max-w-2xl' ||
    maxWidth === 'max-w-3xl' ||
    maxWidth === 'max-w-4xl' ||
    maxWidth === 'max-w-5xl'
  ) {
    size = 'lg'
  } else if (maxWidth === 'max-w-full') {
    size = 'cover'
  }

  const dialogClassName = tokens
    .filter(
      (token) =>
        !token.startsWith('max-h-') &&
        token !== 'overflow-y-auto' &&
        token !== 'overflow-auto' &&
        token !== 'overflow-hidden',
    )
    .join(' ')

  return {
    size,
    dialogClassName: dialogClassName || undefined,
  }
}

/**
 * HeroUI Modal под прежним Dialog API.
 * Anatomy: Backdrop → Container → Dialog → CloseTrigger / Header / Body / Footer
 */
export function Dialog({
  open,
  onOpenChange,
  children,
}: {
  open?: boolean
  onOpenChange?: (open: boolean) => void
  children?: ReactNode
}) {
  const state = useOverlayState(
    open !== undefined ? { isOpen: open, onOpenChange } : { onOpenChange },
  )

  return <Modal state={state}>{children}</Modal>
}

/** asChild оставлен для совместимости — RAC DialogTrigger сам берёт pressable-ребёнка */
export function DialogTrigger({
  children,
}: {
  asChild?: boolean
  children?: ReactNode
}) {
  return <>{children}</>
}

export function DialogHeader({
  className,
  children,
  ...props
}: ComponentProps<typeof Modal.Header>) {
  return (
    <Modal.Header className={className} {...props}>
      {children}
    </Modal.Header>
  )
}

export function DialogTitle({
  className,
  children,
  ...props
}: ComponentProps<typeof Modal.Heading>) {
  return (
    <Modal.Heading className={className} {...props}>
      {children}
    </Modal.Heading>
  )
}

function DialogBodyFromChildren({ children }: { children: ReactNode }) {
  const items = Children.toArray(children)
  const headers: ReactNode[] = []
  const rest: ReactNode[] = []

  for (const child of items) {
    if (isValidElement(child) && child.type === DialogHeader) {
      headers.push(child)
    } else {
      rest.push(child)
    }
  }

  return (
    <>
      {headers}
      {rest.length > 0 ? <Modal.Body>{rest}</Modal.Body> : null}
    </>
  )
}

export function DialogContent({
  className,
  children,
  title,
}: {
  className?: string
  children: ReactNode
  title?: string
}) {
  const { size, dialogClassName } = resolveLayout(className)

  return (
    <Modal.Backdrop variant="blur">
      <Modal.Container size={size} scroll="inside" placement="center">
        <Modal.Dialog className={cn(dialogClassName)}>
          <Modal.CloseTrigger />
          {title ? (
            <>
              <Modal.Header>
                <Modal.Heading>{title}</Modal.Heading>
              </Modal.Header>
              <Modal.Body>{children}</Modal.Body>
            </>
          ) : (
            <DialogBodyFromChildren>{children}</DialogBodyFromChildren>
          )}
        </Modal.Dialog>
      </Modal.Container>
    </Modal.Backdrop>
  )
}

export function DrawerContent({
  className,
  children,
  title,
}: {
  className?: string
  children: ReactNode
  title: string
}) {
  return (
    <Modal.Backdrop variant="opaque">
      <Modal.Container
        size="lg"
        scroll="inside"
        placement="auto"
        className="items-stretch justify-end p-0 sm:p-0"
      >
        <Modal.Dialog
          className={cn(
            'mt-0 h-full min-h-full max-w-md rounded-none shadow-overlay sm:ms-auto',
            className,
          )}
        >
          <Modal.CloseTrigger />
          <Modal.Header>
            <Modal.Heading>{title}</Modal.Heading>
          </Modal.Header>
          <Modal.Body>{children}</Modal.Body>
        </Modal.Dialog>
      </Modal.Container>
    </Modal.Backdrop>
  )
}

/** Совместимость со старыми импортами */
export const DialogClose = Modal.CloseTrigger
export const DialogPortal = ({ children }: { children?: ReactNode }) => <>{children}</>
export const DialogOverlay = Modal.Backdrop
