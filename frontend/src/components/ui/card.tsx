import { Card as HeroCard } from '@heroui/react'
import type { ComponentProps } from 'react'

import { cn } from '@/lib/utils'

type CardProps = ComponentProps<typeof HeroCard>

/**
 * HeroUI Card (compound: Header / Title / Description / Content / Footer).
 * Совместимость: именованные экспорты CardHeader, CardTitle, …
 */
function CardRoot({ className, ...props }: CardProps) {
  return <HeroCard className={cn(className)} {...props} />
}

export const Card = Object.assign(CardRoot, {
  Header: HeroCard.Header,
  Title: HeroCard.Title,
  Description: HeroCard.Description,
  Content: HeroCard.Content,
  Footer: HeroCard.Footer,
})

export const CardHeader = HeroCard.Header
export const CardTitle = HeroCard.Title
export const CardDescription = HeroCard.Description
export const CardContent = HeroCard.Content
export const CardFooter = HeroCard.Footer
