import { HeadContent, Scripts, createRootRoute } from '@tanstack/react-router'
import { SWRConfig } from 'swr'
import { Toaster as Sonner } from '@/components/ui/sonner'
import { Toaster } from '@/components/ui/toaster'
import { TooltipProvider } from '@/components/ui/tooltip'

import appCss from '../styles.css?url'

export const Route = createRootRoute({
  head: () => ({
    meta: [
      { charSet: 'utf-8' },
      { name: 'viewport', content: 'width=device-width, initial-scale=1' },
      { title: 'Scribo Stock Portal' },
      {
        name: 'description',
        content: 'Multi-store inventory overview — upload CSV data to explore stock across all your shops.',
      },
    ],
    links: [{ rel: 'stylesheet', href: appCss }],
  }),
  shellComponent: RootDocument,
})

function RootDocument({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <head>
        <HeadContent />
      </head>
      <body>
        <SWRConfig
          value={{
            revalidateOnFocus: true,
            shouldRetryOnError: false,
          }}
        >
          <TooltipProvider>
            <Toaster />
            <Sonner />
            {children}
          </TooltipProvider>
        </SWRConfig>
        <Scripts />
      </body>
    </html>
  )
}
