import type { Metadata } from 'next'
import { Inter } from 'next/font/google'
import './globals.css'

const inter = Inter({ subsets: ['latin'] })

export const metadata: Metadata = {
  title: 'DFI Labs Admin - Factsheet Platform',
  description: 'Admin panel for DFI Labs factsheet generation platform',
  keywords: 'admin, factsheet, investment, portfolio, DFI Labs, quantitative, management',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body className={inter.className}>
        <div className="min-h-screen bg-dfi-light">
          {children}
        </div>
      </body>
    </html>
  )
}
