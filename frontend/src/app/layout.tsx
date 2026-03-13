import type { Metadata } from 'next'
import './globals.css'
import NavBar from '@/components/layout/NavBar'

export const metadata: Metadata = {
  title: 'Metric Hoops | NBA Dashboard',
  description: 'ESPN-style NBA dashboard with ML game predictions',
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="min-h-screen" style={{ backgroundColor: '#0f1117' }}>
        {/* Subtle background texture */}
        <div
          className="fixed inset-0 z-0 bg-cover bg-center pointer-events-none"
          style={{
            backgroundImage: "url('/metric_hoops_background.png')",
            opacity: 0.04,
          }}
        />
        <div className="relative z-10">
          <NavBar />
          <main className="max-w-7xl mx-auto px-4 py-6">
            {children}
          </main>
        </div>
      </body>
    </html>
  )
}
