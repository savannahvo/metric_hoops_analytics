'use client'
import Image from 'next/image'
import Link from 'next/link'
import { usePathname } from 'next/navigation'

const TABS = [
  { label: 'Matchups',     href: '/' },
  { label: 'Schedule',     href: '/schedule' },
  { label: 'Standings',    href: '/standings' },
  { label: 'Players',      href: '/players' },
  { label: 'Injuries',     href: '/injuries' },
  { label: 'Playoffs',     href: '/playoffs' },
  { label: 'Transactions', href: '/transactions' },
  { label: 'Model',        href: '/model' },
]

export default function NavBar() {
  const pathname = usePathname()

  return (
    <nav
      className="sticky top-0 z-50 border-b border-metric-border"
      style={{ backgroundColor: '#0d1019' }}
    >
      <div className="max-w-7xl mx-auto px-4">
        <div className="flex items-center h-16 gap-4">
          {/* Logo */}
          <Link href="/" className="flex items-center gap-2 shrink-0">
            <div className="w-9 h-9 relative">
              <Image
                src="/metric_logo.png"
                alt="Metric Hoops"
                fill
                className="object-contain"
              />
            </div>
            <span className="font-bold text-white text-base hidden sm:block tracking-tight">
              Metric Hoops
            </span>
          </Link>

          {/* Divider */}
          <div className="h-6 w-px bg-metric-border hidden sm:block" />

          {/* Tabs */}
          <div className="flex items-center gap-0.5 overflow-x-auto scrollbar-hide flex-1">
            {TABS.map((tab) => {
              const active =
                tab.href === '/'
                  ? pathname === '/'
                  : pathname.startsWith(tab.href)
              return (
                <Link
                  key={tab.href}
                  href={tab.href}
                  className={`px-3 py-2 rounded-lg text-sm font-medium whitespace-nowrap transition-all ${
                    active
                      ? 'bg-metric-accent/20 text-metric-accent border border-metric-accent/30'
                      : 'text-metric-muted hover:text-metric-text hover:bg-metric-card'
                  }`}
                >
                  {tab.label}
                </Link>
              )
            })}
          </div>
        </div>
      </div>
    </nav>
  )
}
