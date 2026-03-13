'use client'
import Image from 'next/image'
import useSWR from 'swr'
import { api } from '@/lib/api'
import { LoadingSkeleton } from '@/components/ui/LoadingSkeleton'
import { ErrorState } from '@/components/ui/ErrorState'
import type { TeamStanding } from '@/lib/types'
import { getLogoUrl, pctColor } from '@/lib/utils'

function ConferenceTable({ teams, title }: { teams: TeamStanding[]; title: string }) {
  return (
    <div className="card overflow-hidden">
      <div className="px-4 py-3 border-b border-metric-border">
        <h2 className="font-bold text-lg">{title}</h2>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-xs text-metric-muted border-b border-metric-border">
              <th className="text-left px-4 py-2 w-6">#</th>
              <th className="text-left px-2 py-2">Team</th>
              <th className="text-right px-3 py-2">W</th>
              <th className="text-right px-3 py-2">L</th>
              <th className="text-right px-3 py-2">PCT</th>
              <th className="text-right px-3 py-2">GB</th>
              <th className="text-right px-3 py-2 hidden sm:table-cell">Home</th>
              <th className="text-right px-3 py-2 hidden sm:table-cell">Away</th>
              <th className="text-right px-3 py-2 hidden md:table-cell">L10</th>
              <th className="text-right px-4 py-2 hidden md:table-cell">Strk</th>
            </tr>
          </thead>
          <tbody>
            {teams.map((t, idx) => {
              const pos = idx + 1
              const rowBg =
                pos <= 6  ? 'border-l-2 border-l-green-500' :
                pos <= 8  ? 'border-l-2 border-l-yellow-500' :
                pos <= 10 ? 'border-l-2 border-l-blue-500' : ''

              return (
                <tr
                  key={t.team_id}
                  className={`border-b border-metric-border/50 hover:bg-metric-border/30 transition-colors ${rowBg}`}
                >
                  <td className="px-4 py-3 text-metric-muted text-xs">{pos}</td>
                  <td className="px-2 py-3">
                    <div className="flex items-center gap-2">
                      <div className="w-7 h-7 relative shrink-0">
                        <Image
                          src={getLogoUrl(t.team_id)}
                          alt={t.tricode}
                          fill className="object-contain"
                          onError={(e) => { (e.target as HTMLImageElement).style.display = 'none' }}
                        />
                      </div>
                      <span className="font-medium text-sm hidden sm:inline">{t.team_name}</span>
                      <span className="font-medium text-sm sm:hidden">{t.tricode}</span>
                    </div>
                  </td>
                  <td className="text-right px-3 py-3 font-bold">{t.wins}</td>
                  <td className="text-right px-3 py-3 text-metric-muted">{t.losses}</td>
                  <td className={`text-right px-3 py-3 font-medium ${pctColor(t.win_pct)}`}>
                    {t.win_pct.toFixed(3)}
                  </td>
                  <td className="text-right px-3 py-3 text-metric-muted text-xs">
                    {t.gb === 0 ? '—' : t.gb.toFixed(1)}
                  </td>
                  <td className="text-right px-3 py-3 text-metric-muted hidden sm:table-cell text-xs">{t.home_record}</td>
                  <td className="text-right px-3 py-3 text-metric-muted hidden sm:table-cell text-xs">{t.away_record}</td>
                  <td className="text-right px-3 py-3 text-metric-muted hidden md:table-cell text-xs">{t.last_10}</td>
                  <td className={`text-right px-4 py-3 font-medium text-xs hidden md:table-cell ${
                    t.streak?.startsWith('W') ? 'text-green-400' : 'text-red-400'
                  }`}>
                    {t.streak}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
      {/* Legend */}
      <div className="px-4 py-2 border-t border-metric-border flex gap-4 text-xs text-metric-muted">
        <span><span className="text-green-400">■</span> Playoff (1-6)</span>
        <span><span className="text-yellow-400">■</span> Play-in (7-8)</span>
        <span><span className="text-blue-400">■</span> Play-in (9-10)</span>
      </div>
    </div>
  )
}

export default function StandingsPage() {
  const { data, error, isLoading, mutate } = useSWR(
    'standings',
    () => api.standings.get(),
    { refreshInterval: 300_000 },
  )

  return (
    <div>
      <div className="mb-6">
        <h1 className="text-2xl font-black text-white">Standings</h1>
        <p className="text-metric-muted text-sm mt-1">2025-26 NBA Season</p>
      </div>

      {isLoading ? (
        <div className="space-y-4">
          {Array.from({ length: 3 }).map((_, i) => (
            <LoadingSkeleton key={i} className="h-64 w-full rounded-xl" />
          ))}
        </div>
      ) : error ? (
        <ErrorState message="Could not load standings" retry={() => mutate()} />
      ) : (
        <div className="space-y-6">
          {data?.east && <ConferenceTable teams={data.east} title="Eastern Conference" />}
          {data?.west && <ConferenceTable teams={data.west} title="Western Conference" />}
        </div>
      )}
    </div>
  )
}
