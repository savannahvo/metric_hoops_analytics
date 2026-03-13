'use client'
import useSWR from 'swr'
import { api } from '@/lib/api'
import { LoadingSkeleton } from '@/components/ui/LoadingSkeleton'

export default function PlayoffsPage() {
  const { data, error, isLoading } = useSWR(
    'playoffs',
    () => api.playoffs.bracket(),
    { refreshInterval: 3_600_000 },
  )

  const notStarted =
    !data ||
    (data as Record<string, unknown>).status === 'not_started' ||
    error

  return (
    <div>
      <div className="mb-6">
        <h1 className="text-2xl font-black text-white">NBA Playoffs</h1>
        <p className="text-metric-muted text-sm mt-1">2025-26 Bracket</p>
      </div>

      {isLoading ? (
        <LoadingSkeleton className="h-64 w-full rounded-xl" />
      ) : notStarted ? (
        <div className="card p-16 text-center">
          <div className="text-5xl mb-4">🏆</div>
          <h2 className="text-xl font-bold text-white mb-2">Playoffs Begin April 2026</h2>
          <p className="text-metric-muted">
            The NBA Playoffs bracket will appear here once the postseason begins.
            Check standings to track playoff race.
          </p>
        </div>
      ) : (
        <div className="card p-6">
          <pre className="text-xs text-metric-muted overflow-auto">
            {JSON.stringify(data, null, 2)}
          </pre>
        </div>
      )}
    </div>
  )
}
