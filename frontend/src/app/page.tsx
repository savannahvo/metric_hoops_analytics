'use client'
import useSWR from 'swr'
import { api } from '@/lib/api'
import { MatchupCard } from '@/components/matchups/MatchupCard'
import { CardSkeleton } from '@/components/ui/LoadingSkeleton'
import { ErrorState } from '@/components/ui/ErrorState'
import type { Prediction } from '@/lib/types'

export default function MatchupsPage() {
  const {
    data: gamesData,
    error: gamesError,
    isLoading,
    mutate,
  } = useSWR('games/today', () => api.games.today(), { refreshInterval: 30_000 })

  const { data: predsData } = useSWR(
    'predictions/today',
    () => api.predictions.today(),
    { refreshInterval: 60_000 },
  )

  const { data: accuracy } = useSWR('predictions/accuracy', () => api.predictions.accuracy())

  const games = gamesData?.games ?? []
  const predictions = predsData?.predictions ?? []
  const predMap = Object.fromEntries(predictions.map((p: Prediction) => [p.game_id, p]))

  const today = new Date().toLocaleDateString('en-US', {
    weekday: 'long', month: 'long', day: 'numeric',
  })

  return (
    <div>
      {/* Hero */}
      <div className="relative rounded-2xl mb-8 overflow-hidden">
        <div
          className="absolute inset-0 bg-cover bg-center"
          style={{
            backgroundImage: "url('/metric_hoops_background.png')",
            opacity: 0.12,
          }}
        />
        <div className="relative px-8 py-10" style={{ background: 'linear-gradient(135deg, rgba(15,17,23,0.97), rgba(26,31,46,0.93))' }}>
          <h1 className="text-3xl font-black text-white mb-1 tracking-tight">Today&apos;s Games</h1>
          <p className="text-metric-muted">{today}</p>
          {accuracy && (
            <div className="mt-4 flex gap-6 text-sm">
              <span>
                <span className="text-metric-muted">7-day accuracy: </span>
                <span className="font-bold text-metric-green">{(accuracy.rolling_7d * 100).toFixed(1)}%</span>
              </span>
              <span>
                <span className="text-metric-muted">Season: </span>
                <span className="font-bold text-metric-accent">{(accuracy.season_accuracy * 100).toFixed(1)}%</span>
              </span>
              <span className="text-metric-muted text-xs self-center">
                ({accuracy.correct_games}/{accuracy.total_games} correct)
              </span>
            </div>
          )}
        </div>
      </div>

      {/* Content */}
      {isLoading ? (
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
          {Array.from({ length: 8 }).map((_, i) => <CardSkeleton key={i} />)}
        </div>
      ) : gamesError ? (
        <ErrorState message="Could not load today's games" retry={() => mutate()} />
      ) : games.length === 0 ? (
        <div className="card p-12 text-center">
          <div className="text-4xl mb-4">🏀</div>
          <p className="text-metric-text text-lg font-medium mb-2">No games today</p>
          <p className="text-metric-muted text-sm">Check the Schedule tab for upcoming games.</p>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
          {games.map((game) => (
            <MatchupCard
              key={game.game_id}
              game={game}
              prediction={predMap[game.game_id]}
            />
          ))}
        </div>
      )}
    </div>
  )
}
