'use client'
import { useState, useMemo } from 'react'
import Image from 'next/image'
import useSWR from 'swr'
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from 'recharts'
import { api } from '@/lib/api'
import { LoadingSkeleton } from '@/components/ui/LoadingSkeleton'
import { ErrorState } from '@/components/ui/ErrorState'
import { getLogoUrl, formatDate, isGameFinal } from '@/lib/utils'
import type { Prediction } from '@/lib/types'

function dateRange(days: number): string[] {
  const out: string[] = []
  const today = new Date()
  for (let i = -days; i <= days; i++) {
    const d = new Date(today)
    d.setDate(today.getDate() + i)
    out.push(d.toISOString().slice(0, 10))
  }
  return out
}

export default function SchedulePage() {
  const today = new Date().toISOString().slice(0, 10)
  const [selectedDate, setSelectedDate] = useState(today)
  const [predPage, setPredPage] = useState(1)

  const dates = useMemo(() => dateRange(14), [])

  const { data: gamesData, isLoading: gLoading, error: gError } = useSWR(
    `games-${selectedDate}`,
    () => api.games.byDate(selectedDate),
  )

  const { data: predsData, isLoading: pLoading } = useSWR(
    `pred-log-${predPage}`,
    () => api.predictions.log(predPage, 20),
  )

  const { data: driftData } = useSWR('drift', () => api.predictions.drift())
  const { data: accuracy } = useSWR('acc', () => api.predictions.accuracy())

  const driftRows = (driftData?.drift ?? []).slice().reverse()
  const chartData = driftRows.map(d => ({
    date: formatDate(d.log_date),
    '7-day': +(d.rolling_7d_accuracy * 100).toFixed(1),
    '30-day': +(d.rolling_30d_accuracy * 100).toFixed(1),
  }))

  return (
    <div className="space-y-8">
      {/* Accuracy chart */}
      {accuracy && (
        <div className="card p-4">
          <div className="flex items-center justify-between mb-4">
            <h2 className="font-bold">Model Accuracy</h2>
            <div className="flex gap-4 text-sm">
              <span><span className="text-metric-muted">Season: </span>
                <span className="font-bold text-metric-accent">{(accuracy.season_accuracy * 100).toFixed(1)}%</span></span>
              <span><span className="text-metric-muted">7-day: </span>
                <span className="font-bold text-green-400">{(accuracy.rolling_7d * 100).toFixed(1)}%</span></span>
              <span><span className="text-metric-muted">30-day: </span>
                <span className="font-bold text-yellow-400">{(accuracy.rolling_30d * 100).toFixed(1)}%</span></span>
            </div>
          </div>
          {chartData.length > 0 ? (
            <ResponsiveContainer width="100%" height={180}>
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#2a2f3f" />
                <XAxis dataKey="date" tick={{ fill: '#64748b', fontSize: 11 }} />
                <YAxis domain={[40, 85]} tick={{ fill: '#64748b', fontSize: 11 }} unit="%" />
                <Tooltip
                  contentStyle={{ backgroundColor: '#1a1f2e', border: '1px solid #2a2f3f', borderRadius: 8 }}
                  labelStyle={{ color: '#e2e8f0' }}
                />
                <Legend wrapperStyle={{ fontSize: 12 }} />
                <Line type="monotone" dataKey="7-day"  stroke="#22c55e" strokeWidth={2} dot={false} />
                <Line type="monotone" dataKey="30-day" stroke="#eab308" strokeWidth={2} dot={false} />
              </LineChart>
            </ResponsiveContainer>
          ) : (
            <p className="text-metric-muted text-sm text-center py-6">No drift data yet — predictions settle after games complete</p>
          )}
        </div>
      )}

      {/* Date strip + games */}
      <div className="card overflow-hidden">
        {/* Calendar strip */}
        <div className="border-b border-metric-border px-2 py-3 flex gap-1 overflow-x-auto scrollbar-hide">
          {dates.map(date => {
            const isToday = date === today
            const isSelected = date === selectedDate
            const d = new Date(date + 'T00:00:00')
            return (
              <button
                key={date}
                onClick={() => setSelectedDate(date)}
                className={`flex flex-col items-center px-3 py-2 rounded-lg shrink-0 text-xs transition-colors ${
                  isSelected
                    ? 'bg-metric-accent text-white'
                    : isToday
                    ? 'bg-metric-border text-white'
                    : 'text-metric-muted hover:text-metric-text hover:bg-metric-border/50'
                }`}
              >
                <span className="font-medium">{d.toLocaleDateString('en-US', { weekday: 'short' })}</span>
                <span className="text-sm font-bold">{d.getDate()}</span>
              </button>
            )
          })}
        </div>

        {/* Games for selected date */}
        <div className="p-4">
          <h2 className="font-bold mb-3">
            {formatDate(selectedDate)} Games
          </h2>
          {gLoading ? (
            <div className="space-y-2">
              {Array.from({ length: 3 }).map((_, i) => <LoadingSkeleton key={i} className="h-14 rounded-lg" />)}
            </div>
          ) : gError ? (
            <p className="text-metric-muted text-sm">Could not load games</p>
          ) : !gamesData?.games?.length ? (
            <p className="text-metric-muted text-sm">No games on this date</p>
          ) : (
            <div className="space-y-2">
              {gamesData.games.map(game => (
                <div key={game.game_id} className="flex items-center gap-3 px-3 py-2.5 bg-metric-border/20 rounded-lg text-sm">
                  <div className="flex items-center gap-2 flex-1">
                    <Image src={getLogoUrl(game.away_team_id)} alt="" width={24} height={24} />
                    <span className="font-medium">{game.away_tricode}</span>
                    {isGameFinal(game.status) && (
                      <span className="text-metric-muted">{game.away_score}</span>
                    )}
                  </div>
                  <span className="text-metric-muted text-xs">
                    {isGameFinal(game.status) ? 'Final' : game.status}
                  </span>
                  <div className="flex items-center gap-2 flex-1 flex-row-reverse">
                    <Image src={getLogoUrl(game.home_team_id)} alt="" width={24} height={24} />
                    <span className="font-medium">{game.home_tricode}</span>
                    {isGameFinal(game.status) && (
                      <span className="text-metric-muted">{game.home_score}</span>
                    )}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Past predictions log */}
      <div className="card overflow-hidden">
        <div className="px-4 py-3 border-b border-metric-border">
          <h2 className="font-bold">Prediction Log</h2>
        </div>
        <div className="overflow-x-auto">
          {pLoading ? (
            <div className="p-4 space-y-2">
              {Array.from({ length: 5 }).map((_, i) => <LoadingSkeleton key={i} className="h-10 rounded" />)}
            </div>
          ) : !predsData?.predictions?.length ? (
            <p className="p-4 text-metric-muted text-sm">No settled predictions yet</p>
          ) : (
            <>
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-xs text-metric-muted border-b border-metric-border">
                    <th className="text-left px-4 py-2">Date</th>
                    <th className="text-left px-4 py-2">Matchup</th>
                    <th className="text-left px-4 py-2">Prediction</th>
                    <th className="text-right px-4 py-2">Prob</th>
                    <th className="text-left px-4 py-2">Result</th>
                    <th className="text-center px-4 py-2">✓</th>
                  </tr>
                </thead>
                <tbody>
                  {predsData.predictions.map((p: Prediction) => (
                    <tr key={p.id} className="border-b border-metric-border/40 hover:bg-metric-border/20">
                      <td className="px-4 py-2.5 text-metric-muted text-xs">{formatDate(p.game_date)}</td>
                      <td className="px-4 py-2.5 font-medium text-xs">
                        {p.away_team_name} @ {p.home_team_name}
                      </td>
                      <td className="px-4 py-2.5 text-xs">{p.predicted_winner}</td>
                      <td className="px-4 py-2.5 text-right text-xs text-metric-muted">
                        {p.home_win_prob != null ? `${(p.home_win_prob * 100).toFixed(0)}%` : '—'}
                      </td>
                      <td className="px-4 py-2.5 text-xs text-metric-muted">
                        {p.actual_winner ?? '—'}
                      </td>
                      <td className="px-4 py-2.5 text-center">
                        {p.correct == null ? (
                          <span className="text-metric-muted">—</span>
                        ) : p.correct ? (
                          <span className="text-green-400 font-bold">✓</span>
                        ) : (
                          <span className="text-red-400 font-bold">✗</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {/* Pagination */}
              <div className="flex justify-between items-center px-4 py-3 border-t border-metric-border">
                <button
                  onClick={() => setPredPage(p => Math.max(1, p - 1))}
                  disabled={predPage === 1}
                  className="text-sm text-metric-accent disabled:text-metric-muted"
                >
                  ← Previous
                </button>
                <span className="text-xs text-metric-muted">Page {predPage}</span>
                <button
                  onClick={() => setPredPage(p => p + 1)}
                  disabled={(predsData.predictions?.length ?? 0) < 20}
                  className="text-sm text-metric-accent disabled:text-metric-muted"
                >
                  Next →
                </button>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  )
}
