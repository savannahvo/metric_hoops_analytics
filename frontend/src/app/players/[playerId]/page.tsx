'use client'
import { use } from 'react'
import Image from 'next/image'
import useSWR from 'swr'
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend,
  RadarChart, Radar, PolarGrid, PolarAngleAxis, PolarRadiusAxis,
} from 'recharts'
import { api } from '@/lib/api'
import { LoadingSkeleton } from '@/components/ui/LoadingSkeleton'
import { ErrorState } from '@/components/ui/ErrorState'
import { getHeadshotUrl, getLogoUrl, formatStat, formatDate } from '@/lib/utils'
import type { PlayerGameLog } from '@/lib/types'

export default function PlayerPage({ params }: { params: Promise<{ playerId: string }> }) {
  const { playerId } = use(params)
  const id = parseInt(playerId, 10)

  const { data: glData, isLoading, error } = useSWR(
    `player-gl-${id}`,
    () => api.players.gamelogs(id),
  )
  const { data: infoData } = useSWR(`player-info-${id}`, () => api.players.info(id))

  const gamelogs = glData?.gamelogs ?? []
  const last15   = gamelogs.slice(0, 15).reverse()
  const info = infoData as Record<string, string | number> | null

  // Season averages
  const avgs = gamelogs.length > 0 ? {
    pts: gamelogs.reduce((s, g) => s + (g.pts ?? 0), 0) / gamelogs.length,
    reb: gamelogs.reduce((s, g) => s + (g.reb ?? 0), 0) / gamelogs.length,
    ast: gamelogs.reduce((s, g) => s + (g.ast ?? 0), 0) / gamelogs.length,
    stl: gamelogs.reduce((s, g) => s + (g.stl ?? 0), 0) / gamelogs.length,
    blk: gamelogs.reduce((s, g) => s + (g.blk ?? 0), 0) / gamelogs.length,
    fg_pct: gamelogs.reduce((s, g) => s + (g.fg_pct ?? 0), 0) / gamelogs.length,
    fg3_pct: gamelogs.reduce((s, g) => s + (g.fg3_pct ?? 0), 0) / gamelogs.length,
  } : null

  // Radar data vs league average
  const radarData = avgs ? [
    { stat: 'PTS',  player: Math.min(avgs.pts / 35 * 100, 100),  avg: 55 },
    { stat: 'REB',  player: Math.min(avgs.reb / 14 * 100, 100),  avg: 55 },
    { stat: 'AST',  player: Math.min(avgs.ast / 10 * 100, 100),  avg: 45 },
    { stat: 'STL',  player: Math.min(avgs.stl / 3 * 100, 100),   avg: 40 },
    { stat: 'BLK',  player: Math.min(avgs.blk / 3 * 100, 100),   avg: 35 },
    { stat: 'FG%',  player: Math.min(avgs.fg_pct * 100 / 0.60 * 100, 100), avg: 75 },
    { stat: '3P%',  player: Math.min(avgs.fg3_pct * 100 / 0.45 * 100, 100), avg: 65 },
  ] : []

  // Trend data
  const trendData = last15.map((g: PlayerGameLog) => ({
    date: formatDate(g.game_date),
    PTS:  g.pts,
    REB:  g.reb,
    AST:  g.ast,
  }))

  const playerName = String(info?.player_name ?? info?.displayFirstLast ?? `Player ${id}`)
  const teamId     = Number(info?.team_id ?? 0)

  return (
    <div className="space-y-6">
      {isLoading ? (
        <>
          <LoadingSkeleton className="h-40 rounded-2xl" />
          <LoadingSkeleton className="h-64 rounded-xl" />
        </>
      ) : error ? (
        <ErrorState message="Could not load player data" />
      ) : (
        <>
          {/* Hero */}
          <div className="card p-6 flex items-center gap-6">
            <div className="w-24 h-24 rounded-2xl bg-metric-border overflow-hidden relative shrink-0">
              <Image
                src={getHeadshotUrl(id)}
                alt={playerName}
                fill
                className="object-cover"
                onError={(e) => { (e.target as HTMLImageElement).style.display = 'none' }}
              />
            </div>
            <div>
              <h1 className="text-2xl font-black text-white">{playerName}</h1>
              <div className="flex items-center gap-2 mt-1">
                {teamId > 0 && <Image src={getLogoUrl(teamId)} alt="" width={24} height={24} />}
                <span className="text-metric-muted text-sm">
                  {String(info?.team_name ?? '')} · {String(info?.position ?? '')}
                </span>
              </div>
              {avgs && (
                <div className="flex gap-6 mt-3 text-sm">
                  {[
                    { l: 'PPG', v: avgs.pts },
                    { l: 'RPG', v: avgs.reb },
                    { l: 'APG', v: avgs.ast },
                    { l: 'FG%', v: avgs.fg_pct * 100 },
                    { l: '3P%', v: avgs.fg3_pct * 100 },
                  ].map(s => (
                    <div key={s.l} className="text-center">
                      <div className="text-xl font-black text-white">{formatStat(s.v)}</div>
                      <div className="text-xs text-metric-muted">{s.l}</div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>

          {/* Charts */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Trend */}
            <div className="card p-4">
              <h2 className="font-bold mb-4">Last 15 Games</h2>
              {trendData.length > 0 ? (
                <ResponsiveContainer width="100%" height={200}>
                  <LineChart data={trendData}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#2a2f3f" />
                    <XAxis dataKey="date" tick={{ fill: '#64748b', fontSize: 10 }} />
                    <YAxis tick={{ fill: '#64748b', fontSize: 10 }} />
                    <Tooltip contentStyle={{ backgroundColor: '#1a1f2e', border: '1px solid #2a2f3f', borderRadius: 8 }} />
                    <Legend wrapperStyle={{ fontSize: 12 }} />
                    <Line type="monotone" dataKey="PTS" stroke="#3b82f6" strokeWidth={2} dot={false} />
                    <Line type="monotone" dataKey="REB" stroke="#22c55e" strokeWidth={2} dot={false} />
                    <Line type="monotone" dataKey="AST" stroke="#f59e0b" strokeWidth={2} dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              ) : (
                <p className="text-metric-muted text-sm text-center py-8">No game log data</p>
              )}
            </div>

            {/* Radar */}
            <div className="card p-4">
              <h2 className="font-bold mb-4">vs. Position Average</h2>
              {radarData.length > 0 ? (
                <ResponsiveContainer width="100%" height={200}>
                  <RadarChart data={radarData}>
                    <PolarGrid stroke="#2a2f3f" />
                    <PolarAngleAxis dataKey="stat" tick={{ fill: '#64748b', fontSize: 11 }} />
                    <PolarRadiusAxis domain={[0, 100]} tick={false} axisLine={false} />
                    <Radar name="Player" dataKey="player" stroke="#3b82f6" fill="#3b82f6" fillOpacity={0.3} />
                    <Radar name="Avg"    dataKey="avg"    stroke="#64748b" fill="#64748b" fillOpacity={0.15} />
                    <Legend wrapperStyle={{ fontSize: 12 }} />
                  </RadarChart>
                </ResponsiveContainer>
              ) : (
                <p className="text-metric-muted text-sm text-center py-8">No data available</p>
              )}
            </div>
          </div>

          {/* Game log table */}
          <div className="card overflow-hidden">
            <div className="px-4 py-3 border-b border-metric-border">
              <h2 className="font-bold">Game Log</h2>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-xs text-metric-muted border-b border-metric-border">
                    {['Date', 'W/L', 'MIN', 'PTS', 'REB', 'AST', 'STL', 'BLK', '+/-', 'FG%', '3P%'].map(h => (
                      <th key={h} className="text-right first:text-left px-3 py-2">{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {gamelogs.slice(0, 30).map((g: PlayerGameLog, i: number) => (
                    <tr key={i} className="border-b border-metric-border/40 hover:bg-metric-border/20">
                      <td className="px-3 py-2.5 text-metric-muted text-xs">{formatDate(g.game_date)}</td>
                      <td className={`text-right px-3 py-2.5 text-xs font-bold ${g.win ? 'text-green-400' : 'text-red-400'}`}>
                        {g.win ? 'W' : 'L'}
                      </td>
                      <td className="text-right px-3 py-2.5 text-metric-muted text-xs">{formatStat(g.minutes, 0)}</td>
                      <td className="text-right px-3 py-2.5 font-bold">{g.pts}</td>
                      <td className="text-right px-3 py-2.5">{g.reb}</td>
                      <td className="text-right px-3 py-2.5">{g.ast}</td>
                      <td className="text-right px-3 py-2.5 text-metric-muted">{g.stl}</td>
                      <td className="text-right px-3 py-2.5 text-metric-muted">{g.blk}</td>
                      <td className={`text-right px-3 py-2.5 text-xs font-medium ${
                        (g.plus_minus ?? 0) > 0 ? 'text-green-400' : (g.plus_minus ?? 0) < 0 ? 'text-red-400' : 'text-metric-muted'
                      }`}>
                        {(g.plus_minus ?? 0) > 0 ? '+' : ''}{g.plus_minus}
                      </td>
                      <td className="text-right px-3 py-2.5 text-metric-muted text-xs">{formatStat((g.fg_pct ?? 0) * 100, 1)}%</td>
                      <td className="text-right px-3 py-2.5 text-metric-muted text-xs">{formatStat((g.fg3_pct ?? 0) * 100, 1)}%</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}
    </div>
  )
}
