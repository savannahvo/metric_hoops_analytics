'use client'
import { useState, useMemo } from 'react'
import Image from 'next/image'
import Link from 'next/link'
import useSWR from 'swr'
import { api } from '@/lib/api'
import { LoadingSkeleton } from '@/components/ui/LoadingSkeleton'
import { ErrorState } from '@/components/ui/ErrorState'
import { getHeadshotUrl, getLogoUrl, formatStat } from '@/lib/utils'
import type { Player } from '@/lib/types'

type SortKey = 'pts' | 'reb' | 'ast' | 'stl' | 'blk'
const SORT_OPTIONS: { key: SortKey; label: string }[] = [
  { key: 'pts', label: 'Points' },
  { key: 'reb', label: 'Rebounds' },
  { key: 'ast', label: 'Assists' },
  { key: 'stl', label: 'Steals' },
  { key: 'blk', label: 'Blocks' },
]

export default function PlayersPage() {
  const [search,  setSearch]  = useState('')
  const [sortKey, setSortKey] = useState<SortKey>('pts')

  const { data, error, isLoading, mutate } = useSWR(
    'players/stats',
    () => api.players.stats(),
    { refreshInterval: 3_600_000 },
  )

  const filtered = useMemo(() => {
    let list = data?.players ?? []
    if (search) {
      const q = search.toLowerCase()
      list = list.filter(p =>
        p.player_name.toLowerCase().includes(q) ||
        (p.team_name ?? '').toLowerCase().includes(q)
      )
    }
    return [...list].sort((a, b) => (b[sortKey] ?? 0) - (a[sortKey] ?? 0))
  }, [data, search, sortKey])

  return (
    <div>
      <div className="mb-6">
        <h1 className="text-2xl font-black text-white">Players</h1>
        <p className="text-metric-muted text-sm mt-1">2025-26 Season Stats</p>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap gap-3 mb-6">
        <input
          type="text"
          placeholder="Search players..."
          value={search}
          onChange={e => setSearch(e.target.value)}
          className="bg-metric-card border border-metric-border rounded-lg px-3 py-2 text-sm text-metric-text placeholder-metric-muted focus:outline-none focus:border-metric-accent w-56"
        />
        <div className="flex gap-1">
          {SORT_OPTIONS.map(opt => (
            <button
              key={opt.key}
              onClick={() => setSortKey(opt.key)}
              className={`px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
                sortKey === opt.key
                  ? 'bg-metric-accent text-white'
                  : 'bg-metric-card border border-metric-border text-metric-muted hover:text-metric-text'
              }`}
            >
              {opt.label}
            </button>
          ))}
        </div>
        <span className="text-metric-muted text-sm self-center">{filtered.length} players</span>
      </div>

      {isLoading ? (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
          {Array.from({ length: 12 }).map((_, i) => <LoadingSkeleton key={i} className="h-36 rounded-xl" />)}
        </div>
      ) : error ? (
        <ErrorState message="Could not load player stats" retry={() => mutate()} />
      ) : filtered.length === 0 ? (
        <div className="card p-8 text-center text-metric-muted">No players found</div>
      ) : (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
          {filtered.slice(0, 80).map((p: Player) => (
            <Link key={p.player_id} href={`/players/${p.player_id}`}>
              <div className="card p-4 hover:border-metric-accent/40 transition-colors cursor-pointer h-full">
                <div className="flex items-center gap-3 mb-3">
                  <div className="w-12 h-12 rounded-full bg-metric-border overflow-hidden relative shrink-0">
                    <Image
                      src={getHeadshotUrl(p.player_id)}
                      alt={p.player_name}
                      fill
                      className="object-cover"
                      onError={(e) => { (e.target as HTMLImageElement).style.display = 'none' }}
                    />
                  </div>
                  <div className="min-w-0">
                    <div className="font-bold text-sm truncate">{p.player_name}</div>
                    <div className="flex items-center gap-1.5 mt-0.5">
                      {p.team_id && (
                        <Image src={getLogoUrl(p.team_id)} alt="" width={16} height={16} />
                      )}
                      <span className="text-xs text-metric-muted truncate">{p.team_name}</span>
                      {p.position && (
                        <span className="text-xs text-metric-muted">· {p.position}</span>
                      )}
                    </div>
                  </div>
                </div>
                <div className="grid grid-cols-3 gap-2 text-center">
                  {[
                    { label: 'PPG', val: p.pts },
                    { label: 'RPG', val: p.reb },
                    { label: 'APG', val: p.ast },
                  ].map(s => (
                    <div key={s.label} className="bg-metric-border/40 rounded-lg py-1.5">
                      <div className="text-base font-black text-white">{formatStat(s.val)}</div>
                      <div className="text-xs text-metric-muted">{s.label}</div>
                    </div>
                  ))}
                </div>
                <div className="mt-2 text-xs text-metric-muted text-center">
                  {p.games_played} GP · FG {formatStat(p.fg_pct * 100, 1)}% · 3P {formatStat(p.fg3_pct * 100, 1)}%
                </div>
              </div>
            </Link>
          ))}
        </div>
      )}
    </div>
  )
}
