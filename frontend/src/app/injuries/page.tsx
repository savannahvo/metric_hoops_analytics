'use client'
import { useState, useMemo } from 'react'
import useSWR from 'swr'
import { api } from '@/lib/api'
import { LoadingSkeleton } from '@/components/ui/LoadingSkeleton'
import { ErrorState } from '@/components/ui/ErrorState'
import { StatusBadge } from '@/components/ui/StatBadge'
import type { Injury } from '@/lib/types'

const STATUS_OPTIONS = ['All', 'Out', 'Doubtful', 'Questionable', 'Day-To-Day', 'Probable']

export default function InjuriesPage() {
  const [teamFilter, setTeamFilter]     = useState('')
  const [statusFilter, setStatusFilter] = useState('All')
  const [sourceFilter, setSourceFilter] = useState('all')

  const { data, error, isLoading, mutate } = useSWR(
    'injuries',
    () => api.injuries.get(),
    { refreshInterval: 3_600_000 },
  )

  const injuries = useMemo(() => {
    let list = data?.injuries ?? []
    if (teamFilter)              list = list.filter(i => i.team_name.toLowerCase().includes(teamFilter.toLowerCase()))
    if (statusFilter !== 'All')  list = list.filter(i => i.status === statusFilter)
    if (sourceFilter !== 'all')  list = list.filter(i => i.source === sourceFilter)
    return list
  }, [data, teamFilter, statusFilter, sourceFilter])

  // Group by team
  const byTeam = useMemo(() => {
    const map = new Map<string, Injury[]>()
    injuries.forEach(inj => {
      const key = inj.team_name || 'Unknown'
      if (!map.has(key)) map.set(key, [])
      map.get(key)!.push(inj)
    })
    return Array.from(map.entries()).sort(([a], [b]) => a.localeCompare(b))
  }, [injuries])

  return (
    <div>
      <div className="mb-6">
        <h1 className="text-2xl font-black text-white">Injury Report</h1>
        {data?.snapshot_date && (
          <p className="text-metric-muted text-sm mt-1">
            Updated: {new Date(data.snapshot_date + 'T00:00:00').toLocaleDateString('en-US', {
              weekday: 'long', month: 'long', day: 'numeric', year: 'numeric',
            })}
          </p>
        )}
      </div>

      {/* Filters */}
      <div className="flex flex-wrap gap-3 mb-6">
        <input
          type="text"
          placeholder="Search team..."
          value={teamFilter}
          onChange={e => setTeamFilter(e.target.value)}
          className="bg-metric-card border border-metric-border rounded-lg px-3 py-2 text-sm text-metric-text placeholder-metric-muted focus:outline-none focus:border-metric-accent w-48"
        />
        <select
          value={statusFilter}
          onChange={e => setStatusFilter(e.target.value)}
          className="bg-metric-card border border-metric-border rounded-lg px-3 py-2 text-sm text-metric-text focus:outline-none focus:border-metric-accent"
        >
          {STATUS_OPTIONS.map(s => <option key={s}>{s}</option>)}
        </select>
        <select
          value={sourceFilter}
          onChange={e => setSourceFilter(e.target.value)}
          className="bg-metric-card border border-metric-border rounded-lg px-3 py-2 text-sm text-metric-text focus:outline-none focus:border-metric-accent"
        >
          <option value="all">All Sources</option>
          <option value="espn">ESPN</option>
          <option value="cbs">CBS Sports</option>
        </select>
        <span className="text-metric-muted text-sm self-center">
          {injuries.length} players
        </span>
      </div>

      {/* Content */}
      {isLoading ? (
        <div className="space-y-3">
          {Array.from({ length: 5 }).map((_, i) => (
            <LoadingSkeleton key={i} className="h-20 w-full rounded-xl" />
          ))}
        </div>
      ) : error ? (
        <ErrorState message="Could not load injury report" retry={() => mutate()} />
      ) : byTeam.length === 0 ? (
        <div className="card p-8 text-center text-metric-muted">No injuries found matching filters</div>
      ) : (
        <div className="space-y-4">
          {byTeam.map(([team, players]) => (
            <div key={team} className="card overflow-hidden">
              <div className="px-4 py-2.5 border-b border-metric-border bg-metric-border/20">
                <h3 className="font-bold text-sm">{team}</h3>
              </div>
              <div className="divide-y divide-metric-border/40">
                {players.map((inj, i) => (
                  <div key={i} className="flex items-center px-4 py-3 gap-4 text-sm">
                    <div className="flex-1 min-w-0">
                      <span className="font-medium">{inj.player_name}</span>
                      {inj.position && (
                        <span className="text-metric-muted text-xs ml-2">{inj.position}</span>
                      )}
                    </div>
                    {inj.injury_type && (
                      <span className="text-metric-muted text-xs hidden sm:block truncate max-w-[140px]">
                        {inj.injury_type}
                      </span>
                    )}
                    <StatusBadge status={inj.status} />
                    {inj.updated && (
                      <span className="text-metric-muted text-xs hidden md:block whitespace-nowrap">
                        {inj.updated}
                      </span>
                    )}
                    <span className={`text-xs px-1.5 py-0.5 rounded ${
                      inj.source === 'espn' ? 'text-red-400 bg-red-900/20' : 'text-blue-400 bg-blue-900/20'
                    }`}>
                      {inj.source.toUpperCase()}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
