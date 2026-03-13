'use client'
import { useState, useMemo } from 'react'
import useSWR from 'swr'
import { api } from '@/lib/api'
import { LoadingSkeleton } from '@/components/ui/LoadingSkeleton'
import { ErrorState } from '@/components/ui/ErrorState'

const TX_TYPES = ['All', 'Trade', 'Signing', 'Waiver', 'Released', 'Two-Way']

export default function TransactionsPage() {
  const [typeFilter, setTypeFilter] = useState('All')

  const { data, error, isLoading, mutate } = useSWR(
    'transactions',
    () => api.transactions.get(),
    { refreshInterval: 3_600_000 },
  )

  const transactions = useMemo(() => {
    const list = (data?.transactions ?? []) as Record<string, string>[]
    if (typeFilter === 'All') return list
    return list.filter(t =>
      String(t.transaction_type || '').toLowerCase().includes(typeFilter.toLowerCase())
    )
  }, [data, typeFilter])

  function txTypeColor(type: string) {
    const t = type.toLowerCase()
    if (t.includes('trade'))   return 'text-blue-400 bg-blue-900/30'
    if (t.includes('sign'))    return 'text-green-400 bg-green-900/30'
    if (t.includes('waive') || t.includes('release')) return 'text-red-400 bg-red-900/30'
    if (t.includes('two-way')) return 'text-yellow-400 bg-yellow-900/30'
    return 'text-metric-muted bg-metric-border'
  }

  function formatTxDate(dateStr: string) {
    if (!dateStr) return ''
    try {
      return new Date(dateStr).toLocaleDateString('en-US', {
        month: 'short', day: 'numeric', year: 'numeric',
      })
    } catch { return dateStr }
  }

  return (
    <div>
      <div className="mb-6">
        <h1 className="text-2xl font-black text-white">Transactions</h1>
        <p className="text-metric-muted text-sm mt-1">Player movement — trades, signings, waivers</p>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap gap-2 mb-6">
        {TX_TYPES.map(t => (
          <button
            key={t}
            onClick={() => setTypeFilter(t)}
            className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-colors ${
              typeFilter === t
                ? 'bg-metric-accent text-white'
                : 'bg-metric-card border border-metric-border text-metric-muted hover:text-metric-text'
            }`}
          >
            {t}
          </button>
        ))}
      </div>

      {isLoading ? (
        <div className="space-y-2">
          {Array.from({ length: 8 }).map((_, i) => (
            <LoadingSkeleton key={i} className="h-16 w-full rounded-xl" />
          ))}
        </div>
      ) : error ? (
        <ErrorState message="Could not load transactions" retry={() => mutate()} />
      ) : transactions.length === 0 ? (
        <div className="card p-8 text-center text-metric-muted">No transactions found</div>
      ) : (
        <div className="card divide-y divide-metric-border/50">
          {transactions.map((tx, i) => (
            <div key={i} className="flex items-start gap-4 px-4 py-3">
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 flex-wrap">
                  <span className="font-bold text-sm">{tx.player_name}</span>
                  {tx.transaction_type && (
                    <span className={`text-xs px-2 py-0.5 rounded font-medium ${txTypeColor(tx.transaction_type)}`}>
                      {tx.transaction_type}
                    </span>
                  )}
                </div>
                <div className="text-xs text-metric-muted mt-0.5 flex items-center gap-1.5">
                  {tx.team_from && <span>{tx.team_from}</span>}
                  {tx.team_from && tx.team_to && <span>→</span>}
                  {tx.team_to && <span className="text-metric-text">{tx.team_to}</span>}
                </div>
                {tx.notes && (
                  <p className="text-xs text-metric-muted mt-1 line-clamp-2">{tx.notes}</p>
                )}
              </div>
              <span className="text-xs text-metric-muted whitespace-nowrap shrink-0 mt-0.5">
                {formatTxDate(tx.transaction_date)}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
