export function formatGameStatus(status: string, period: number, clock: string): string {
  if (!status) return ''
  const s = status.toLowerCase()
  if (s.includes('final') || s.includes('end')) return 'Final'
  if (period > 0 && clock) return `Q${period} ${clock}`
  if (period > 0) return `Q${period}`
  return status
}

export function formatWinProb(prob: number): string {
  return `${Math.round(prob * 100)}%`
}

export function getConfidenceColor(confidence: string): string {
  switch (confidence) {
    case 'HIGH':   return 'text-green-400 bg-green-900/40'
    case 'MEDIUM': return 'text-yellow-400 bg-yellow-900/40'
    default:       return 'text-slate-400 bg-slate-800/40'
  }
}

export function formatStat(val: number | null | undefined, decimals = 1): string {
  if (val == null) return '—'
  return Number(val).toFixed(decimals)
}

export function getLogoUrl(teamId: number): string {
  return `https://cdn.nba.com/logos/nba/${teamId}/global/L/logo.svg`
}

export function getHeadshotUrl(playerId: number): string {
  return `https://cdn.nba.com/headshots/nba/latest/1040x760/${playerId}.png`
}

export function formatDate(dateStr: string): string {
  if (!dateStr) return ''
  const today = new Date().toISOString().slice(0, 10)
  if (dateStr === today) return 'Today'
  const d = new Date(dateStr + 'T00:00:00')
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
}

export function isGameLive(status: string): boolean {
  if (!status) return false
  const s = status.toLowerCase()
  return (
    s.includes('q1') || s.includes('q2') || s.includes('q3') || s.includes('q4') ||
    s.includes('halftime') || s.includes('half') || s.includes('ot') ||
    /^\d+:\d+/.test(s) ||
    (s.includes('in progress') || s.includes('live'))
  )
}

export function isGameFinal(status: string): boolean {
  if (!status) return false
  const s = status.toLowerCase()
  return s.includes('final') || s.includes('end') || s === 'f'
}

export function formatRecord(wins: number, losses: number): string {
  return `${wins}-${losses}`
}

export function pctColor(val: number): string {
  if (val >= 0.6) return 'text-green-400'
  if (val >= 0.5) return 'text-metric-text'
  if (val >= 0.4) return 'text-yellow-400'
  return 'text-red-400'
}
