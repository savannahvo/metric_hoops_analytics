import type {
  Game, Prediction, TeamStanding, Player, PlayerGameLog,
  Injury, ModelFeature, AccuracyStats, DriftLog,
} from './types'

const API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'

async function apiFetch<T>(path: string): Promise<T> {
  const res = await fetch(`${API}${path}`, { cache: 'no-store' })
  if (!res.ok) throw new Error(`API ${res.status}: ${path}`)
  return res.json() as Promise<T>
}

export const api = {
  games: {
    today:      () => apiFetch<{ games: Game[] }>('/api/games/today'),
    byDate:     (date: string) => apiFetch<{ games: Game[] }>(`/api/games/date/${date}`),
    boxscore:   (id: string) => apiFetch<Record<string, unknown>>(`/api/games/${id}/boxscore`),
    playbyplay: (id: string) => apiFetch<Record<string, unknown>>(`/api/games/${id}/playbyplay`),
    schedule:   () => apiFetch<Record<string, unknown>>('/api/games/schedule'),
  },
  standings: {
    get: () => apiFetch<{ east: TeamStanding[]; west: TeamStanding[] }>('/api/standings'),
  },
  players: {
    stats:    () => apiFetch<{ players: Player[] }>('/api/players/stats'),
    gamelogs: (id: number) => apiFetch<{ gamelogs: PlayerGameLog[] }>(`/api/players/${id}/gamelogs`),
    info:     (id: number) => apiFetch<Record<string, unknown>>(`/api/players/${id}/info`),
  },
  injuries: {
    get: (team?: string, status?: string) => {
      const p = new URLSearchParams()
      if (team)   p.set('team_name', team)
      if (status) p.set('status', status)
      const qs = p.toString()
      return apiFetch<{ injuries: Injury[]; snapshot_date: string }>(`/api/injuries${qs ? '?' + qs : ''}`)
    },
  },
  predictions: {
    today:    () => apiFetch<{ predictions: Prediction[] }>('/api/predictions/today'),
    log:      (page = 1, size = 20) => apiFetch<{ predictions: Prediction[]; total: number }>(`/api/predictions/log?page=${page}&size=${size}`),
    accuracy: () => apiFetch<AccuracyStats>('/api/predictions/accuracy'),
    drift:    () => apiFetch<{ drift: DriftLog[] }>('/api/predictions/drift'),
  },
  playoffs: {
    bracket: () => apiFetch<Record<string, unknown>>('/api/playoffs'),
  },
  transactions: {
    get: () => apiFetch<{ transactions: Record<string, unknown>[] }>('/api/transactions'),
  },
  model: {
    features: () => apiFetch<{ features: ModelFeature[] }>('/api/model/features'),
  },
}
