'use client'
import Image from 'next/image'
import type { Game, Prediction } from '@/lib/types'
import { WinProbBar } from './WinProbBar'
import { LiveBadge, FinalBadge } from './LiveBadge'
import { isGameLive, isGameFinal } from '@/lib/utils'

interface MatchupCardProps {
  game: Game
  prediction?: Prediction
}

export function MatchupCard({ game, prediction }: MatchupCardProps) {
  const live  = isGameLive(game.status)
  const final = isGameFinal(game.status)
  const hasScore = game.home_score > 0 || game.away_score > 0 || final

  return (
    <div className="card p-4 hover:border-metric-accent/40 transition-colors cursor-pointer">
      {/* Status row */}
      <div className="flex items-center justify-between mb-4">
        <div>
          {live  ? <LiveBadge />  :
           final ? <FinalBadge /> :
           <span className="text-xs text-metric-muted">{game.status}</span>}
        </div>
        {live && game.clock && (
          <span className="text-xs text-metric-muted">
            Q{game.period} · {game.clock}
          </span>
        )}
        {game.arena && !live && !final && (
          <span className="text-xs text-metric-muted truncate max-w-[120px]">{game.arena}</span>
        )}
      </div>

      {/* Teams */}
      <div className="flex items-center gap-3">
        {/* Away */}
        <div className="flex-1 flex items-center gap-3">
          <div className="w-10 h-10 relative shrink-0">
            <Image
              src={`https://cdn.nba.com/logos/nba/${game.away_team_id}/global/L/logo.svg`}
              alt={game.away_tricode}
              fill
              className="object-contain"
              onError={(e) => { (e.target as HTMLImageElement).style.display = 'none' }}
            />
          </div>
          <div className="min-w-0">
            <div className="font-bold text-sm">{game.away_tricode}</div>
            <div className="text-xs text-metric-muted truncate">{game.away_team}</div>
          </div>
          {hasScore && (
            <span className={`text-2xl font-black ml-auto tabular-nums ${
              final && game.away_score > game.home_score ? 'text-white' : 'text-metric-text'
            }`}>
              {game.away_score}
            </span>
          )}
        </div>

        <div className="text-metric-muted text-xs font-medium px-1">@</div>

        {/* Home */}
        <div className="flex-1 flex items-center gap-3 flex-row-reverse">
          <div className="w-10 h-10 relative shrink-0">
            <Image
              src={`https://cdn.nba.com/logos/nba/${game.home_team_id}/global/L/logo.svg`}
              alt={game.home_tricode}
              fill
              className="object-contain"
              onError={(e) => { (e.target as HTMLImageElement).style.display = 'none' }}
            />
          </div>
          <div className="min-w-0 text-right">
            <div className="font-bold text-sm">{game.home_tricode}</div>
            <div className="text-xs text-metric-muted truncate">{game.home_team}</div>
          </div>
          {hasScore && (
            <span className={`text-2xl font-black mr-auto tabular-nums ${
              final && game.home_score > game.away_score ? 'text-white' : 'text-metric-text'
            }`}>
              {game.home_score}
            </span>
          )}
        </div>
      </div>

      {/* Prediction section */}
      {prediction && (
        <div className="mt-4 pt-4 border-t border-metric-border space-y-3">
          <WinProbBar
            homeProb={prediction.home_win_prob}
            homeName={game.home_tricode}
            awayName={game.away_tricode}
          />

          {prediction.predicted_home_score > 0 && (
            <p className="text-center text-xs text-metric-muted">
              Projected:{' '}
              <span className="text-metric-text font-medium">
                {game.away_tricode} {prediction.predicted_away_score} — {game.home_tricode} {prediction.predicted_home_score}
              </span>
            </p>
          )}

          <div className="flex items-center justify-between">
            <span className={`text-xs px-2 py-0.5 rounded font-semibold ${
              prediction.confidence === 'HIGH'   ? 'bg-green-900/40 text-green-400' :
              prediction.confidence === 'MEDIUM' ? 'bg-yellow-900/40 text-yellow-400' :
              'bg-metric-border text-metric-muted'
            }`}>
              {prediction.confidence}
            </span>
            <span className="text-xs text-metric-muted">
              {prediction.predicted_winner} favored
            </span>
          </div>
        </div>
      )}

      {/* Settled result */}
      {prediction?.correct != null && (
        <div className={`mt-2 text-center text-xs font-bold ${
          prediction.correct ? 'text-green-400' : 'text-red-400'
        }`}>
          {prediction.correct ? '✓ Correct' : '✗ Incorrect'}
        </div>
      )}
    </div>
  )
}
