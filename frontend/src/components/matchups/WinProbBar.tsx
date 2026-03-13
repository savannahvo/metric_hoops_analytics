'use client'

interface WinProbBarProps {
  homeProb: number
  homeName: string
  awayName: string
  homeColor?: string
  awayColor?: string
}

export function WinProbBar({
  homeProb,
  homeName,
  awayName,
  homeColor = '#1d428a',
  awayColor = '#c8102e',
}: WinProbBarProps) {
  const homeW = Math.round(homeProb * 100)
  const awayW = 100 - homeW

  return (
    <div className="w-full space-y-1">
      <div className="flex justify-between text-xs text-metric-muted">
        <span>{awayName}</span>
        <span>Win Probability</span>
        <span>{homeName}</span>
      </div>
      <div className="flex h-2.5 rounded-full overflow-hidden bg-metric-border">
        <div
          style={{ width: `${awayW}%`, backgroundColor: awayColor }}
          className="transition-all duration-500"
        />
        <div
          style={{ width: `${homeW}%`, backgroundColor: homeColor }}
          className="transition-all duration-500"
        />
      </div>
      <div className="flex justify-between text-xs font-bold">
        <span style={{ color: awayColor }}>{awayW}%</span>
        <span style={{ color: homeColor }}>{homeW}%</span>
      </div>
    </div>
  )
}
