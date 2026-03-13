export function LiveBadge() {
  return (
    <div className="flex items-center gap-1.5 text-xs font-bold text-red-500">
      <span className="w-2 h-2 rounded-full bg-red-500 live-dot" />
      LIVE
    </div>
  )
}

export function FinalBadge() {
  return <span className="text-xs font-semibold text-metric-muted tracking-wide">FINAL</span>
}
