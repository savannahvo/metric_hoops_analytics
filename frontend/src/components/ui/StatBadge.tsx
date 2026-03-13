type Color = 'default' | 'green' | 'red' | 'yellow' | 'blue'

const COLOR_MAP: Record<Color, string> = {
  default: 'bg-metric-border text-metric-text',
  green:   'bg-green-900/40 text-green-400',
  red:     'bg-red-900/40 text-red-400',
  yellow:  'bg-yellow-900/40 text-yellow-400',
  blue:    'bg-blue-900/40 text-blue-400',
}

export function StatBadge({
  label,
  value,
  color = 'default',
}: {
  label: string
  value: string | number
  color?: Color
}) {
  return (
    <div className={`inline-flex items-center gap-1 px-2 py-1 rounded text-xs font-medium ${COLOR_MAP[color]}`}>
      <span className="text-metric-muted">{label}</span>
      <span>{value}</span>
    </div>
  )
}

export function StatusBadge({ status }: { status: string }) {
  const s = status.toLowerCase()
  let color: Color = 'default'
  if (s.includes('out')) color = 'red'
  else if (s.includes('doubtful')) color = 'red'
  else if (s.includes('questionable')) color = 'yellow'
  else if (s.includes('day-to-day') || s.includes('probable')) color = 'blue'

  return (
    <span className={`inline-flex px-2 py-0.5 rounded text-xs font-semibold ${COLOR_MAP[color]}`}>
      {status}
    </span>
  )
}
