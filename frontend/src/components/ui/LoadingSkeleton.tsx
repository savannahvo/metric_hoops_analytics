export function LoadingSkeleton({ className = '' }: { className?: string }) {
  return <div className={`animate-pulse bg-metric-border rounded ${className}`} />
}

export function CardSkeleton() {
  return (
    <div className="card p-4 space-y-3">
      <LoadingSkeleton className="h-5 w-3/4" />
      <LoadingSkeleton className="h-4 w-1/2" />
      <LoadingSkeleton className="h-8 w-full" />
      <LoadingSkeleton className="h-3 w-full" />
    </div>
  )
}

export function TableRowSkeleton({ cols = 6 }: { cols?: number }) {
  return (
    <tr>
      {Array.from({ length: cols }).map((_, i) => (
        <td key={i} className="px-3 py-3">
          <LoadingSkeleton className="h-4 w-full" />
        </td>
      ))}
    </tr>
  )
}
