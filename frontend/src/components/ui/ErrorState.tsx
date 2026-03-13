export function ErrorState({
  message = 'Failed to load data',
  retry,
}: {
  message?: string
  retry?: () => void
}) {
  return (
    <div className="card p-8 text-center">
      <p className="text-metric-red mb-3">⚠ {message}</p>
      {retry && (
        <button
          onClick={retry}
          className="text-sm text-metric-accent hover:underline"
        >
          Try again
        </button>
      )}
    </div>
  )
}
