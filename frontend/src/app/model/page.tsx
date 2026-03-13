'use client'
import useSWR from 'swr'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell,
} from 'recharts'
import { api } from '@/lib/api'
import { LoadingSkeleton } from '@/components/ui/LoadingSkeleton'
import type { ModelFeature } from '@/lib/types'

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="card p-6">
      <h2 className="text-lg font-bold mb-4 text-white">{title}</h2>
      {children}
    </div>
  )
}

export default function ModelPage() {
  const { data: featData, isLoading } = useSWR('model/features', () => api.model.features())
  const { data: accuracy }            = useSWR('accuracy', () => api.predictions.accuracy())

  const features: ModelFeature[] = featData?.features ?? []
  const chartData = features
    .filter(f => f.shap_importance > 0)
    .sort((a, b) => b.shap_importance - a.shap_importance)
    .slice(0, 15)
    .map(f => ({ name: f.label || f.feature, value: +(f.shap_importance * 1000).toFixed(2) }))

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="relative card overflow-hidden">
        <div
          className="absolute inset-0 bg-cover bg-center opacity-10"
          style={{ backgroundImage: "url('/metric_hoops_background.png')" }}
        />
        <div className="relative p-8">
          <h1 className="text-3xl font-black text-white mb-2">About the Model</h1>
          <p className="text-metric-muted max-w-2xl">
            Metric Hoops uses a stacked ensemble (v2.0) to predict NBA game outcomes.
            Four base models feed into a meta-learner trained with time-blocked out-of-fold cross-validation across 5 seasons.
          </p>
          {accuracy && (
            <div className="flex gap-8 mt-6 text-sm">
              {[
                { label: 'Season Accuracy', val: `${(accuracy.season_accuracy * 100).toFixed(1)}%`, color: 'text-metric-accent' },
                { label: '7-Day Accuracy',  val: `${(accuracy.rolling_7d * 100).toFixed(1)}%`,     color: 'text-green-400' },
                { label: '30-Day Accuracy', val: `${(accuracy.rolling_30d * 100).toFixed(1)}%`,    color: 'text-yellow-400' },
                { label: 'Total Predicted', val: accuracy.total_games,                              color: 'text-metric-text' },
              ].map(s => (
                <div key={s.label}>
                  <div className={`text-2xl font-black ${s.color}`}>{s.val}</div>
                  <div className="text-xs text-metric-muted">{s.label}</div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* SHAP chart */}
      <Section title="Feature Importance (SHAP)">
        <p className="text-metric-muted text-sm mb-4">
          Normalised XGBoost gain importance for each feature. Higher = more influential in the ensemble.
        </p>
        {isLoading ? (
          <LoadingSkeleton className="h-64 w-full rounded-lg" />
        ) : chartData.length === 0 ? (
          <p className="text-metric-muted text-sm">No feature data yet — run ml/train_classifier.py first</p>
        ) : (
          <ResponsiveContainer width="100%" height={Math.max(300, chartData.length * 28)}>
            <BarChart data={chartData} layout="vertical" margin={{ left: 150, right: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2a2f3f" horizontal={false} />
              <XAxis type="number" tick={{ fill: '#64748b', fontSize: 11 }} />
              <YAxis type="category" dataKey="name" tick={{ fill: '#e2e8f0', fontSize: 11 }} width={150} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1a1f2e', border: '1px solid #2a2f3f', borderRadius: 8 }}
                formatter={(v: number) => [v.toFixed(3), 'Mean |SHAP|']}
              />
              <Bar dataKey="value" radius={[0, 4, 4, 0]}>
                {chartData.map((_, i) => (
                  <Cell key={i} fill={`hsl(${220 - i * 10}, 70%, ${65 - i * 2}%)`} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        )}
      </Section>

      {/* Feature glossary */}
      <Section title="Feature Glossary">
        {isLoading ? (
          <div className="space-y-3">
            {Array.from({ length: 6 }).map((_, i) => <LoadingSkeleton key={i} className="h-16 rounded-lg" />)}
          </div>
        ) : features.length === 0 ? (
          <p className="text-metric-muted text-sm">Train the model to populate the feature glossary.</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-xs text-metric-muted border-b border-metric-border">
                  <th className="text-left px-3 py-2 w-48">Feature</th>
                  <th className="text-left px-3 py-2">Description</th>
                  <th className="text-left px-3 py-2 hidden md:table-cell">Why It Matters</th>
                  <th className="text-right px-3 py-2 w-20">Importance</th>
                </tr>
              </thead>
              <tbody>
                {features.map(f => (
                  <tr key={f.feature} className="border-b border-metric-border/40 hover:bg-metric-border/20 align-top">
                    <td className="px-3 py-3">
                      <div className="font-mono text-xs text-metric-accent">{f.feature}</div>
                      <div className="text-xs text-metric-text mt-0.5">{f.label}</div>
                    </td>
                    <td className="px-3 py-3 text-xs text-metric-muted leading-relaxed">{f.description}</td>
                    <td className="px-3 py-3 text-xs text-metric-muted leading-relaxed hidden md:table-cell">{f.why}</td>
                    <td className="px-3 py-3 text-right text-xs font-mono text-metric-accent">
                      {f.shap_importance != null ? f.shap_importance.toFixed(4) : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Section>

      {/* Methodology */}
      <Section title="Methodology">
        <div className="space-y-4 text-sm text-metric-muted leading-relaxed">
          <div>
            <h3 className="text-white font-semibold mb-2">Model Architecture</h3>
            <p>
              A stacked ensemble (v2.0) combining four base learners — Logistic Regression, Random Forest,
              XGBoost, and MLP — whose out-of-fold predictions are fed into a Logistic Regression
              meta-learner. This approach extracts diverse signal from each model while preventing
              overfitting via time-blocked cross-validation.
            </p>
          </div>
          <div>
            <h3 className="text-white font-semibold mb-2">Training Approach</h3>
            <p>
              Trained on 5 seasons of NBA data (2020-21 through 2024-25) using strict time-ordered splits:
              train 2020-23, validate 2023-24, holdout 2024-25. No data shuffling anywhere — every fold
              trains on the past only. Features are computed as{' '}
              <em className="text-metric-text">home minus away differentials</em>, so positive values
              always favor the home team. Odds features (spread, moneyline probability) are sourced
              from real closing lines and merged before training.
            </p>
          </div>
          <div>
            <h3 className="text-white font-semibold mb-2">Prediction Locking</h3>
            <p>
              Predictions are locked automatically 25 minutes before each game&apos;s tip-off by the
              APScheduler. At lock time, the model fetches live odds from the NBA CDN and the latest
              injury status from our database — ensuring the prediction reflects the most current
              information available.
            </p>
          </div>
          <div>
            <h3 className="text-white font-semibold mb-2">Score Prediction</h3>
            <p>
              A separate XGBoost regressor predicts the point differential (home − away). This is
              converted to projected scores using the league average points per game:
              <span className="font-mono text-metric-text"> home_score = 113.5 + diff/2</span>.
            </p>
          </div>
        </div>
      </Section>

      {/* Data sources */}
      <Section title="Data Sources">
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 text-sm">
          {[
            {
              name: 'Kaggle — NBA Traditional Stats',
              desc: 'Game results, team box scores, and player statistics (szymonjwiak/nba-traditional). Downloaded fresh daily.',
              badge: 'Training',
            },
            {
              name: 'SBR (SportsbookReview)',
              desc: 'Historical closing spreads and moneylines. Used for SPREAD_DIFF and ML_PROB_DIFF features.',
              badge: 'Odds',
            },
            {
              name: 'NBA CDN',
              desc: 'Live scores, play-by-play, today\'s odds, playoff bracket, and season schedule. No rate limits.',
              badge: 'Live',
            },
            {
              name: 'ESPN + CBS Sports',
              desc: 'Daily injury report snapshots. Scraped every morning at 9 AM ET.',
              badge: 'Injuries',
            },
          ].map(src => (
            <div key={src.name} className="bg-metric-border/20 rounded-xl p-4">
              <div className="flex items-center gap-2 mb-1">
                <span className="font-semibold text-white">{src.name}</span>
                <span className="text-xs px-2 py-0.5 bg-metric-accent/20 text-metric-accent rounded">
                  {src.badge}
                </span>
              </div>
              <p className="text-metric-muted text-xs">{src.desc}</p>
            </div>
          ))}
        </div>
      </Section>
    </div>
  )
}
