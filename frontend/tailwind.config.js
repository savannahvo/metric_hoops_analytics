/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    './src/pages/**/*.{js,ts,jsx,tsx,mdx}',
    './src/components/**/*.{js,ts,jsx,tsx,mdx}',
    './src/app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      colors: {
        'metric-dark':   '#0f1117',
        'metric-card':   '#1a1f2e',
        'metric-border': '#2a2f3f',
        'metric-text':   '#e2e8f0',
        'metric-muted':  '#64748b',
        'metric-accent': '#3b82f6',
        'metric-green':  '#22c55e',
        'metric-red':    '#ef4444',
        'metric-yellow': '#eab308',
      },
    },
  },
  plugins: [],
}
