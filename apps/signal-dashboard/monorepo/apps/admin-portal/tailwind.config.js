/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    './pages/**/*.{js,ts,jsx,tsx,mdx}',
    './components/**/*.{js,ts,jsx,tsx,mdx}',
    './app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      colors: {
        'dfi-primary': '#1a1a1a',
        'dfi-secondary': '#f8f9fa',
        'dfi-accent': '#6366f1', // Blue-purple gradient start
        'dfi-accent-end': '#8b5cf6', // Blue-purple gradient end
        'dfi-gold': '#ffd700',
        'dfi-dark': '#ffffff',
        'dfi-light': '#ffffff',
        'dfi-gray': '#6b7280',
        'dfi-border': '#e5e7eb',
        'dfi-text': '#1f2937',
        'dfi-text-light': '#6b7280',
      },
      fontFamily: {
        'sans': ['Inter', 'system-ui', 'sans-serif'],
      },
      animation: {
        'fade-in': 'fadeIn 0.5s ease-in-out',
        'slide-up': 'slideUp 0.3s ease-out',
      },
      keyframes: {
        fadeIn: {
          '0%': { opacity: '0' },
          '100%': { opacity: '1' },
        },
        slideUp: {
          '0%': { transform: 'translateY(10px)', opacity: '0' },
          '100%': { transform: 'translateY(0)', opacity: '1' },
        },
      },
    },
  },
  plugins: [],
}
