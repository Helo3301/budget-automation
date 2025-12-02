/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./web/static/**/*.{html,js}",
    "./web/templates/**/*.html"
  ],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        gray: {
          750: '#2D374D',
          850: '#1A2234',
        }
      }
    },
  },
  plugins: [],
}
