/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./app/**/*.{js,ts,jsx,tsx}",
    "./components/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        pegasus: {
          bg: "#080c14",
          violet: "#7c3aed",
          cyan: "#06b6d4",
        },
      },
    },
  },
  plugins: [],
};

