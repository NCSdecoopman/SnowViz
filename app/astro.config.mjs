import { defineConfig } from 'astro/config';

import tailwindcss from '@tailwindcss/vite';

export default defineConfig({
  site: 'https://NCSdecoopman.github.io',
  base: '/niveo',
  server: { port: 4321 },
  vite: {
    build: { sourcemap: false },
    plugins: [tailwindcss()],
  },
});