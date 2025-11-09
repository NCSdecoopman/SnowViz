import { defineConfig } from 'astro/config';
import tailwindcss from '@tailwindcss/vite';

export default defineConfig({
  site: 'https://ncsdecoopman.github.io',
  base: '/niveo/',               // <-- slash final
  server: { port: 4321 },
  vite: {
    build: { sourcemap: false },
    plugins: [tailwindcss()],
  },
});
