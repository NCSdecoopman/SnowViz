// Copy ../data => app/public/data
import fs from 'node:fs';
import path from 'node:path';

const src = path.resolve(process.cwd(), '..', 'data');
const dst = path.resolve(process.cwd(), 'public', 'data');

function rmrf(p) { if (fs.existsSync(p)) fs.rmSync(p, { recursive: true, force: true }); }
function mkdirp(p) { fs.mkdirSync(p, { recursive: true }); }
function copyDir(s, d) {
  mkdirp(d);
  for (const e of fs.readdirSync(s, { withFileTypes: true })) {
    const sp = path.join(s, e.name);
    const dp = path.join(d, e.name);
    if (e.isDirectory()) copyDir(sp, dp);
    else fs.copyFileSync(sp, dp);
  }
}

if (!fs.existsSync(src)) {
  console.error(`Missing source dir: ${src}`);
  process.exit(1);
}
rmrf(dst);
copyDir(src, dst);
console.log(`Copied ${src} -> ${dst}`);
