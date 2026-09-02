import { readdir, readFile } from "node:fs/promises";
import { extname, join } from "node:path";

const roots = ["src"];
const extensions = new Set([".astro", ".html", ".svelte"]);
const violations = [];

async function walk(path) {
  for (const entry of await readdir(path, { withFileTypes: true })) {
    const fullPath = join(path, entry.name);
    if (entry.isDirectory()) {
      await walk(fullPath);
      continue;
    }
    if (!extensions.has(extname(entry.name))) continue;

    const source = await readFile(fullPath, "utf8");
    if (/\bclass(?::list)?\s*=/.test(source)) violations.push(fullPath);
  }
}

for (const root of roots) await walk(root);

if (violations.length) {
  console.error("Cobogó semantic-only: não use class= no markup do site:");
  for (const file of violations) console.error(`- ${file}`);
  process.exit(1);
}

console.log("Cobogó semantic-only: markup sem class=.");
