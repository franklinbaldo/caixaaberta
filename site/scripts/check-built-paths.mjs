import { readdir, readFile, stat } from "node:fs/promises";
import { join } from "node:path";

const dist = new URL("../dist/", import.meta.url);
const badPath = "/caixaabertadata/";
const expectedPath = "/caixaaberta/data/latest.json";

async function files(dir) {
  const paths = [];
  for (const name of await readdir(dir)) {
    const path = join(dir, name);
    const info = await stat(path);
    if (info.isDirectory()) paths.push(...(await files(path)));
    else paths.push(path);
  }
  return paths;
}

const emitted = await files(dist);
const textFiles = emitted.filter((path) => /\.(?:html|js|css)$/.test(path));
const contents = await Promise.all(textFiles.map((path) => readFile(path, "utf8")));
const joined = contents.join("\n");

if (joined.includes(badPath)) {
  throw new Error(`Artefato compilado contém caminho inválido: ${badPath}`);
}

if (!joined.includes(expectedPath)) {
  throw new Error(`Artefato compilado não referencia o manifesto em ${expectedPath}`);
}

for (const relative of ["data/latest.json", "data/snapshot.parquet"]) {
  await stat(new URL(`../dist/${relative}`, import.meta.url));
}

console.log("Caminhos publicados e mirror de dados: OK.");
