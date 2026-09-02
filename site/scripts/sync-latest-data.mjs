import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";

const manifestUrl = "https://archive.org/download/imoveis-caixa-economica-federal/latest.json";
const outputDir = join("public", "data");

const manifestResponse = await fetch(manifestUrl, { redirect: "follow" });
if (!manifestResponse.ok) {
  throw new Error(`latest.json respondeu ${manifestResponse.status}`);
}
const manifest = await manifestResponse.json();
if (!manifest.data || !manifest.item || !manifest.parquet_url) {
  throw new Error("latest.json não contém data, item e parquet_url");
}

const parquetResponse = await fetch(manifest.parquet_url, { redirect: "follow" });
if (!parquetResponse.ok) {
  throw new Error(`Parquet respondeu ${parquetResponse.status}`);
}
const parquet = new Uint8Array(await parquetResponse.arrayBuffer());
if (parquet.byteLength < 4 || new TextDecoder().decode(parquet.slice(0, 4)) !== "PAR1") {
  throw new Error("o artefato baixado não parece ser Parquet");
}

await mkdir(outputDir, { recursive: true });
await writeFile(join(outputDir, "latest.json"), `${JSON.stringify(manifest, null, 2)}\n`);
await writeFile(join(outputDir, "snapshot.parquet"), parquet);

console.log(`Snapshot ${manifest.data} espelhado no site (${parquet.byteLength} bytes).`);
