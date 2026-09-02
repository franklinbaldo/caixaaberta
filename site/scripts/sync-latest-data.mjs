import { mkdir, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";

const manifestUrl = "https://archive.org/download/imoveis-caixa-economica-federal/latest.json";
const outputDir = join("public", "data");
const decoder = new TextDecoder();

function assertParquet(bytes, label) {
  if (bytes.byteLength < 4 || decoder.decode(bytes.slice(0, 4)) !== "PAR1") {
    throw new Error(`${label} não é Parquet`);
  }
}

const manifestResponse = await fetch(manifestUrl, { redirect: "follow" });
if (!manifestResponse.ok) throw new Error(`latest.json respondeu ${manifestResponse.status}`);
const manifest = await manifestResponse.json();
if (!manifest.data || !manifest.item || !manifest.parquet_url) throw new Error("latest.json inválido");

const parquetResponse = await fetch(manifest.parquet_url, { redirect: "follow" });
if (!parquetResponse.ok) throw new Error(`Parquet respondeu ${parquetResponse.status}`);
const parquet = new Uint8Array(await parquetResponse.arrayBuffer());
assertParquet(parquet, "snapshot");

await mkdir(outputDir, { recursive: true });
await writeFile(join(outputDir, "latest.json"), `${JSON.stringify(manifest, null, 2)}\n`);
await writeFile(join(outputDir, "snapshot.parquet"), parquet);

const changesPath = join(outputDir, "changes.parquet");
if (manifest.mudancas_url) {
  const changesResponse = await fetch(manifest.mudancas_url, { redirect: "follow" });
  if (!changesResponse.ok) throw new Error(`Mudanças responderam ${changesResponse.status}`);
  const changes = new Uint8Array(await changesResponse.arrayBuffer());
  assertParquet(changes, "derivado de mudanças");
  await writeFile(changesPath, changes);
  console.log(
    `Snapshot ${manifest.data} e mudanças desde ${manifest.mudancas_desde ?? "o retrato anterior"} ` +
      `espelhados no site (${parquet.byteLength} + ${changes.byteLength} bytes).`,
  );
} else {
  // Builds locais/sucessivos não podem reaproveitar um derivado de outro dia.
  await rm(changesPath, { force: true });
  console.log(`Snapshot ${manifest.data} espelhado no site (${parquet.byteLength} bytes); histórico ainda indisponível.`);
}
