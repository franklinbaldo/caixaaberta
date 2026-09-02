const manifestUrl = "https://archive.org/download/imoveis-caixa-economica-federal/latest.json";

function requireCors(response, label) {
  const allowOrigin = response.headers.get("access-control-allow-origin");
  if (allowOrigin !== "*") {
    throw new Error(`${label}: CORS inesperado (${allowOrigin ?? "ausente"})`);
  }
}

const manifestResponse = await fetch(manifestUrl, { redirect: "follow" });
if (!manifestResponse.ok) {
  throw new Error(`latest.json respondeu ${manifestResponse.status}`);
}
requireCors(manifestResponse, "latest.json");
const manifest = await manifestResponse.json();
if (!manifest.data || !manifest.item || !manifest.parquet_url) {
  throw new Error("latest.json não contém data, item e parquet_url");
}

const metadataResponse = await fetch(`https://archive.org/metadata/${manifest.item}`, {
  redirect: "follow",
});
if (!metadataResponse.ok) {
  throw new Error(`Metadata API respondeu ${metadataResponse.status}`);
}
requireCors(metadataResponse, "Metadata API");
const metadata = await metadataResponse.json();
if (!Array.isArray(metadata.files)) {
  throw new Error("Metadata API não retornou files[]");
}

const parquetResponse = await fetch(manifest.parquet_url, {
  redirect: "follow",
  headers: { Range: "bytes=0-0" },
});
if (!parquetResponse.ok) {
  throw new Error(`Parquet respondeu ${parquetResponse.status}`);
}
requireCors(parquetResponse, "Parquet");

console.log(`Fontes browser OK: snapshot ${manifest.data}, ${metadata.files.length} arquivos no item.`);
