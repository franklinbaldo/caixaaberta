import * as duckdb from "@duckdb/duckdb-wasm";
import "../styles/daily-changes.css";

type Manifest = {
  data: string;
  item: string;
  parquet_url: string;
  mudancas_url?: string;
  mudancas_desde?: string;
};

type RawRow = Record<string, unknown>;

const integer = new Intl.NumberFormat("pt-BR");

function localDataUrl(file: string) {
  const basePath = import.meta.env.BASE_URL.replace(/\/?$/, "/");
  return new URL(`${basePath}data/${file}`, window.location.origin).href;
}

function required<T extends Element>(selector: string): T {
  const element = document.querySelector<T>(selector);
  if (!element) throw new Error(`Elemento obrigatório ausente: ${selector}`);
  return element;
}

async function aggregateChanges(bytes: Uint8Array) {
  const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
  if (!bundle.mainWorker) throw new Error("DuckDB não encontrou worker compatível");
  const workerUrl = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], { type: "text/javascript" }),
  );
  const worker = new Worker(workerUrl);
  const db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), worker);

  try {
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    URL.revokeObjectURL(workerUrl);
    await db.registerFileBuffer("changes.parquet", bytes);
    const connection = await db.connect();
    try {
      const table = await connection.query(`
        WITH stats AS (
          SELECT
            count(*) FILTER (WHERE mudanca = 'entrou_no_estoque') AS entered,
            count(*) FILTER (WHERE mudanca = 'saiu_do_estoque') AS exited,
            count(*) FILTER (WHERE mudanca = 'alterou') AS altered,
            count(*) FILTER (
              WHERE mudanca = 'alterou'
                AND campos_alterados LIKE '%preco%'
                AND preco_anterior IS NOT NULL
                AND preco_atual IS NOT NULL
                AND preco_atual < preco_anterior
            ) AS price_drops
          FROM 'changes.parquet'
        )
        SELECT
          entered,
          exited,
          price_drops,
          greatest(altered - price_drops, 0) AS other_altered
        FROM stats
      `);
      const row = table.toArray()[0]?.toJSON() as RawRow | undefined;
      if (!row) throw new Error("Derivado de mudanças não produziu resumo");
      return {
        entered: Number(row.entered ?? 0),
        exited: Number(row.exited ?? 0),
        priceDrops: Number(row.price_drops ?? 0),
        otherAltered: Number(row.other_altered ?? 0),
      };
    } finally {
      await connection.close();
    }
  } finally {
    URL.revokeObjectURL(workerUrl);
    await db.terminate();
    worker.terminate();
  }
}

export async function initDailyChanges() {
  const panel = document.querySelector<HTMLElement>("[data-daily-changes]");
  if (!panel || panel.dataset.initialized === "true") return;
  panel.dataset.initialized = "true";

  const period = required<HTMLElement>("#daily-changes-period");
  const note = required<HTMLElement>("#daily-changes-note");
  const entered = required<HTMLElement>("#daily-entered");
  const priceDrops = required<HTMLElement>("#daily-price-drops");
  const altered = required<HTMLElement>("#daily-altered");
  const exited = required<HTMLElement>("#daily-exited");

  try {
    const manifestResponse = await fetch(localDataUrl("latest.json"), { cache: "no-store" });
    if (!manifestResponse.ok) throw new Error(`manifesto respondeu ${manifestResponse.status}`);
    const manifest = (await manifestResponse.json()) as Manifest;

    if (!manifest.mudancas_url) {
      panel.dataset.state = "unavailable";
      period.textContent = `Retrato atual: ${manifest.data}`;
      note.textContent =
        "Ainda não há comparação publicada para este retrato. A busca atual continua disponível normalmente.";
      return;
    }

    period.textContent = manifest.mudancas_desde
      ? `${manifest.mudancas_desde} → ${manifest.data}`
      : `Até ${manifest.data}`;
    note.textContent = "Lendo as mudanças publicadas…";

    const changesResponse = await fetch(localDataUrl("changes.parquet"), { cache: "no-store" });
    if (!changesResponse.ok) throw new Error(`mudanças responderam ${changesResponse.status}`);
    const changes = new Uint8Array(await changesResponse.arrayBuffer());
    const summary = await aggregateChanges(changes);

    entered.textContent = integer.format(summary.entered);
    priceDrops.textContent = integer.format(summary.priceDrops);
    altered.textContent = integer.format(summary.otherAltered);
    exited.textContent = integer.format(summary.exited);
    note.textContent =
      "Eventos são diferenças observadas entre snapshots. “Saiu” significa apenas ausência no retrato atual; não inferimos a causa.";
    panel.dataset.state = "ready";
  } catch (error) {
    panel.dataset.state = "error";
    period.textContent = "Comparação indisponível";
    note.textContent =
      "Não foi possível ler a memória temporal agora. O retrato corrente e os dados preservados continuam disponíveis.";
    console.error("Falha ao inicializar Hoje no radar", error);
  }
}
