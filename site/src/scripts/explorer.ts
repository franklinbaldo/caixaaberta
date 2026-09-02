import * as duckdb from "@duckdb/duckdb-wasm";
import {
  Map,
  NavigationControl,
  Popup,
  type GeoJSONSource,
  type StyleSpecification,
} from "maplibre-gl";

type Manifest = {
  data: string;
  item: string;
  parquet_url: string;
};

type Snapshot = {
  date: string;
  url: string;
};

type ArchiveMetadata = {
  files?: Array<{ name?: string }>;
};

type RawRow = Record<string, unknown>;

type Property = {
  link: string;
  link_acesso: string;
  endereco: string;
  bairro: string;
  cidade: string;
  estado: string;
  descricao: string;
  preco: number | null;
  avaliacao: number | null;
  desconto: number | null;
  financiamento: string;
  modalidade: string;
  latitude: number | null;
  longitude: number | null;
  precisao: string;
};

const MANIFEST_URL = "https://archive.org/download/imoveis-caixa-economica-federal/latest.json";
const ITEM_PREFIX = "imoveis-caixa-economica-federal";
const FIRST_SNAPSHOT_YEAR = 2026;
const SNAPSHOT_FILE = /^imoveis_geocoded_(\d{4}-\d{2}-\d{2})\.parquet$/;
const DATE_VALUE = /^\d{4}-\d{2}-\d{2}$/;
const currency = new Intl.NumberFormat("pt-BR", { style: "currency", currency: "BRL" });
const integer = new Intl.NumberFormat("pt-BR");

const text = (value: unknown) => (value == null ? "" : String(value));
const number = (value: unknown): number | null => {
  if (value == null || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

function normalize(row: RawRow): Property {
  return {
    link: text(row.link),
    link_acesso: text(row.link_acesso),
    endereco: text(row.endereco),
    bairro: text(row.bairro),
    cidade: text(row.cidade),
    estado: text(row.estado),
    descricao: text(row.descricao),
    preco: number(row.preco),
    avaliacao: number(row.avaliacao),
    desconto: number(row.desconto),
    financiamento: text(row.financiamento),
    modalidade: text(row.modalidade),
    latitude: number(row.latitude),
    longitude: number(row.longitude),
    precisao: text(row.precisao),
  };
}

function required<T extends Element>(selector: string): T {
  const element = document.querySelector<T>(selector);
  if (!element) throw new Error(`Elemento obrigatório ausente: ${selector}`);
  return element;
}

function snapshotUrl(date: string) {
  const year = date.slice(0, 4);
  return `https://archive.org/download/${ITEM_PREFIX}-${year}/imoveis_geocoded_${date}.parquet`;
}

async function discoverSnapshots(latest: Manifest): Promise<Snapshot[]> {
  const latestYear = Number(latest.data.slice(0, 4));
  const years = Array.from(
    { length: Math.max(1, latestYear - FIRST_SNAPSHOT_YEAR + 1) },
    (_, index) => latestYear - index,
  );

  const groups = await Promise.all(
    years.map(async (year) => {
      try {
        const item = `${ITEM_PREFIX}-${year}`;
        const response = await fetch(`https://archive.org/metadata/${item}`, {
          cache: "no-store",
        });
        if (!response.ok) return [] as Snapshot[];
        const metadata = (await response.json()) as ArchiveMetadata;
        return (metadata.files ?? []).flatMap((file) => {
          const match = SNAPSHOT_FILE.exec(file.name ?? "");
          if (!match) return [];
          const date = match[1];
          return [{ date, url: snapshotUrl(date) }];
        });
      } catch {
        return [] as Snapshot[];
      }
    }),
  );

  const byDate = new globalThis.Map<string, Snapshot>();
  byDate.set(latest.data, { date: latest.data, url: latest.parquet_url });
  for (const snapshot of groups.flat()) byDate.set(snapshot.date, snapshot);
  return [...byDate.values()].sort((a, b) => b.date.localeCompare(a.date));
}

function requestedSnapshot(latest: Manifest): Snapshot {
  const value = new URL(window.location.href).searchParams.get("data") ?? "";
  if (!DATE_VALUE.test(value)) {
    return { date: latest.data, url: latest.parquet_url };
  }
  return { date: value, url: snapshotUrl(value) };
}

async function fetchSnapshotBytes(snapshot: Snapshot, latest: Manifest) {
  let selected = snapshot;
  let response = await fetch(selected.url);
  if (!response.ok && selected.date !== latest.data) {
    selected = { date: latest.data, url: latest.parquet_url };
    response = await fetch(selected.url);
  }
  if (!response.ok) throw new Error(`Parquet respondeu ${response.status}`);
  return { selected, bytes: new Uint8Array(await response.arrayBuffer()) };
}

async function loadProperties(onStatus: (message: string) => void) {
  onStatus("Descobrindo os snapshots preservados…");
  const manifestResponse = await fetch(MANIFEST_URL, { cache: "no-store" });
  if (!manifestResponse.ok) throw new Error(`latest.json respondeu ${manifestResponse.status}`);
  const manifest = (await manifestResponse.json()) as Manifest;
  if (!manifest.parquet_url || !DATE_VALUE.test(manifest.data)) {
    throw new Error("latest.json não contém um snapshot válido");
  }

  const historyPromise = discoverSnapshots(manifest);
  const wanted = requestedSnapshot(manifest);
  onStatus(`Baixando o snapshot de ${wanted.date}…`);
  const { selected, bytes: parquet } = await fetchSnapshotBytes(wanted, manifest);

  onStatus("Abrindo o Parquet com DuckDB no navegador…");
  const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
  if (!bundle.mainWorker) throw new Error("DuckDB não encontrou um worker compatível");
  const workerUrl = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], { type: "text/javascript" }),
  );
  const worker = new Worker(workerUrl);
  const db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), worker);

  try {
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    URL.revokeObjectURL(workerUrl);
    await db.registerFileBuffer("snapshot.parquet", parquet);
    const connection = await db.connect();
    try {
      // SELECT * mantém compatibilidade entre snapshots: `link_acesso` só
      // existe nas observações produzidas depois de sua introdução no schema.
      const table = await connection.query("SELECT * FROM 'snapshot.parquet'");
      const rows = table.toArray().map((row) => row.toJSON() as RawRow).map(normalize);
      const snapshots = await historyPromise;
      return { manifest, selected, snapshots, rows };
    } finally {
      await connection.close();
    }
  } finally {
    URL.revokeObjectURL(workerUrl);
    await db.terminate();
    worker.terminate();
  }
}

function options(select: HTMLSelectElement, values: string[]) {
  for (const value of values.filter(Boolean).sort((a, b) => a.localeCompare(b, "pt-BR"))) {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = value;
    select.append(option);
  }
}

function snapshotOptions(
  select: HTMLSelectElement,
  snapshots: Snapshot[],
  selected: Snapshot,
  latest: Manifest,
) {
  const fragment = document.createDocumentFragment();
  for (const snapshot of snapshots) {
    const option = document.createElement("option");
    option.value = snapshot.date;
    option.textContent = snapshot.date === latest.data ? `${snapshot.date} — mais recente` : snapshot.date;
    option.selected = snapshot.date === selected.date;
    option.defaultSelected = option.selected;
    fragment.append(option);
  }
  if (!snapshots.some((snapshot) => snapshot.date === selected.date)) {
    const option = document.createElement("option");
    option.value = selected.date;
    option.textContent = selected.date;
    option.selected = true;
    option.defaultSelected = true;
    fragment.prepend(option);
  }
  select.replaceChildren(fragment);
}

function precisionLabel(value: string) {
  return (
    {
      logradouro_localidade: "logradouro + localidade",
      logradouro: "logradouro",
      localidade: "localidade",
      municipio: "município",
    }[value] ?? value ?? ""
  );
}

function safeSourceUrl(value: string) {
  if (!value) return "";
  try {
    const url = new URL(value);
    return url.protocol === "https:" || url.protocol === "http:" ? url.href : "";
  } catch {
    return "";
  }
}

function mapStyle(): StyleSpecification {
  return {
    version: 8,
    sources: {
      osm: {
        type: "raster",
        tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
        tileSize: 256,
        attribution: "© OpenStreetMap contributors",
      },
    },
    layers: [{ id: "osm", type: "raster", source: "osm" }],
  };
}

function setupMap() {
  const styles = getComputedStyle(document.documentElement);
  const azul = styles.getPropertyValue("--azul").trim() || "#155a72";
  const verde = styles.getPropertyValue("--verde").trim() || "#31735b";
  const ocre = styles.getPropertyValue("--ocre").trim() || "#ba7a2e";
  const concreto = styles.getPropertyValue("--concreto-600").trim() || "#74736d";

  const map = new Map({
    container: "map",
    style: mapStyle(),
    center: [-52.5, -15.5],
    zoom: 3.1,
    minZoom: 2.5,
    maxZoom: 17,
  });
  map.addControl(new NavigationControl({ showCompass: false }), "top-right");

  const ready = new Promise<void>((resolve) => {
    map.once("load", () => {
      map.addSource("imoveis", {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
        cluster: true,
        clusterRadius: 48,
        clusterMaxZoom: 12,
      });
      map.addLayer({
        id: "clusters",
        type: "circle",
        source: "imoveis",
        filter: ["has", "point_count"],
        paint: {
          "circle-color": azul,
          "circle-opacity": 0.88,
          "circle-radius": ["step", ["get", "point_count"], 17, 50, 22, 250, 29, 1000, 36],
          "circle-stroke-width": 2,
          "circle-stroke-color": "#ffffff",
        },
      });
      map.addLayer({
        id: "cluster-count",
        type: "symbol",
        source: "imoveis",
        filter: ["has", "point_count"],
        layout: {
          "text-field": ["get", "point_count_abbreviated"],
          "text-size": 12,
        },
        paint: { "text-color": "#ffffff" },
      });
      map.addLayer({
        id: "unclustered-point",
        type: "circle",
        source: "imoveis",
        filter: ["!", ["has", "point_count"]],
        paint: {
          "circle-color": [
            "match",
            ["get", "precisao"],
            "logradouro_localidade",
            verde,
            "logradouro",
            azul,
            "localidade",
            ocre,
            concreto,
          ],
          "circle-radius": 6,
          "circle-stroke-width": 1.5,
          "circle-stroke-color": "#ffffff",
        },
      });

      map.on("click", "clusters", async (event) => {
        const feature = map.queryRenderedFeatures(event.point, { layers: ["clusters"] })[0];
        if (!feature || feature.geometry.type !== "Point") return;
        const clusterId = Number(feature.properties?.cluster_id);
        const source = map.getSource("imoveis") as GeoJSONSource;
        const zoom = await source.getClusterExpansionZoom(clusterId);
        map.easeTo({ center: feature.geometry.coordinates as [number, number], zoom });
      });

      map.on("click", "unclustered-point", (event) => {
        const feature = event.features?.[0];
        if (!feature || feature.geometry.type !== "Point") return;
        const props = feature.properties ?? {};
        const content = document.createElement("article");
        const title = document.createElement("strong");
        title.textContent = `${props.cidade ?? ""} / ${props.estado ?? ""}`;
        const address = document.createElement("p");
        address.textContent = props.endereco ?? "Endereço não informado";
        const detail = document.createElement("small");
        detail.textContent = `Imóvel ${props.link ?? ""} · precisão: ${precisionLabel(props.precisao ?? "")}`;
        content.append(title, address, detail);
        const href = safeSourceUrl(String(props.link_acesso ?? ""));
        if (href) {
          const sourceLink = document.createElement("a");
          sourceLink.href = href;
          sourceLink.target = "_blank";
          sourceLink.rel = "noopener noreferrer";
          sourceLink.textContent = "Ver oferta na Caixa";
          content.append(document.createElement("br"), sourceLink);
        }
        new Popup({ closeButton: true })
          .setLngLat(feature.geometry.coordinates as [number, number])
          .setDOMContent(content)
          .addTo(map);
      });

      for (const layer of ["clusters", "unclustered-point"]) {
        map.on("mouseenter", layer, () => (map.getCanvas().style.cursor = "pointer"));
        map.on("mouseleave", layer, () => (map.getCanvas().style.cursor = ""));
      }
      resolve();
    });
  });

  return { map, ready };
}

function toGeoJSON(rows: Property[]) {
  return {
    type: "FeatureCollection" as const,
    features: rows
      .filter((row) => row.latitude != null && row.longitude != null)
      .map((row) => ({
        type: "Feature" as const,
        geometry: {
          type: "Point" as const,
          coordinates: [row.longitude as number, row.latitude as number],
        },
        properties: {
          link: row.link,
          link_acesso: row.link_acesso,
          endereco: row.endereco,
          cidade: row.cidade,
          estado: row.estado,
          precisao: row.precisao,
        },
      })),
  };
}

function appendTextCell(row: HTMLTableRowElement, value: string) {
  const cell = document.createElement("td");
  cell.textContent = value || "—";
  row.append(cell);
}

function renderTable(tbody: HTMLTableSectionElement, rows: Property[]) {
  const fragment = document.createDocumentFragment();
  for (const property of rows.slice(0, 100)) {
    const tr = document.createElement("tr");

    const propertyCell = document.createElement("td");
    const id = document.createElement("strong");
    const href = safeSourceUrl(property.link_acesso);
    if (href) {
      const sourceLink = document.createElement("a");
      sourceLink.href = href;
      sourceLink.target = "_blank";
      sourceLink.rel = "noopener noreferrer";
      sourceLink.textContent = property.link || "Abrir oferta";
      id.append(sourceLink);
    } else {
      id.textContent = property.link || "—";
    }
    const address = document.createElement("small");
    address.textContent = property.endereco || "Endereço não informado";
    propertyCell.append(id, document.createElement("br"), address);
    tr.append(propertyCell);

    appendTextCell(tr, [property.cidade, property.estado].filter(Boolean).join(" / "));
    appendTextCell(tr, property.preco == null ? "—" : currency.format(property.preco));
    appendTextCell(tr, property.desconto == null ? "—" : `${property.desconto.toLocaleString("pt-BR")} %`);
    appendTextCell(tr, property.modalidade);
    appendTextCell(tr, precisionLabel(property.precisao));
    fragment.append(tr);
  }
  tbody.replaceChildren(fragment);
}

export async function initExplorer() {
  const app = document.querySelector<HTMLElement>("[data-explorer-app]");
  if (!app || app.dataset.initialized === "true") return;
  app.dataset.initialized = "true";

  const form = required<HTMLFormElement>("#explorer-filters");
  const snapshotDate = required<HTMLSelectElement>("#filter-date");
  const query = required<HTMLInputElement>("#filter-query");
  const state = required<HTMLSelectElement>("#filter-state");
  const modality = required<HTMLSelectElement>("#filter-modality");
  const precision = required<HTMLSelectElement>("#filter-precision");
  const discount = required<HTMLInputElement>("#filter-discount");
  const count = required<HTMLElement>("#explorer-count");
  const date = required<HTMLElement>("#explorer-date");
  const status = required<HTMLElement>("#explorer-status");
  const tbody = required<HTMLTableSectionElement>("#explorer-rows");

  const setStatus = (message: string) => {
    status.textContent = message;
  };

  try {
    const { map, ready } = setupMap();
    const { manifest, selected, snapshots, rows } = await loadProperties(setStatus);
    snapshotOptions(snapshotDate, snapshots, selected, manifest);
    options(state, [...new Set(rows.map((row) => row.estado))]);
    options(modality, [...new Set(rows.map((row) => row.modalidade))]);
    date.textContent = `Snapshot ${selected.date}${selected.date === manifest.data ? " · mais recente" : ""}`;

    snapshotDate.addEventListener("change", () => {
      if (!DATE_VALUE.test(snapshotDate.value)) return;
      const url = new URL(window.location.href);
      if (snapshotDate.value === manifest.data) url.searchParams.delete("data");
      else url.searchParams.set("data", snapshotDate.value);
      url.hash = "explorar";
      window.location.assign(url);
    });

    const render = async () => {
      const needle = query.value.trim().toLocaleLowerCase("pt-BR");
      const minimumDiscount = discount.value === "" ? null : Number(discount.value);
      const filtered = rows.filter((property) => {
        if (state.value && property.estado !== state.value) return false;
        if (modality.value && property.modalidade !== modality.value) return false;
        if (precision.value && property.precisao !== precision.value) return false;
        if (minimumDiscount != null && (property.desconto ?? -Infinity) < minimumDiscount) return false;
        if (needle) {
          const haystack = [
            property.link,
            property.endereco,
            property.bairro,
            property.cidade,
            property.estado,
            property.descricao,
          ]
            .join(" ")
            .toLocaleLowerCase("pt-BR");
          if (!haystack.includes(needle)) return false;
        }
        return true;
      });

      count.textContent = `${integer.format(filtered.length)} imóvel${filtered.length === 1 ? "" : "is"}`;
      const geocoded = filtered.filter((property) => property.latitude != null && property.longitude != null).length;
      setStatus(`${integer.format(geocoded)} com coordenadas para o mapa; lista limitada a 100 linhas visíveis.`);
      renderTable(tbody, filtered);
      await ready;
      const source = map.getSource("imoveis") as GeoJSONSource;
      source.setData(toGeoJSON(filtered));
    };

    let timer = 0;
    const scheduleRender = () => {
      window.clearTimeout(timer);
      timer = window.setTimeout(() => void render(), 80);
    };
    form.addEventListener("input", scheduleRender);
    form.addEventListener("change", scheduleRender);
    form.addEventListener("reset", () => {
      snapshotDate.value = selected.date;
      window.setTimeout(() => void render(), 0);
    });

    app.dataset.state = "ready";
    await render();
  } catch (error) {
    app.dataset.state = "error";
    count.textContent = "Explorador indisponível";
    setStatus("Não foi possível abrir o Parquet no navegador. O arquivo bruto continua disponível no Internet Archive.");
    console.error("Falha ao iniciar explorador", error);
  }
}
