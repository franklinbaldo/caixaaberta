import * as duckdb from "@duckdb/duckdb-wasm";
import {
  Map as MapLibreMap,
  NavigationControl,
  Popup,
  type GeoJSONSource,
  type StyleSpecification,
} from "maplibre-gl";

type Manifest = { data: string; item: string; parquet_url: string };
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
  modalidade: string;
  financiamento: string;
  latitude: number | null;
  longitude: number | null;
  precisao: string;
  cno: string;
  cno_match_status: string;
  cno_match_score: number | null;
  cno_match_method: string;
  cno_match_candidate_count: number | null;
  cno_situacao: string;
  cno_data_inicio: string;
  cno_area_total: number | null;
  cno_nome_obra: string;
  cno_categorias: string;
  cno_destinacoes: string;
  cno_tipos_obra: string;
};

const currency = new Intl.NumberFormat("pt-BR", { style: "currency", currency: "BRL" });
const integer = new Intl.NumberFormat("pt-BR");
const decimal = new Intl.NumberFormat("pt-BR", { maximumFractionDigits: 2 });
const text = (value: unknown) => (value == null ? "" : String(value));
const number = (value: unknown): number | null => {
  const parsed = Number(value);
  return value == null || value === "" || !Number.isFinite(parsed) ? null : parsed;
};

function localDataUrl(file: string) {
  const basePath = import.meta.env.BASE_URL.replace(/\/?$/, "/");
  return new URL(`${basePath}data/${file}`, window.location.origin).href;
}

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
    modalidade: text(row.modalidade),
    financiamento: text(row.financiamento),
    latitude: number(row.latitude),
    longitude: number(row.longitude),
    precisao: text(row.precisao),
    cno: text(row.cno),
    cno_match_status: text(row.cno_match_status) || "sem_dados",
    cno_match_score: number(row.cno_match_score),
    cno_match_method: text(row.cno_match_method),
    cno_match_candidate_count: number(row.cno_match_candidate_count),
    cno_situacao: text(row.cno_situacao),
    cno_data_inicio: text(row.cno_data_inicio),
    cno_area_total: number(row.cno_area_total),
    cno_nome_obra: text(row.cno_nome_obra),
    cno_categorias: text(row.cno_categorias),
    cno_destinacoes: text(row.cno_destinacoes),
    cno_tipos_obra: text(row.cno_tipos_obra),
  };
}

function required<T extends Element>(selector: string): T {
  const element = document.querySelector<T>(selector);
  if (!element) throw new Error(`Elemento obrigatório ausente: ${selector}`);
  return element;
}

async function loadProperties(onStatus: (message: string) => void) {
  onStatus("Lendo o snapshot espelhado no site…");
  const [manifestResponse, parquetResponse] = await Promise.all([
    fetch(localDataUrl("latest.json"), { cache: "no-store" }),
    fetch(localDataUrl("snapshot.parquet"), { cache: "no-store" }),
  ]);
  if (!manifestResponse.ok) throw new Error(`manifesto respondeu ${manifestResponse.status}`);
  if (!parquetResponse.ok) throw new Error(`Parquet respondeu ${parquetResponse.status}`);
  const manifest = (await manifestResponse.json()) as Manifest;
  const parquet = new Uint8Array(await parquetResponse.arrayBuffer());

  onStatus("Abrindo o Parquet com DuckDB no navegador…");
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
    await db.registerFileBuffer("snapshot.parquet", parquet);
    const connection = await db.connect();
    try {
      const table = await connection.query("SELECT * FROM 'snapshot.parquet'");
      const rows = table.toArray().map((row) => row.toJSON() as RawRow).map(normalize);
      return { manifest, rows };
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

function matchLabel(value: string) {
  return (
    {
      forte: "CNO associado",
      provavel: "CNO provável — não associado",
      ambiguo: "CNO ambíguo — não associado",
      sem_match: "sem candidato CNO",
      sem_dados: "snapshot sem CNO",
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

  const map = new MapLibreMap({
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
        layout: { "text-field": ["get", "point_count_abbreviated"], "text-size": 12 },
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
        const offer = document.createElement("p");
        const price = Number(props.preco);
        const discount = Number(props.desconto);
        offer.textContent = [
          Number.isFinite(price) ? currency.format(price) : "preço não informado",
          Number.isFinite(discount) ? `${decimal.format(discount)}% de desconto` : "",
        ].filter(Boolean).join(" · ");
        const detail = document.createElement("small");
        detail.textContent = `Imóvel ${props.link ?? ""} · precisão: ${precisionLabel(String(props.precisao ?? ""))}`;
        content.append(title, address, offer, detail);

        if (props.cno) {
          const cnoDetail = document.createElement("p");
          const area = Number(props.cno_area_total);
          const areaLabel = Number.isFinite(area) ? ` · ${decimal.format(area)} m²` : "";
          cnoDetail.textContent = `CNO ${props.cno} · ${props.cno_situacao || "situação não informada"}${areaLabel}`;
          content.append(cnoDetail);
        }

        const href = safeSourceUrl(String(props.link_acesso ?? ""));
        if (href) {
          const sourceLink = document.createElement("a");
          sourceLink.href = href;
          sourceLink.target = "_blank";
          sourceLink.rel = "noopener noreferrer";
          sourceLink.textContent = "Ver oferta oficial";
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
        geometry: { type: "Point" as const, coordinates: [row.longitude as number, row.latitude as number] },
        properties: {
          link: row.link,
          link_acesso: row.link_acesso,
          endereco: row.endereco,
          cidade: row.cidade,
          estado: row.estado,
          preco: row.preco,
          desconto: row.desconto,
          precisao: row.precisao,
          cno: row.cno,
          cno_situacao: row.cno_situacao,
          cno_area_total: row.cno_area_total,
        },
      })),
  };
}

function appendTextCell(row: HTMLTableRowElement, value: string) {
  const cell = document.createElement("td");
  cell.textContent = value || "—";
  row.append(cell);
}

function appendCnoCell(row: HTMLTableRowElement, property: Property) {
  const cell = document.createElement("td");
  if (!property.cno) {
    cell.textContent = matchLabel(property.cno_match_status) || "—";
    row.append(cell);
    return;
  }

  const id = document.createElement("strong");
  id.textContent = `CNO ${property.cno}`;
  const details = document.createElement("small");
  const pieces = [property.cno_situacao, property.cno_data_inicio];
  if (property.cno_area_total != null) pieces.push(`${decimal.format(property.cno_area_total)} m²`);
  details.textContent = pieces.filter(Boolean).join(" · ");
  cell.append(id);
  if (details.textContent) cell.append(document.createElement("br"), details);
  if (property.cno_nome_obra) {
    const name = document.createElement("small");
    name.textContent = property.cno_nome_obra;
    cell.append(document.createElement("br"), name);
  }
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
    appendTextCell(tr, property.avaliacao == null ? "—" : currency.format(property.avaliacao));
    appendTextCell(tr, property.desconto == null ? "—" : `${decimal.format(property.desconto)} %`);
    appendTextCell(tr, property.modalidade);
    appendTextCell(tr, property.financiamento);
    appendCnoCell(tr, property);
    appendTextCell(tr, precisionLabel(property.precisao));
    fragment.append(tr);
  }
  tbody.replaceChildren(fragment);
}

function metric(term: string, value: string, emphasis = false) {
  const wrapper = document.createElement("div");
  const dt = document.createElement("dt");
  const dd = document.createElement("dd");
  dt.textContent = term;
  dd.textContent = value;
  if (emphasis) dd.dataset.emphasis = "true";
  wrapper.append(dt, dd);
  return wrapper;
}

function renderCards(container: HTMLElement, rows: Property[]) {
  const fragment = document.createDocumentFragment();
  for (const property of rows.slice(0, 100)) {
    const card = document.createElement("article");
    card.dataset.propertyCard = "";

    const header = document.createElement("header");
    const place = document.createElement("strong");
    place.textContent = [property.cidade, property.estado].filter(Boolean).join(" / ") || "Local não informado";
    const modality = document.createElement("small");
    modality.textContent = property.modalidade || "Modalidade não informada";
    header.append(place, modality);

    const title = document.createElement("h3");
    title.textContent = property.descricao || `Imóvel ${property.link || "Caixa"}`;
    const address = document.createElement("p");
    address.dataset.propertyAddress = "";
    address.textContent = [property.bairro, property.endereco].filter(Boolean).join(" · ") || "Endereço não informado";

    const metrics = document.createElement("dl");
    metrics.dataset.propertyMetrics = "";
    metrics.append(
      metric("Preço", property.preco == null ? "—" : currency.format(property.preco), true),
      metric("Avaliação", property.avaliacao == null ? "—" : currency.format(property.avaliacao)),
      metric("Desconto", property.desconto == null ? "—" : `${decimal.format(property.desconto)}%`, true),
    );

    const context = document.createElement("p");
    context.dataset.propertyContext = "";
    const contextParts = [];
    if (property.financiamento) contextParts.push(`Financiamento: ${property.financiamento}`);
    if (property.cno) contextParts.push(`CNO ${property.cno}${property.cno_situacao ? ` · ${property.cno_situacao}` : ""}`);
    else if (property.cno_match_status && property.cno_match_status !== "sem_dados") contextParts.push(matchLabel(property.cno_match_status));
    context.textContent = contextParts.join(" · ") || "Sem contexto adicional publicado";

    const footer = document.createElement("footer");
    const source = safeSourceUrl(property.link_acesso);
    if (source) {
      const link = document.createElement("a");
      link.href = source;
      link.target = "_blank";
      link.rel = "noopener noreferrer";
      link.textContent = "Ver oferta oficial ↗";
      footer.append(link);
    } else {
      const unavailable = document.createElement("span");
      unavailable.textContent = "Link oficial indisponível neste snapshot";
      footer.append(unavailable);
    }
    const identifier = document.createElement("small");
    identifier.textContent = property.link ? `Imóvel ${property.link}` : "Identificador não informado";
    footer.append(identifier);

    card.append(header, title, address, metrics, context, footer);
    fragment.append(card);
  }
  container.replaceChildren(fragment);
}

function sortRows(rows: Property[], order: string) {
  const sorted = [...rows];
  const finite = (value: number | null, fallback: number) => value ?? fallback;
  switch (order) {
    case "price-asc":
      return sorted.sort((a, b) => finite(a.preco, Infinity) - finite(b.preco, Infinity));
    case "price-desc":
      return sorted.sort((a, b) => finite(b.preco, -Infinity) - finite(a.preco, -Infinity));
    case "city-asc":
      return sorted.sort((a, b) => `${a.cidade}-${a.estado}`.localeCompare(`${b.cidade}-${b.estado}`, "pt-BR"));
    default:
      return sorted.sort((a, b) => finite(b.desconto, -Infinity) - finite(a.desconto, -Infinity));
  }
}

export async function initExplorer() {
  const app = document.querySelector<HTMLElement>("[data-explorer-app]");
  if (!app || app.dataset.initialized === "true") return;
  app.dataset.initialized = "true";

  const form = required<HTMLFormElement>("#explorer-filters");
  const query = required<HTMLInputElement>("#filter-query");
  const state = required<HTMLSelectElement>("#filter-state");
  const modality = required<HTMLSelectElement>("#filter-modality");
  const priceMax = required<HTMLInputElement>("#filter-price-max");
  const discount = required<HTMLInputElement>("#filter-discount");
  const financing = required<HTMLSelectElement>("#filter-financing");
  const sort = required<HTMLSelectElement>("#filter-sort");
  const cnoMatch = required<HTMLSelectElement>("#filter-cno-match");
  const cnoSituation = required<HTMLSelectElement>("#filter-cno-situation");
  const precision = required<HTMLSelectElement>("#filter-precision");
  const count = required<HTMLElement>("#explorer-count");
  const date = required<HTMLElement>("#explorer-date");
  const status = required<HTMLElement>("#explorer-status");
  const cards = required<HTMLElement>("#explorer-cards");
  const tbody = required<HTMLTableSectionElement>("#explorer-rows");
  const setStatus = (message: string) => { status.textContent = message; };

  try {
    const { map, ready } = setupMap();
    const { manifest, rows } = await loadProperties(setStatus);
    options(state, [...new Set(rows.map((row) => row.estado))]);
    options(modality, [...new Set(rows.map((row) => row.modalidade))]);
    options(financing, [...new Set(rows.map((row) => row.financiamento))]);
    options(cnoSituation, [...new Set(rows.map((row) => row.cno_situacao))]);
    date.textContent = `Snapshot ${manifest.data}`;

    const render = async () => {
      const needle = query.value.trim().toLocaleLowerCase("pt-BR");
      const maximumPrice = priceMax.value === "" ? null : Number(priceMax.value);
      const minimumDiscount = discount.value === "" ? null : Number(discount.value);
      const filtered = rows.filter((property) => {
        if (state.value && property.estado !== state.value) return false;
        if (modality.value && property.modalidade !== modality.value) return false;
        if (financing.value && property.financiamento !== financing.value) return false;
        if (cnoMatch.value && property.cno_match_status !== cnoMatch.value) return false;
        if (cnoSituation.value && property.cno_situacao !== cnoSituation.value) return false;
        if (precision.value && property.precisao !== precision.value) return false;
        if (maximumPrice != null && (property.preco ?? Infinity) > maximumPrice) return false;
        if (minimumDiscount != null && (property.desconto ?? -Infinity) < minimumDiscount) return false;
        if (needle) {
          const haystack = [
            property.link,
            property.endereco,
            property.bairro,
            property.cidade,
            property.estado,
            property.descricao,
            property.modalidade,
            property.financiamento,
            property.cno,
            property.cno_nome_obra,
            property.cno_categorias,
            property.cno_destinacoes,
            property.cno_tipos_obra,
          ].join(" ").toLocaleLowerCase("pt-BR");
          if (!haystack.includes(needle)) return false;
        }
        return true;
      });

      const ordered = sortRows(filtered, sort.value);
      count.textContent = `${integer.format(ordered.length)} imóvel${ordered.length === 1 ? "" : "is"}`;
      const geocoded = ordered.filter((property) => property.latitude != null && property.longitude != null).length;
      setStatus(`${integer.format(geocoded)} com coordenadas para o mapa · até 100 oportunidades visíveis na lista.`);
      renderCards(cards, ordered);
      renderTable(tbody, ordered);
      await ready;
      (map.getSource("imoveis") as GeoJSONSource).setData(toGeoJSON(ordered));
    };

    let timer = 0;
    const scheduleRender = () => {
      window.clearTimeout(timer);
      timer = window.setTimeout(() => void render(), 80);
    };
    form.addEventListener("input", scheduleRender);
    form.addEventListener("change", scheduleRender);
    form.addEventListener("reset", () => window.setTimeout(() => void render(), 0));
    await render();
    app.dataset.state = "ready";
  } catch (error) {
    app.dataset.state = "error";
    setStatus("Não foi possível abrir o retrato. Os dados preservados continuam disponíveis no Internet Archive.");
    console.error("Falha ao inicializar o explorador", error);
  }
}
