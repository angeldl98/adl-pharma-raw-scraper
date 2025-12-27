import { createHash } from "crypto";
import { Pool } from "pg";

const CIMA_BASE =
  process.env.PHARMA_CIMA_URL || "https://cima.aemps.es/cima/rest/medicamentos";
const PAGE_SIZE = Number(process.env.PHARMA_PAGE_SIZE || "200");
const MAX_PAGES = Number(process.env.PHARMA_MAX_PAGES || "200"); // safety hard-cap

const pool = new Pool({
  host: process.env.PGHOST || process.env.POSTGRES_HOST || "postgres",
  user: process.env.PGUSER || process.env.POSTGRES_USER || "adl",
  password: process.env.PGPASSWORD || process.env.POSTGRES_PASSWORD || "",
  database: process.env.PGDATABASE || process.env.POSTGRES_DB || "adl_core",
  port: Number(process.env.PGPORT || "5432")
});

function checksumRecord(rec) {
  const h = createHash("sha256");
  h.update(
    [
      rec.nregistro ?? "",
      String(rec.comerc ?? ""),
      String(rec.estado?.aut ?? ""),
      String(rec.estado?.rev ?? ""),
      String(rec.estado?.susp ?? "")
    ].join("|")
  );
  return h.digest("hex");
}

async function ensureTable(client) {
  await client.query(`CREATE SCHEMA IF NOT EXISTS pharma_raw`);
  await client.query(`
    CREATE TABLE IF NOT EXISTS pharma_raw.medicamentos (
      id SERIAL PRIMARY KEY,
      nregistro TEXT,
      nombre TEXT,
      labtitular TEXT,
      labcomercializador TEXT,
      comerc BOOLEAN,
      estado_aut BIGINT,
      estado_rev BIGINT,
      estado_susp BIGINT,
      checksum TEXT UNIQUE,
      payload JSONB,
      fetched_at TIMESTAMPTZ DEFAULT now()
    )
  `);
}

async function fetchPage(page) {
  const url = `${CIMA_BASE}?pagina=${page}&tamanioPagina=${PAGE_SIZE}`;
  const res = await fetch(url, { headers: { Accept: "application/json" } });
  if (!res.ok) throw new Error(`fetch_failed status=${res.status} page=${page}`);
  return res.json();
}

async function main() {
  console.log(`[info] PHARMA_RAW_START source=${CIMA_BASE}`);
  const client = await pool.connect();
  try {
    await ensureTable(client);

    const first = await fetchPage(1);
    const total = Number(first.totalFilas || 0);
    const totalPages = Math.min(Math.ceil(total / PAGE_SIZE), MAX_PAGES);
    console.log(
      `[info] cima_total=${total} page_size=${PAGE_SIZE} total_pages=${totalPages}`
    );

    let inserted = 0;
    const insertStmt = `
      INSERT INTO pharma_raw.medicamentos
        (nregistro, nombre, labtitular, labcomercializador, comerc, estado_aut, estado_rev, estado_susp, checksum, payload)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
      ON CONFLICT (checksum) DO NOTHING
    `;

    const processPage = async (pageData) => {
      const rows = pageData?.resultados || [];
      for (const rec of rows) {
        const cs = checksumRecord(rec);
        await client.query(insertStmt, [
          rec.nregistro ?? null,
          rec.nombre ?? null,
          rec.labtitular ?? null,
          rec.labcomercializador ?? null,
          rec.comerc ?? null,
          rec.estado?.aut ?? null,
          rec.estado?.rev ?? null,
          rec.estado?.susp ?? null,
          cs,
          rec ?? null
        ]);
        inserted += 1;
      }
    };

    await processPage(first);
    for (let page = 2; page <= totalPages; page++) {
      const data = await fetchPage(page);
      await processPage(data);
      if (page % 10 === 0) {
        console.log(`[info] processed_page=${page}/${totalPages}`);
      }
    }

    console.log(`[info] PHARMA_RAW_OK inserted=${inserted}`);
    process.exit(0);
  } catch (err) {
    console.error(`[error] PHARMA_RAW_FAIL ${err?.message || err}`);
    process.exit(1);
  } finally {
    client.release();
    await pool.end();
  }
}

main().catch((err) => {
  console.error(`[error] PHARMA_RAW_FAIL ${err?.message || err}`);
  process.exit(1);
});

