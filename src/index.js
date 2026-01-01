import { createHash } from "crypto";
import { Pool } from "pg";
import axios from "axios";

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
  await client.query(
    `CREATE UNIQUE INDEX IF NOT EXISTS idx_medicamentos_nregistro ON pharma_raw.medicamentos (nregistro)`
  );
}

async function fetchPage(page) {
  const url = `${CIMA_BASE}?pagina=${page}&tamanioPagina=${PAGE_SIZE}`;
  const res = await axios.get(url, {
    timeout: 15000,
    headers: { Accept: "application/json", "User-Agent": "adl-pharma-raw-scraper/0.1" }
  });
  return res.data;
}

async function main() {
  console.log(`[info] PHARMA_RAW_START source=${CIMA_BASE}`);
  const client = await pool.connect();
  let runId = null;
  const stats = { inserted: 0, updated: 0, unchanged: 0, total: 0 };
  try {
    await ensureTable(client);
    await client.query(`CREATE EXTENSION IF NOT EXISTS "pgcrypto"`);
    const run = await client.query(
      `INSERT INTO pipeline_runs (pipeline, status, started_at, stats) VALUES ($1, 'running', now(), '{}'::jsonb) RETURNING id`,
      ["pharma-raw"]
    );
    runId = run.rows[0].id;

    const first = await fetchPage(1);
    const total = Number(first.totalFilas || 0);
    const totalPages = Math.min(Math.ceil(total / PAGE_SIZE), MAX_PAGES);
    console.log(
      `[info] cima_total=${total} page_size=${PAGE_SIZE} total_pages=${totalPages}`
    );

    const upsertStmt = `
      INSERT INTO pharma_raw.medicamentos
        (nregistro, nombre, labtitular, labcomercializador, comerc, estado_aut, estado_rev, estado_susp, checksum, payload)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
      ON CONFLICT (nregistro) DO UPDATE SET
        nombre = EXCLUDED.nombre,
        labtitular = EXCLUDED.labtitular,
        labcomercializador = EXCLUDED.labcomercializador,
        comerc = EXCLUDED.comerc,
        estado_aut = EXCLUDED.estado_aut,
        estado_rev = EXCLUDED.estado_rev,
        estado_susp = EXCLUDED.estado_susp,
        checksum = EXCLUDED.checksum,
        payload = EXCLUDED.payload,
        fetched_at = now()
      WHERE pharma_raw.medicamentos.checksum IS DISTINCT FROM EXCLUDED.checksum
      RETURNING (xmax = 0) AS inserted
    `;

    const processPage = async (pageData) => {
      const rows = pageData?.resultados || [];
      for (const rec of rows) {
        const cs = checksumRecord(rec);
        const res = await client.query(upsertStmt, [
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
        stats.total += 1;
        if (res.rowCount === 0) {
          stats.unchanged += 1;
        } else if (res.rows[0].inserted) {
          stats.inserted += 1;
        } else {
          stats.updated += 1;
        }
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

    console.log(
      `[info] PHARMA_RAW_OK inserted=${stats.inserted} updated=${stats.updated} unchanged=${stats.unchanged} total=${stats.total}`
    );
    if (runId) {
      await client.query(
        `UPDATE pipeline_runs SET status='ok', finished_at=now(), stats=$2 WHERE id=$1`,
        [runId, JSON.stringify(stats)]
      );
    }
    process.exit(0);
  } catch (err) {
    console.error(`[error] PHARMA_RAW_FAIL ${err?.message || err}`);
    if (runId) {
      await client.query(
        `UPDATE pipeline_runs SET status='error', finished_at=now(), error=$2 WHERE id=$1`,
        [runId, String(err?.message || err)]
      );
    }
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

