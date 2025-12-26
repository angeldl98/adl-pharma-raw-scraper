import { createHash } from "crypto";
import { Pool } from "pg";
import fs from "fs";
import path from "path";

const CSV_URL =
  process.env.PHARMA_CSV_URL ||
  "https://raw.githubusercontent.com/angeldl98/adl-pharma-raw-scraper/master/data/pharmacies.csv";

const pool = new Pool();

function checksum(row) {
  const h = createHash("sha256");
  h.update(row.join("|"));
  return h.digest("hex");
}

function parseCsv(text) {
  const lines = text.split(/\r?\n/).filter((l) => l.trim().length > 0);
  if (lines.length <= 1) return [];
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const parts = lines[i]
      .split(",")
      .map((p) => p.replace(/^"+|"+$/g, "").trim());
    if (parts.length < 5) continue;
    const [name, address, municipality, province, status] = parts;
    rows.push({ name, address, municipality, province, status });
  }
  return rows;
}

async function fetchCsv(url) {
  const res = await fetch(url, { timeout: 8000 });
  if (!res.ok) {
    throw new Error(`fetch_failed status=${res.status}`);
  }
  return res.text();
}

async function ensureTable(client) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS pharma_raw (
      id SERIAL PRIMARY KEY,
      name TEXT,
      address TEXT,
      municipality TEXT,
      province TEXT,
      status TEXT,
      checksum TEXT UNIQUE,
      fetched_at TIMESTAMPTZ DEFAULT now()
    )
  `);
}

async function main() {
  console.log(`[info] PHARMA_RAW_START source=${CSV_URL}`);
  const client = await pool.connect();
  try {
    await ensureTable(client);
    const csvText =
      CSV_URL.startsWith("file://")
        ? fs.readFileSync(path.join(CSV_URL.replace("file://", "")), "utf8")
        : await fetchCsv(CSV_URL);
    const rows = parseCsv(csvText);
    console.log(`[info] parsed rows=${rows.length}`);
    let inserted = 0;
    for (const row of rows) {
      const cs = checksum([row.name, row.address, row.municipality, row.province, row.status]);
      await client.query(
        `
          INSERT INTO pharma_raw (name, address, municipality, province, status, checksum)
          VALUES ($1,$2,$3,$4,$5,$6)
          ON CONFLICT (checksum) DO NOTHING
        `,
        [row.name, row.address, row.municipality, row.province, row.status, cs]
      );
      inserted += 1;
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

