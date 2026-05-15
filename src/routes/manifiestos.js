import { Router } from 'express';
import { pool }   from '../db.js';
import { scrapeAereo }    from '../scrapers/aereo.js';
import { scrapeMaritimo } from '../scrapers/maritimo.js';
import { parseFechaToISO } from '../scrapers/clasificacion.js';

const router = Router();

// ── Diccionario global para evitar scraping duplicado ─────────────────────────
const activeJobs = new Set();

// ── Whitelist de columnas filtrables (evita SQL injection) ────────────────────
const ALLOWED_COLS = new Set([
  'Manifiesto', 'Fecha de Zarpe', 'Nombre de Nave', 'Detalle', 'Puerto', 'BL',
  'Fecha de Transmisión', 'Bultos', 'Peso Bruto', 'Empaques', 'Embarcador',
  'Consignatario', 'Marcas y Números', 'Descripción de Mercadería',
  'Envio', 'Semana', 'Año', 'Producto', 'Tipo', 'Pais', 'Ciudad destino', 'Continente',
]);

/**
 * Parsea ColumnFiltersState de TanStack Table y construye fragmentos WHERE.
 * Formato de entrada: [{ id: "Columna", value: ["v1","v2"] }]
 *
 * @param {string} filtersJson  - JSON string del query param `filters`
 * @param {number} startIdx     - índice del próximo parámetro ($n) en la query
 * @returns {{ sql: string, params: any[][] }}
 */
function buildFilterClauses(filtersJson, startIdx) {
  if (!filtersJson) return { sql: '', params: [] };
  let parsed;
  try { parsed = JSON.parse(filtersJson); } catch { return { sql: '', params: [] }; }
  if (!Array.isArray(parsed) || parsed.length === 0) return { sql: '', params: [] };

  const parts  = [];
  const params = [];

  for (const f of parsed) {
    if (!f || typeof f.id !== 'string') continue;
    if (!ALLOWED_COLS.has(f.id)) continue;                          // seguridad: solo cols conocidas
    const values = (Array.isArray(f.value) ? f.value : [f.value])
      .filter(v => v != null && v !== '');
    if (values.length === 0) continue;

    params.push(values);
    parts.push(`"${f.id}" = ANY($${startIdx + params.length - 1})`);
  }

  return {
    sql:    parts.length ? ' AND ' + parts.join(' AND ') : '',
    params,
  };
}

// Columnas expuestas en la API (excluye _fecha_iso)
const SELECT_COLS = [
  '"Manifiesto"', '"Fecha de Zarpe"', '"Nombre de Nave"', '"Detalle"', '"Puerto"', '"BL"',
  '"Fecha de Transmisión"', '"Bultos"', '"Peso Bruto"', '"Empaques"', '"Embarcador"',
  '"Consignatario"', '"Marcas y Números"', '"Descripción de Mercadería"',
  '"Envio"', '"Semana"', '"Año"', '"Producto"', '"Tipo"', '"Pais"', '"Ciudad destino"', '"Continente"',
].join(', ');

// Columnas para INSERT (incluye _fecha_iso)
const DB_COLS = [
  'Manifiesto', 'Fecha de Zarpe', 'Nombre de Nave', 'Detalle', 'Puerto', 'BL',
  'Fecha de Transmisión', 'Bultos', 'Peso Bruto', 'Empaques', 'Embarcador',
  'Consignatario', 'Marcas y Números', 'Descripción de Mercadería',
  'Envio', 'Semana', 'Año', 'Producto', 'Tipo', 'Pais', 'Ciudad destino', 'Continente',
  '_fecha_iso',
];
const COL_NAMES_SQL = DB_COLS.map(c => `"${c}"`).join(', ');

// ── Helpers DB ────────────────────────────────────────────────────────────────

function getDaysInRange(fechaIsoStart, fechaIsoEnd) {
  const days = [];
  const end  = new Date(fechaIsoEnd);
  for (const d = new Date(fechaIsoStart); d <= end; d.setUTCDate(d.getUTCDate() + 1)) {
    days.push(d.toISOString().slice(0, 10));
  }
  return days;
}

function isoToFecha(isoDate) {
  const [y, m, d] = isoDate.split('-');
  return `${d}/${m}/${y}`;
}

async function getMissingDays(tipo, days) {
  const r = await pool.query(
    'SELECT fecha FROM scrape_log WHERE tipo=$1 AND fecha = ANY($2)',
    [tipo, days],
  );
  const scraped = new Set(r.rows.map(row => row.fecha));
  return days.filter(d => !scraped.has(d));
}

async function clearRange(tipo, fechaIsoStart, fechaIsoEnd) {
  await pool.query(
    'DELETE FROM manifiestos WHERE "Envio"=$1 AND _fecha_iso BETWEEN $2 AND $3',
    [tipo, fechaIsoStart, fechaIsoEnd],
  );
}

async function saveBatch(rows) {
  if (!rows || rows.length === 0) return;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const BATCH = 1000;
    for (let i = 0; i < rows.length; i += BATCH) {
      const batch = rows.slice(i, i + BATCH);
      const placeholders = batch.map((_, j) =>
        `(${DB_COLS.map((__, k) => `$${j * DB_COLS.length + k + 1}`).join(', ')})`
      ).join(', ');
      const values = batch.flatMap(row => DB_COLS.map(col => row[col] ?? null));
      await client.query(
        `INSERT INTO manifiestos (${COL_NAMES_SQL}) VALUES ${placeholders}`,
        values,
      );
    }
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

async function markAsScraped(tipo, logKey, rowCount) {
  await pool.query(
    `INSERT INTO scrape_log (tipo, fecha, row_count) VALUES ($1, $2, $3)
     ON CONFLICT (tipo, fecha) DO UPDATE SET scraped_at = NOW(), row_count = EXCLUDED.row_count`,
    [tipo, logKey, rowCount],
  );
}

// ── GET /api/manifiestos/filtros ──────────────────────────────────────────────
// Devuelve los valores únicos de una columna para poblar los dropdowns del front.
// Ej: GET /api/manifiestos/filtros?columna=Producto&fecha_inicio=13/04/2026&fecha_fin=13/04/2026
router.get('/filtros', async (req, res) => {
  try {
    const { columna, fecha_inicio, fecha_fin, tipos: tiposParam } = req.query;
    if (!columna || !ALLOWED_COLS.has(columna)) {
      return res.status(400).json({ error: `Columna no válida. Opciones: ${[...ALLOWED_COLS].join(', ')}` });
    }

    const params = [];
    const conditions = [`"${columna}" IS NOT NULL AND "${columna}"::text <> ''`];

    // Filtro opcional por rango de fechas
    if (fecha_inicio && fecha_fin) {
      const fechaIsoStart = parseFechaToISO(fecha_inicio);
      const fechaIsoEnd   = parseFechaToISO(fecha_fin);
      if (fechaIsoStart && fechaIsoEnd) {
        params.push(fechaIsoStart, fechaIsoEnd);
        conditions.push(`_fecha_iso BETWEEN $${params.length - 1} AND $${params.length}`);
      }
    }

    // Filtro opcional por tipo
    const tipos = (tiposParam ?? '')
      .split(',').map(t => t.trim().toLowerCase())
      .filter(t => ['aereo', 'maritimo'].includes(t));
    if (tipos.length) {
      params.push(tipos);
      conditions.push(`"Envio" = ANY($${params.length})`);
    }

    const where = conditions.join(' AND ');
    // El nombre de columna ya está validado contra la whitelist → interpolación segura
    const result = await pool.query(
      `SELECT DISTINCT "${columna}" AS val
       FROM manifiestos
       WHERE ${where}
       ORDER BY val ASC`,
      params,
    );
    res.json(result.rows.map(r => r.val));
  } catch (err) {
    console.error('[/filtros]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/manifiestos/fechas ───────────────────────────────────────────────
router.get('/fechas', async (req, res) => {
  try {
    const tiposParam = req.query.tipos;
    const tipos = tiposParam
      ? tiposParam.split(',').map(t => t.trim().toLowerCase()).filter(t => ['aereo', 'maritimo'].includes(t))
      : ['aereo', 'maritimo'];

    const result = await pool.query(
      `SELECT _fecha_iso::text AS fecha, "Envio", COUNT(*)::int AS count
       FROM manifiestos
       WHERE "Envio" = ANY($1)
       GROUP BY _fecha_iso, "Envio"
       ORDER BY _fecha_iso DESC`,
      [tipos],
    );

    // Agrupar por fecha → { fecha, aereo, maritimo, total }
    const byFecha = {};
    for (const row of result.rows) {
      const f = row.fecha;
      if (!byFecha[f]) byFecha[f] = { fecha: f, aereo: 0, maritimo: 0, total: 0 };
      byFecha[f][row.Envio] = row.count;
      byFecha[f].total      += row.count;
    }

    res.json({ fechas: Object.values(byFecha) });
  } catch (err) {
    console.error('[/fechas]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/manifiestos ──────────────────────────────────────────────────────
router.get('/', async (req, res) => {
  try {
    const {
      fecha_inicio,
      fecha_fin,
      tipos: tiposParam,
      filters       = '',
      page          = '1',
      page_size     = '1000',
      force_refresh = 'false',
    } = req.query;

    if (!fecha_inicio || !fecha_fin) {
      return res.status(400).json({ error: 'fecha_inicio y fecha_fin son requeridos (dd/mm/yyyy)' });
    }

    const tipos = (tiposParam ?? '')
      .split(',').map(t => t.trim().toLowerCase())
      .filter(t => ['aereo', 'maritimo'].includes(t));

    if (!tipos.length) {
      return res.status(400).json({ error: 'El parámetro tipos debe ser "aereo", "maritimo" o ambos.' });
    }

    const forceRefresh  = force_refresh === 'true';
    const pageNum       = Math.max(1, parseInt(page)      || 1);
    const pageSize      = Math.min(5000, Math.max(1, parseInt(page_size) || 1000));
    const fechaIsoStart = parseFechaToISO(fecha_inicio);
    const fechaIsoEnd   = parseFechaToISO(fecha_fin);

    if (!fechaIsoStart || !fechaIsoEnd) {
      return res.status(400).json({ error: 'Formato de fecha inválido. Use dd/mm/yyyy' });
    }
    if (fechaIsoStart > fechaIsoEnd) {
      return res.status(400).json({ error: 'fecha_inicio no puede ser mayor que fecha_fin' });
    }

    const logKey   = `${fecha_inicio}|${fecha_fin}`;
    const allDays  = getDaysInRange(fechaIsoStart, fechaIsoEnd);

    // ── Determinar qué días faltan por tipo ────────────────────────────────
    const toScrape = {};
    for (const tipo of tipos) {
      const missing = forceRefresh
        ? allDays
        : await getMissingDays(tipo, allDays);
      if (missing.length > 0) toScrape[tipo] = missing;
    }

    const jobKey         = `${tipos.join('-')}|${logKey}`;
    const hasPendingWork = Object.keys(toScrape).length > 0;

    // ── Background Worker: dispara scraping día a día sin bloquear Express ─
    if (hasPendingWork) {
      if (!activeJobs.has(jobKey)) {
        activeJobs.add(jobKey);

        (async () => {
          try {
            for (const [tipo, days] of Object.entries(toScrape)) {
              for (const day of days) {
                const fechaDD = isoToFecha(day);
                console.log(`[worker] Iniciando ${tipo}: ${fechaDD}`);
                await clearRange(tipo, day, day);

                let totalFilas = 0;
                const onBatchScraped = async (filasMani) => {
                  await saveBatch(filasMani);
                  totalFilas += filasMani.length;
                };

                if (tipo === 'aereo') {
                  await scrapeAereo(fechaDD, fechaDD, onBatchScraped);
                } else {
                  await scrapeMaritimo(fechaDD, fechaDD, onBatchScraped);
                }

                await markAsScraped(tipo, day, totalFilas);
                console.log(`[worker] ${tipo} ${fechaDD} terminado. Filas: ${totalFilas}`);
              }
            }
          } catch (error) {
            console.error(`[worker] Error fatal en job:`, error);
          } finally {
            activeJobs.delete(jobKey);
          }
        })();
      }

      return res.status(202).json({
        status: 'procesando',
        total: 0, page: pageNum, page_size: pageSize, pages: 1, data: [],
      });
    }

    // ── Construir WHERE dinámico con filtros de columna ────────────────────
    // baseParams: $1=tipos, $2=fechaIsoStart, $3=fechaIsoEnd
    const baseParams = [tipos, fechaIsoStart, fechaIsoEnd];
    const { sql: filterSQL, params: filterParams } = buildFilterClauses(filters, baseParams.length + 1);
    const allBaseParams = [...baseParams, ...filterParams];
    const whereClause = `"Envio" = ANY($1) AND _fecha_iso BETWEEN $2 AND $3${filterSQL}`;

    // ── Consulta paginada ──────────────────────────────────────────────────
    const offset     = (pageNum - 1) * pageSize;
    const limitIdx   = allBaseParams.length + 1;
    const offsetIdx  = allBaseParams.length + 2;

    const [countRes, dataRes] = await Promise.all([
      pool.query(
        `SELECT COUNT(*)::int AS total FROM manifiestos WHERE ${whereClause}`,
        allBaseParams,
      ),
      pool.query(
        `SELECT ${SELECT_COLS}
         FROM manifiestos
         WHERE ${whereClause}
         ORDER BY "Manifiesto", "BL"
         LIMIT $${limitIdx} OFFSET $${offsetIdx}`,
        [...allBaseParams, pageSize, offset],
      ),
    ]);

    const total = countRes.rows[0].total;

    res.json({
      total,
      page:      pageNum,
      page_size: pageSize,
      pages:     Math.ceil(total / pageSize) || 1,
      data:      dataRes.rows,
    });
  } catch (err) {
    console.error('[/manifiestos]', err.message);
    res.status(500).json({ error: err.message });
  }
});

export default router;
