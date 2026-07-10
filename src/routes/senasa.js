import { Router } from 'express';
import { pool } from '../db.js';
import ExcelJS from 'exceljs';
import { scrapeSenasa } from '../scrapers/senasa.js';
import { parseFechaToISO } from '../scrapers/clasificacion.js';

const router = Router();

// Diccionarios globales para evitar scraping duplicado
const activeJobs = new Set();
const refreshedJobs = new Set();

const ALLOWED_COLS = new Set([
  'exportadora', 'empacadora', 'fecha', 'producto', 'pais_destino'
]);

function buildFilterClauses(filtersJson, startIdx) {
  if (!filtersJson) return { sql: '', params: [] };
  let parsed;
  try { parsed = JSON.parse(filtersJson); } catch { return { sql: '', params: [] }; }
  if (!Array.isArray(parsed) || parsed.length === 0) return { sql: '', params: [] };

  const parts = [];
  const params = [];

  for (const f of parsed) {
    if (!f || typeof f.id !== 'string') continue;
    if (!ALLOWED_COLS.has(f.id)) continue;
    const values = (Array.isArray(f.value) ? f.value : [f.value])
      .filter(v => v != null && v !== '');
    if (values.length === 0) continue;

    params.push(values);
    parts.push(`"${f.id}"::text = ANY($${startIdx + params.length - 1})`);
  }

  return {
    sql: parts.length ? ' AND ' + parts.join(' AND ') : '',
    params,
  };
}

function getDaysInRange(fechaIsoStart, fechaIsoEnd) {
  const days = [];
  const end = new Date(fechaIsoEnd);
  for (const d = new Date(fechaIsoStart); d <= end; d.setUTCDate(d.getUTCDate() + 1)) {
    days.push(d.toISOString().slice(0, 10));
  }
  return days;
}

function isoToFecha(isoDate) {
  const [y, m, d] = isoDate.split('-');
  return `${d}/${m}/${y}`;
}

async function getMissingDays(days) {
  const r = await pool.query(
    'SELECT fecha FROM scrape_log WHERE tipo=$1 AND fecha = ANY($2)',
    ['senasa', days],
  );
  const scraped = new Set(r.rows.map(row => row.fecha));
  return days.filter(d => !scraped.has(d));
}

async function clearRange(fechaIsoStart, fechaIsoEnd) {
  await pool.query(
    'DELETE FROM senasa_programacion WHERE fecha BETWEEN $1 AND $2',
    [fechaIsoStart, fechaIsoEnd],
  );
}

async function saveBatch(rows) {
  if (!rows || rows.length === 0) return;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const BATCH = 1000;
    const DB_COLS = ['exportadora', 'empacadora', 'fecha', 'producto', 'pais_destino'];
    const COL_NAMES_SQL = DB_COLS.map(c => `"${c}"`).join(', ');

    for (let i = 0; i < rows.length; i += BATCH) {
      const batch = rows.slice(i, i + BATCH);
      const placeholders = batch.map((_, j) =>
        `(${DB_COLS.map((__, k) => `$${j * DB_COLS.length + k + 1}`).join(', ')})`
      ).join(', ');
      const values = batch.flatMap(row => DB_COLS.map(col => row[col] ?? null));
      await client.query(
        `INSERT INTO senasa_programacion (${COL_NAMES_SQL}) VALUES ${placeholders}`,
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

async function markAsScraped(logKey, rowCount) {
  await pool.query(
    `INSERT INTO scrape_log (tipo, fecha, row_count) VALUES ($1, $2, $3)
     ON CONFLICT (tipo, fecha) DO UPDATE SET scraped_at = NOW(), row_count = EXCLUDED.row_count`,
    ['senasa', logKey, rowCount],
  );
}

// ── GET /api/senasa/filtros ───────────────────────────────────────────────────
router.get('/filtros', async (req, res) => {
  try {
    const { columna, fecha_inicio, fecha_fin } = req.query;
    if (!columna || !ALLOWED_COLS.has(columna)) {
      return res.status(400).json({ error: `Columna no válida. Opciones: ${[...ALLOWED_COLS].join(', ')}` });
    }

    const params = [];
    const conditions = [`"${columna}" IS NOT NULL AND "${columna}"::text <> ''`];

    if (fecha_inicio && fecha_fin) {
      const fechaIsoStart = parseFechaToISO(fecha_inicio);
      const fechaIsoEnd = parseFechaToISO(fecha_fin);
      if (fechaIsoStart && fechaIsoEnd) {
        params.push(fechaIsoStart, fechaIsoEnd);
        conditions.push(`fecha BETWEEN $${params.length - 1} AND $${params.length}`);
      }
    }

    const where = conditions.join(' AND ');
    const result = await pool.query(
      `SELECT DISTINCT "${columna}" AS val
       FROM senasa_programacion
       WHERE ${where}
       ORDER BY val ASC`,
      params,
    );
    res.json(result.rows.map(r => r.val));
  } catch (err) {
    console.error('[/senasa/filtros]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/senasa/fechas ────────────────────────────────────────────────────
router.get('/fechas', async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT fecha::text AS fecha, COUNT(*)::int AS count
       FROM senasa_programacion
       GROUP BY fecha
       ORDER BY fecha DESC`
    );
    res.json(result.rows.map(row => ({ fecha: row.fecha, count: row.count })));
  } catch (err) {
    console.error('[/senasa/fechas]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/senasa ───────────────────────────────────────────────────────────
router.get('/', async (req, res) => {
  try {
    const {
      fecha_inicio,
      fecha_fin,
      filters = '',
      page = '1',
      page_size = '1000',
      force_refresh = 'false',
    } = req.query;

    if (!fecha_inicio || !fecha_fin) {
      return res.status(400).json({ error: 'fecha_inicio y fecha_fin son requeridos (dd/mm/yyyy)' });
    }

    const forceRefresh = force_refresh === 'true';
    const pageNum = Math.max(1, parseInt(page) || 1);
    const pageSize = Math.min(5000, Math.max(1, parseInt(page_size) || 1000));
    const fechaIsoStart = parseFechaToISO(fecha_inicio);
    const fechaIsoEnd = parseFechaToISO(fecha_fin);

    if (!fechaIsoStart || !fechaIsoEnd) {
      return res.status(400).json({ error: 'Formato de fecha inválido. Use dd/mm/yyyy' });
    }
    if (fechaIsoStart > fechaIsoEnd) {
      return res.status(400).json({ error: 'fecha_inicio no puede ser mayor que fecha_fin' });
    }

    const logKey = `${fecha_inicio}|${fecha_fin}`;
    const allDays = getDaysInRange(fechaIsoStart, fechaIsoEnd);
    const jobKey = `senasa|${logKey}`;

    // Determinar qué días faltan por scrapear
    const shouldForce = forceRefresh && !activeJobs.has(jobKey) && !refreshedJobs.has(jobKey);
    const missingDays = shouldForce ? allDays : await getMissingDays(allDays);

    const hasPendingWork = missingDays.length > 0;

    if (hasPendingWork || activeJobs.has(jobKey)) {
      if (!activeJobs.has(jobKey)) {
        activeJobs.add(jobKey);
        if (forceRefresh) refreshedJobs.add(jobKey);

        (async () => {
          try {
            console.log(`[senasa-worker] Iniciando scraping de ${missingDays.length} días...`);
            // Limpiar rangos antes de scrapear
            for (const day of missingDays) {
              await clearRange(day, day);
            }

            let totalFilas = 0;
            const onBatchScraped = async (filas) => {
              await saveBatch(filas);
              totalFilas += filas.length;
            };

            // Ejecutar scraper para el rango de días faltantes completo
            const minDay = isoToFecha(missingDays[0]);
            const maxDay = isoToFecha(missingDays[missingDays.length - 1]);
            const inserted = await scrapeSenasa(minDay, maxDay, onBatchScraped);

            // Registrar individualmente cada día como escaneado
            for (const day of missingDays) {
              await markAsScraped(day, inserted > 0 ? Math.ceil(inserted / missingDays.length) : 0);
            }
            console.log(`[senasa-worker] Finalizado. Filas insertadas: ${totalFilas}`);
          } catch (error) {
            console.error('[senasa-worker] Error fatal en job:', error);
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

    // Consulta paginada de base de datos
    const baseParams = [fechaIsoStart, fechaIsoEnd];
    const { sql: filterSQL, params: filterParams } = buildFilterClauses(filters, baseParams.length + 1);
    const allBaseParams = [...baseParams, ...filterParams];
    const whereClause = `fecha BETWEEN $1 AND $2${filterSQL}`;

    const offset = (pageNum - 1) * pageSize;
    const limitIdx = allBaseParams.length + 1;
    const offsetIdx = allBaseParams.length + 2;

    const [countRes, dataRes] = await Promise.all([
      pool.query(
        `SELECT COUNT(*)::int AS total FROM senasa_programacion WHERE ${whereClause}`,
        allBaseParams,
      ),
      pool.query(
        `SELECT exportadora, empacadora, fecha::text, producto, pais_destino
         FROM senasa_programacion
         WHERE ${whereClause}
         ORDER BY fecha DESC, exportadora ASC
         LIMIT $${limitIdx} OFFSET $${offsetIdx}`,
        [...allBaseParams, pageSize, offset],
      ),
    ]);

    const total = countRes.rows[0].total;

    res.json({
      total,
      page: pageNum,
      page_size: pageSize,
      pages: Math.ceil(total / pageSize) || 1,
      data: dataRes.rows,
    });
  } catch (err) {
    console.error('[/senasa]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/senasa/export ────────────────────────────────────────────────────
router.get('/export', async (req, res) => {
  try {
    const { fecha_inicio, fecha_fin, filters = '' } = req.query;
    if (!fecha_inicio || !fecha_fin) {
      return res.status(400).json({ error: 'fecha_inicio y fecha_fin son requeridos (dd/mm/yyyy)' });
    }

    const fechaIsoStart = parseFechaToISO(fecha_inicio);
    const fechaIsoEnd = parseFechaToISO(fecha_fin);

    if (!fechaIsoStart || !fechaIsoEnd) {
      return res.status(400).json({ error: 'Formato de fecha inválido' });
    }

    const baseParams = [fechaIsoStart, fechaIsoEnd];
    const { sql: filterSQL, params: filterParams } = buildFilterClauses(filters, baseParams.length + 1);
    const allBaseParams = [...baseParams, ...filterParams];

    const whereClause = `fecha BETWEEN $1 AND $2${filterSQL}`;

    const dataRes = await pool.query(
      `SELECT exportadora, empacadora, fecha::text, producto, pais_destino
       FROM senasa_programacion
       WHERE ${whereClause}
       ORDER BY fecha DESC, exportadora ASC`,
      allBaseParams
    );

    const workbook = new ExcelJS.Workbook();
    const sheet = workbook.addWorksheet('Programación SENASA');

    sheet.columns = [
      { header: 'Exportadora', key: 'exportadora', width: 40 },
      { header: 'Empacadora', key: 'empacadora', width: 40 },
      { header: 'Fecha', key: 'fecha', width: 15 },
      { header: 'Producto', key: 'producto', width: 25 },
      { header: 'País Destino', key: 'pais_destino', width: 25 }
    ];

    // Aplicar estilos a la fila de cabecera
    const headerRow = sheet.getRow(1);
    headerRow.font = { name: 'Arial', size: 11, bold: true, color: { argb: 'FFFFFF' } };
    headerRow.fill = {
      type: 'pattern',
      pattern: 'solid',
      fgColor: { argb: '366092' }
    };
    headerRow.alignment = { vertical: 'middle', horizontal: 'center' };

    dataRes.rows.forEach(row => {
      sheet.addRow({
        exportadora: row.exportadora,
        empacadora: row.empacadora,
        fecha: isoToFecha(row.fecha),
        producto: row.producto,
        pais_destino: row.pais_destino
      });
    });

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="programacion_senasa_${fecha_inicio.replace(/\//g, '-')}_to_${fecha_fin.replace(/\//g, '-')}.xlsx"`);

    await workbook.xlsx.write(res);
    res.end();
  } catch (err) {
    console.error('[/senasa/export]', err.message);
    if (!res.headersSent) {
      res.status(500).json({ error: err.message });
    }
  }
});

export default router;
