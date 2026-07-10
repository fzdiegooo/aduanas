import axios from 'axios';
import * as cheerio from 'cheerio';
import pLimit from 'p-limit';
import * as pdfjs from 'pdfjs-dist/legacy/build/pdf.mjs';
import { parseFechaToISO } from './clasificacion.js';

const CALENDAR_URL = 'https://www.senasa.gob.pe/senasa/junin-2026/';
const MAX_WORKERS = 3;

const HEADERS = {
  'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'Accept-Language': 'es-PE,es;q=0.9,en;q=0.8',
};

const MONTH_MAP = {
  '01': ['enero', '01'],
  '02': ['febrero', '02'],
  '03': ['marzo', '03'],
  '04': ['abril', '04'],
  '05': ['mayo', '05'],
  '06': ['junio', '06'],
  '07': ['julio', '07'],
  '08': ['agosto', '08'],
  '09': ['setiembre', 'septiembre', '09'],
  '10': ['octubre', '10'],
  '11': ['noviembre', '11'],
  '12': ['diciembre', '12']
};

/**
 * Helper to identify covered year-month ranges.
 */
function getYearMonthsInRange(startDateStr, endDateStr) {
  const startIso = parseFechaToISO(startDateStr);
  const endIso = parseFechaToISO(endDateStr);
  if (!startIso || !endIso) return [];

  const start = new Date(startIso);
  const end = new Date(endIso);
  const result = [];

  const current = new Date(start.getUTCFullYear(), start.getUTCMonth(), 1);
  while (current <= end) {
    const yyyy = current.getFullYear();
    const mm = String(current.getMonth() + 1).padStart(2, '0');
    result.push({ yyyy, mm });
    current.setMonth(current.getMonth() + 1);
  }

  return result;
}

/**
 * Decides whether a PDF URL matches any target year-month.
 */
function urlMatchesYearMonths(url, yearMonths) {
  const urlLower = url.toLowerCase();
  for (const { yyyy, mm } of yearMonths) {
    const words = MONTH_MAP[mm] || [];
    const hasYear = urlLower.includes(String(yyyy));
    const hasMonth = urlLower.includes(`/${mm}/`) || words.some(w => urlLower.includes(w));
    if (hasYear && hasMonth) return true;
  }
  return false;
}

/**
 * Downloads a PDF and parses its contents.
 */
async function fetchAndParsePdf(pdfUrl) {
  try {
    console.log(`  [senasa] Descargando: ${pdfUrl}`);
    const response = await axios.get(pdfUrl, {
      headers: HEADERS,
      responseType: 'arraybuffer',
      timeout: 30_000,
    });
    const buffer = new Uint8Array(response.data);

    const loadingTask = pdfjs.getDocument({ data: buffer });
    const pdfDoc = await loadingTask.promise;
    const allRows = [];

    // Iterar sobre todas las páginas
    for (let pageNum = 1; pageNum <= pdfDoc.numPages; pageNum++) {
      const page = await pdfDoc.getPage(pageNum);
      const textContent = await page.getTextContent();

      const items = textContent.items
        .map(item => ({
          str: item.str.trim(),
          x: item.transform[4],
          y: item.transform[5],
        }))
        .filter(item => item.str.length > 0);

      // Encontrar la coordenada y de la cabecera
      const headerItem = items.find(it => it.str.includes('Expediente'));
      const headerY = headerItem ? headerItem.y : 510;

      const tableItems = items.filter(it => it.y < headerY - 5);

      // Identificar expedientes como anclas
      const expedientes = tableItems.filter(it => /^\d{12}$/.test(it.str));
      if (expedientes.length === 0) continue;

      const rowsMap = new Map();
      expedientes.forEach(exp => {
        rowsMap.set(exp.y, {
          yAnchor: exp.y,
          cols: Array.from({ length: 10 }, () => []),
        });
      });

      tableItems.forEach(item => {
        if (/^\d{12}$/.test(item.str)) return;

        let bestAnchor = null;
        let minDistance = Infinity;
        for (const yAnchor of rowsMap.keys()) {
          const dist = Math.abs(item.y - yAnchor);
          if (dist < minDistance) {
            minDistance = dist;
            bestAnchor = yAnchor;
          }
        }

        if (bestAnchor !== null && minDistance < 25) {
          const row = rowsMap.get(bestAnchor);
          const x = item.x;
          let colIdx = -1;

          if (x < 90) colIdx = 0;
          else if (x >= 90 && x < 220) colIdx = 1;
          else if (x >= 220 && x < 380) colIdx = 2;
          else if (x >= 380 && x < 450) colIdx = 3;
          else if (x >= 450 && x < 570) colIdx = 4;
          else if (x >= 570 && x < 640) colIdx = 5;
          else if (x >= 640 && x < 725) colIdx = 6;
          else if (x >= 725 && x < 775) colIdx = 7;
          else if (x >= 775 && x < 845) colIdx = 8;
          else colIdx = 9;

          row.cols[colIdx].push(item);
        }
      });

      // Construir las filas uniendo textos
      rowsMap.forEach(row => {
        const finalCols = row.cols.map(colItems => {
          return colItems
            .sort((a, b) => b.y - a.y)
            .map(it => it.str)
            .join(' ');
        });

        // Validar que al menos la fecha y exportadora estén presentes
        const rawFecha = finalCols[3];
        const fechaIso = parseFechaToISO(rawFecha);
        if (!fechaIso) return; // Si no tiene fecha válida, omitir

        allRows.push({
          exportadora: finalCols[1],
          empacadora: finalCols[2],
          fecha: fechaIso,
          producto: finalCols[5],
          pais_destino: finalCols[6],
        });
      });
    }

    return allRows;
  } catch (err) {
    console.error(`  [senasa] Error al procesar PDF ${pdfUrl}: ${err.message}`);
    return [];
  }
}

/**
 * Scrapea la programación diaria de SENASA Junín para el rango de fechas dado.
 * @returns {Promise<number>} Número total de registros insertados.
 */
export async function scrapeSenasa(fechaInicio, fechaFin, onBatchScraped) {
  // 1. Obtener los meses/años del rango solicitado
  const targetYearMonths = getYearMonthsInRange(fechaInicio, fechaFin);
  if (targetYearMonths.length === 0) return 0;

  console.log(`  [senasa] Consultando calendario principal...`);
  const resp = await axios.get(CALENDAR_URL, { headers: HEADERS });
  const $ = cheerio.load(resp.data);

  // 2. Extraer todas las URLs de archivos PDF
  const pdfLinks = [];
  $('a[href$=".pdf"]').each((_, el) => {
    const href = $(el).attr('href');
    if (href && !pdfLinks.includes(href)) {
      pdfLinks.push(href);
    }
  });

  // 3. Filtrar PDFs que coincidan con los meses cubiertos
  const matchedPdfs = pdfLinks.filter(url => urlMatchesYearMonths(url, targetYearMonths));
  console.log(`  [senasa] PDFs encontrados en calendario: ${pdfLinks.length}. Coincidentes con rango: ${matchedPdfs.length}`);

  if (matchedPdfs.length === 0) return 0;

  const limit = pLimit(MAX_WORKERS);
  let totalRows = 0;

  // 4. Descargar y parsear en paralelo
  const tasks = matchedPdfs.map(url => limit(async () => {
    const rows = await fetchAndParsePdf(url);
    if (rows.length > 0) {
      // Filtrar filas por el rango exacto de fechas solicitado
      const startIso = parseFechaToISO(fechaInicio);
      const endIso = parseFechaToISO(fechaFin);
      const filtered = rows.filter(r => r.fecha >= startIso && r.fecha <= endIso);
      if (filtered.length > 0) {
        await onBatchScraped(filtered);
        totalRows += filtered.length;
      }
    }
  }));

  await Promise.all(tasks);
  return totalRows;
}
