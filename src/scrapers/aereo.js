import * as cheerio from 'cheerio';
import pLimit from 'p-limit';
import { initSession, postHtml } from './http.js';
import { clasificarAereo, getISOWeek, parseFechaToISO } from './clasificacion.js';

const BASE_URL  = 'http://www.aduanet.gob.pe/cl-ad-itconsmanifiesto/manifiestoITS01Alias';
const BASE_HOST = 'http://www.aduanet.gob.pe';
const MAX_WORKERS  = 4;
const MAX_PAGES_N1 = 200;
const MAX_PAGES_N2 = 100;

function clean(text) {
  const s = String(text ?? '').replace(/\u00a0/g, ' ').trim();
  // El HTML de SUNAT repite el texto en dos nodos del mismo TD
  if (s.length > 0 && s.length % 2 === 0) {
    const half = s.length / 2;
    if (s.slice(0, half) === s.slice(half)) return s.slice(0, half);
  }
  return s;
}

async function fetchNivel3(cookies, mani, guia, annoFull) {
  let html;
  try {
    html = await postHtml(BASE_URL, {
      accion:       'consultarDetalleConocimientoEmbarqueExportacion',
      CG_cadu:      '235',
      CMc2_Anno:    annoFull,
      CMc2_Numero:  mani.numero,
      CMc2_numcon:  guia.numcon,
      CMc2_numconm: guia.numconm,
      CMc2_NumDet:  guia.numdet,
      CMc2_TipM:    guia.tipomani,
      tipo_archivo: '',
      reporte:      'ExpAerGuia',
      backPage:     'ConsulManifExpAerFechList',
    }, cookies);
  } catch (err) {
    console.error(`    [aereo] Error nivel 3 BL=${guia.bl}: ${err.message}`);
    return [];
  }

  const $ = cheerio.load(html);
  const rows = [];

  $('tr.bg').each((_, tr) => {
    const tds = $(tr).children('td');
    if (tds.length !== 7) return;

    const bultos = clean(tds.eq(0).text());
    if (!bultos) return;

    let embarcador = clean(tds.eq(3).text());
    if (embarcador.includes('Ley 29733')) embarcador = '';

    const descripcion   = clean(tds.eq(6).text());
    const consignatario = clean(tds.eq(4).text());
    const clasif        = clasificarAereo(descripcion, embarcador, consignatario);
    const { semana, año } = getISOWeek(mani.fecha_salida);

    rows.push({
      'Manifiesto':               mani.texto,
      'Fecha de Zarpe':           mani.fecha_salida,
      'Nombre de Nave':           mani.aerolinea,
      'Detalle':                  mani.vuelo,
      'Puerto':                   mani.puerto,
      'BL':                       guia.bl,
      'Fecha de Transmisión':     guia.fecha_transmision,
      'Bultos':                   bultos,
      'Peso Bruto':               clean(tds.eq(1).text()),
      'Empaques':                 clean(tds.eq(2).text()),
      'Embarcador':               embarcador,
      'Consignatario':            consignatario,
      'Marcas y Números':         clean(tds.eq(5).text()),
      'Descripción de Mercadería': descripcion,
      'Envio':                    'aereo',
      'Semana':                   semana,
      'Año':                      año,
      ...clasif,
      '_fecha_iso': parseFechaToISO(mani.fecha_salida),
    });
  });

  return rows;
}

/**
 * Scrapea manifiestos aéreos SUNAT para el rango de fechas dado.
 * @returns {Promise<object[]>} Array de filas listas para insertar en DB.
 */
export async function scrapeAereo(fechaInicio, fechaFin) {
  const cookies = await initSession();

  // ── Nivel 1: lista de manifiestos (paginada) ──────────────────────────────
  const manifiestos = [];
  const seenMani    = new Set();
  let paginaN1 = 1;

  while (paginaN1 <= MAX_PAGES_N1) {
    let html1;
    if (paginaN1 === 1) {
      html1 = await postHtml(BASE_URL, {
        accion:       'consultaManifiesto',
        fec_inicio:   fechaInicio,
        fec_fin:      fechaFin,
        cod_terminal: '0000',
      }, cookies);
    } else {
      html1 = await postHtml(
        `${BASE_HOST}/cl-ad-itconsmanifiesto/ConsulManifExpAerFechList.jsp`,
        { tamanioPagina: '10', pagina: String(paginaN1 - 1) },
        cookies,
      );
    }

    const $1 = cheerio.load(html1);
    let nuevos = 0;

    $1('a[href*="jsDetalle2"]').each((_, link) => {
      const href = $1(link).attr('href') ?? '';
      const m = href.match(/jsDetalle2\('(\d+)','(\d+)'\)/);
      if (!m) return;
      const [, anio, numero] = m;
      const clave = `${anio}|${numero}`;
      if (seenMani.has(clave)) return;
      seenMani.add(clave);

      const tr  = $1(link).closest('tr');
      const tds = tr.find('td');
      if (tds.length < 6) return;

      manifiestos.push({
        texto:        clean($1(link).text()),
        anio,
        numero,
        fecha_salida: clean(tds.eq(1).text()),
        aerolinea:    clean(tds.eq(3).text()),
        puerto:       clean(tds.eq(4).text()),
        vuelo:        clean(tds.eq(5).text()),
      });
      nuevos++;
    });

    if (nuevos === 0) break;
    const mc = html1.match(/(\d+)\s+a\s+(\d+)\s+de\s+(\d+)/);
    if (mc && parseInt(mc[2]) < parseInt(mc[3])) {
      paginaN1++;
    } else {
      break;
    }
  }

  console.log(`  [aereo] Manifiestos encontrados: ${manifiestos.length}`);

  const limit   = pLimit(MAX_WORKERS);
  const allRows = [];

  for (let idx = 0; idx < manifiestos.length; idx++) {
    const mani     = manifiestos[idx];
    const annoFull = String(2000 + parseInt(mani.anio)); // "26" → "2026"

    // ── Nivel 2: guías del manifiesto (paginadas) ─────────────────────────
    const guias    = [];
    const seenBls  = new Set();
    let paginaDet  = 1;

    while (paginaDet <= MAX_PAGES_N2) {
      let html2;
      if (paginaDet === 1) {
        html2 = await postHtml(BASE_URL, {
          accion:        'consultaManifiestoGuia',
          CMc1_Anno:     `00${mani.anio}`,
          CMc1_Numero:   mani.numero,
          CMc1_Terminal: '0000',
          viat:          '4',
          CG_cadu:       '235',
        }, cookies);
      } else {
        html2 = await postHtml(
          `${BASE_HOST}/cl-ad-itconsmanifiesto/ConsulManifExpAerGuia.jsp`,
          { tamanioPagina: '10', pagina: String(paginaDet - 1) },
          cookies,
        );
      }

      const $2 = cheerio.load(html2);
      const guiasPag = [];

      $2('a[href*="jsDetalleD"]').each((_, linkD) => {
        const href = $2(linkD).attr('href') ?? '';
        const md = href.match(/jsDetalleD\('([^']+)','([^']+)','([^']*)','([^']+)'\)/);
        if (!md) return;
        const [, numdet, numcon, tipomani, numconm] = md;

        const tr2  = $2(linkD).closest('tr');
        const tds2 = tr2.find('td');
        if (tds2.length < 13) return;

        const blA = tds2.eq(0).find('a');
        const bl  = blA.length ? clean(blA.text()) : numcon.trim();
        const ft  = clean(tds2.eq(12).text());

        const clave = `${bl.trim()}|${numdet.trim()}`;
        if (seenBls.has(clave)) return;
        seenBls.add(clave);

        guiasPag.push({ bl, numdet, numcon, numconm, tipomani, fecha_transmision: ft });
      });

      if (!guiasPag.length) break;
      guias.push(...guiasPag);

      const mc = html2.match(/(\d+)\s+a\s+(\d+)\s+de\s+(\d+)/);
      if (mc && parseInt(mc[2]) < parseInt(mc[3])) {
        paginaDet++;
      } else {
        break;
      }
    }

    if (!guias.length) continue;

    // ── Nivel 3: detalles de mercadería en paralelo ───────────────────────
    const tasks     = guias.map(guia => limit(() => fetchNivel3(cookies, mani, guia, annoFull)));
    const results   = await Promise.all(tasks);
    const filasMani = results.flat();
    allRows.push(...filasMani);

    console.log(`  [aereo] [${idx + 1}/${manifiestos.length}] ${mani.texto}: ${guias.length} guías → ${filasMani.length} filas`);
  }

  return allRows;
}
