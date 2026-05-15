import * as cheerio from 'cheerio';
import pLimit from 'p-limit';
import { initSession, postHtml } from './http.js';
import { clasificarMaritimo, getISOWeek, parseFechaToISO } from './clasificacion.js';

const BASE_URL      = 'http://www.aduanet.gob.pe/cl-ad-itconsmanifiesto/manifiestoITS01Alias';
const URL_PAG_N1    = 'http://www.aduanet.gob.pe/cl-ad-itconsmanifiesto/ConsultaManifExpMarFecha.jsp';
const URL_PAG_N2    = 'http://www.aduanet.gob.pe/cl-ad-itconsmanifiesto/ConsultaManifExpMarDetalle.jsp';
const MAX_WORKERS   = 4;

function clean(text) {
  const s = String(text ?? '').replace(/\u00a0/g, ' ').trim();
  // El HTML de SUNAT repite el texto en dos nodos del mismo TD
  if (s.length > 0 && s.length % 2 === 0) {
    const half = s.length / 2;
    if (s.slice(0, half) === s.slice(half)) return s.slice(0, half);
  }
  return s;
}

async function fetchNivel3(cookies, dataPayload, regDet,
                            valManifiesto, valFechaZarpe, valNombreNave,
                            valDetalle, valPuerto, valBl, valFechaTransmision) {
  let html;
  try {
    html = await postHtml(
      `${BASE_URL}?accion=consultarConocimientoDetalle&reg=${regDet}`,
      dataPayload,
      cookies,
    );
  } catch (err) {
    console.error(`    [maritimo] Error nivel 3 BL=${valBl}: ${err.message}`);
    return [];
  }

  const $ = cheerio.load(html);
  const rows = [];

  $('tr.bg').each((_, tr) => {
    const tds = $(tr).find('td');
    if (tds.length < 7) return;

    const bultos = clean(tds.eq(0).text());
    if (!bultos || bultos.toLowerCase().includes('bultos')) return;

    const descripcion   = clean(tds.eq(6).text());
    const embarcador    = clean(tds.eq(3).text());
    const consignatario = clean(tds.eq(4).text());
    const clasif        = clasificarMaritimo(descripcion, embarcador, consignatario, valPuerto);
    const { semana, año } = getISOWeek(valFechaZarpe);

    rows.push({
      'Manifiesto':               valManifiesto,
      'Fecha de Zarpe':           valFechaZarpe,
      'Nombre de Nave':           valNombreNave,
      'Detalle':                  valDetalle,
      'Puerto':                   valPuerto,
      'BL':                       valBl,
      'Fecha de Transmisión':     valFechaTransmision,
      'Bultos':                   bultos,
      'Peso Bruto':               clean(tds.eq(1).text()),
      'Empaques':                 clean(tds.eq(2).text()),
      'Embarcador':               embarcador,
      'Consignatario':            consignatario,
      'Marcas y Números':         clean(tds.eq(5).text()),
      'Descripción de Mercadería': descripcion,
      'Envio':                    'maritimo',
      'Semana':                   semana,
      'Año':                      año,
      ...clasif,
      '_fecha_iso': parseFechaToISO(valFechaZarpe),
    });
  });

  return rows;
}

/**
 * Scrapea manifiestos marítimos SUNAT para el rango de fechas dado.
 * @returns {Promise<object[]>} Array de filas listas para insertar en DB.
 */
export async function scrapeMaritimo(fechaInicio, fechaFin) {
  const cookies = await initSession();

  const dataPayload = {
    fech_llega_ini: fechaInicio,
    fech_llega_fin: fechaFin,
    matr_nave:      '',
  };

  const limit   = pLimit(MAX_WORKERS);
  const allRows = [];

  // ── Nivel 1: lista de manifiestos (paginada) ──────────────────────────────
  let respManifHtml = await postHtml(`${BASE_URL}?accion=consultarManifiesto`, dataPayload, cookies);
  let paginaN1 = 1;

  while (true) {
    const $1  = cheerio.load(respManifHtml);
    const filas = $1('tr.bg').filter((_, tr) => $1(tr).find('a').length > 0);

    console.log(`  [maritimo] Página ${paginaN1}: ${filas.length} manifiestos`);

    // ── Nivel 2 por cada manifiesto ────────────────────────────────────────
    for (let iM = 0; iM < filas.length; iM++) {
      const tr       = filas.eq(iM);
      const cols     = tr.find('td');
      if (cols.length < 3) continue;

      const btnA = cols.eq(0).find('a');
      if (!btnA.length) continue;

      const valManifiesto  = clean(btnA.text());
      const valFechaZarpe  = clean(cols.eq(1).text());
      const valNombreNave  = clean(cols.eq(2).text());

      const matchReg = (btnA.attr('href') ?? '').match(/detalle\('(\d+)'\)/);
      if (!matchReg) continue;
      const regManif = matchReg[1];

      let respDetHtml = await postHtml(
        `${BASE_URL}?accion=consultarManifiestoDetalle&reg=${regManif}`,
        dataPayload,
        cookies,
      );
      let paginaN2 = 1;

      while (true) {
        const $2       = cheerio.load(respDetHtml);
        const filasDet = $2('tr.bg').filter((_, tr2) => $2(tr2).find('a').length > 0);

        // Recopilar trabajos de nivel 3
        const jobs = [];
        filasDet.each((_, tr2) => {
          const cols2 = $2(tr2).find('td');
          if (cols2.length < 14) return;

          const valPuerto          = clean(cols2.eq(0).text());
          const valBl              = clean(cols2.eq(2).text());
          const btnDet             = cols2.eq(3).find('a');
          if (!btnDet.length) return;
          const valDetalle         = clean(btnDet.text());
          const valFechaTransmision = clean(cols2.eq(13).text());

          const matchDet = (btnDet.attr('href') ?? '').match(/detalle\('(\d+)'\)/);
          if (!matchDet) return;
          const regDet = matchDet[1];

          jobs.push({ regDet, valDetalle, valPuerto, valBl, valFechaTransmision });
        });

        // ── Nivel 3 en paralelo ───────────────────────────────────────────
        if (jobs.length) {
          const tasks   = jobs.map(j => limit(() =>
            fetchNivel3(cookies, dataPayload, j.regDet,
              valManifiesto, valFechaZarpe, valNombreNave,
              j.valDetalle, j.valPuerto, j.valBl, j.valFechaTransmision),
          ));
          const results = await Promise.all(tasks);
          allRows.push(...results.flat());
        }

        // Paginación nivel 2
        const sigteN2 = $2('a').filter((_, a) => /siguiente/i.test($2(a).text()));
        if (!sigteN2.length) break;
        const mPagN2 = (sigteN2.attr('href') ?? '').match(/paginacion\((\d+),(\d+)\)/);
        if (!mPagN2) break;

        respDetHtml = await postHtml(URL_PAG_N2, {
          ...dataPayload,
          tamanioPagina: mPagN2[1],
          pagina:        mPagN2[2],
        }, cookies);
        paginaN2++;
      }

      console.log(`  [maritimo] [${iM + 1}/${filas.length}] ${valManifiesto} (acumulado: ${allRows.length} filas)`);
    }

    // Paginación nivel 1
    const sigteN1 = $1('a').filter((_, a) => /siguiente/i.test($1(a).text()));
    if (!sigteN1.length) break;
    const mPagN1 = (sigteN1.attr('href') ?? '').match(/paginacion\((\d+),(\d+)\)/);
    if (!mPagN1) break;

    respManifHtml = await postHtml(URL_PAG_N1, {
      ...dataPayload,
      tamanioPagina: mPagN1[1],
      pagina:        mPagN1[2],
    }, cookies);
    paginaN1++;
  }

  return allRows;
}
