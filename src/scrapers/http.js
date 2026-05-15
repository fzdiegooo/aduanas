import axios from 'axios';

const HEADERS = {
  'User-Agent':      'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36',
  'Content-Type':    'application/x-www-form-urlencoded',
  'Accept':          'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'Accept-Language': 'es-PE,es;q=0.9,en;q=0.8',
};

const BASE_URL = 'http://www.aduanet.gob.pe/cl-ad-itconsmanifiesto/manifiestoITS01Alias';

/**
 * Inicializa la sesión SUNAT y devuelve el string de cookies.
 */
export async function initSession() {
  const resp = await axios.get(BASE_URL, {
    params:  { accion: 'cargaConsultaManifiesto', tipoConsulta: 'fechaSalida' },
    headers: HEADERS,
    timeout: 30_000,
  });
  const setCookies = resp.headers['set-cookie'] ?? [];
  return setCookies.map(c => c.split(';')[0]).join('; ');
}

/**
 * POST con body url-encoded; devuelve el HTML decodificado en latin1.
 */
export async function postHtml(url, body, cookies, referer = BASE_URL) {
  const resp = await axios.post(url, new URLSearchParams(body).toString(), {
    headers: { ...HEADERS, Referer: referer, Cookie: cookies },
    responseType: 'arraybuffer',
    timeout: 30_000,
  });
  return Buffer.from(resp.data).toString('latin1');
}
