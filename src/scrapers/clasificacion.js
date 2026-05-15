// ============================================================
//  Clasificación de productos — compartido aereo + marítimo
// ============================================================

// ── Producto ─────────────────────────────────────────────────────────────────

export const PALABRAS_CLAVE_PRODUCTOS = {
  'Jengibre fresco': ['jengibre', 'ginger', 'organic ginger', 'fresh organic ginger', 'organic fresh ginger'],
  'Cúrcuma':   ['turmeric', 'curcuma', 'cúrcuma', 'organic turmeric'],
  'Palta':     ['palta', 'paltas', 'avocado', 'aguacate', 'aguacates'],
  'Arándano':  ['arándano', 'arándanos', 'blueberries'],
  'Naranja':   ['naranja', 'naranjas', 'orange', 'oranges'],
  'Café':      ['cafe', 'café', 'cafes', 'coffee'],
  'Cacao':     ['cacao', 'cocoa'],
  'Uva':       ['uva', 'uvas', 'grape', 'grapes'],
  'Mango':     ['mango', 'mangos'],
  'Achiote':   ['achiote', 'annatto'],
  'Ajo':       ['ajo', 'garlic'],
  'Limón':     ['limon', 'lemon', 'limón', 'lemons'],
  'Esparrago': ['esparrago', 'esparragos'],
  'Fresas':    ['fresa', 'strawberries', 'fresas'],
  'Quinua':    ['quinua', 'quinoa'],
  'Mandarina': ['mandarina', 'clementinas', 'clementine'],
  'Menestras': ['beans', 'lentejas', 'menestras', 'frejoles', 'alberja'],
  'Cebolla':   ['cebolla', 'onion', 'onions', 'cebollas'],
};

export const EXPORTADORES_ESPECIFICOS = [
  'vancard', 'elisur', 'fruitxchange', 'hamillton', 'la grama', 'fruticola selva',
  'namaskar', 'masojo', 'anawi', 'jungle fresh organic', 'la campiña',
  'jch exportaciones', 'blue pacific oils', 'aseptic peruvian fruit',
];

// Aéreo: consignatario → país
export const IMPORTADORES_PAISES_AEREO = {
  'AXARFRUIT':                           'ESPAÑA',
  'JALHUCA EXPLOTACIONES SL':            'ESPAÑA',
  'PIMENTON Y DERIVADOS S.L CALLE CATA': 'ESPAÑA',
  'CASCONE':                             'PAISES BAJOS',
  'FRUCHTHANSA':                         'PAISES BAJOS',
  'IPOKI BV':                            'PAISES BAJOS',
  'NATURE S PRODUCE SP Z':               'PAISES BAJOS',
  'NFG New Fruit Group':                 'PAISES BAJOS',
  'VISION INTERNATIONAL B.V.':           'PAISES BAJOS',
  'AGROFAIR':                            'ITALIA',
  'ANAWI USA':                           'ESTADOS UNIDOS',
  'DELINA':                              'ESTADOS UNIDOS',
  'ECORIPE TROPICALS':                   'ESTADOS UNIDOS',
  'GLOBAL FARMS ENTERPRISES':            'ESTADOS UNIDOS',
  'HEATH AND LEJEUNE':                   'ESTADOS UNIDOS',
  'I LOVE PRODUCE':                      'ESTADOS UNIDOS',
  'INTERNATIONAL SPECIALTY PRODUCE':     'ESTADOS UNIDOS',
  'IPOKI PRODUCE LLC':                   'ESTADOS UNIDOS',
  'J Y C TROPICALS INC':                 'ESTADOS UNIDOS',
  'JLZ PRODUCE':                         'ESTADOS UNIDOS',
  'SUNDINE PRODUCE INC':                 'ESTADOS UNIDOS',
  'TRINITY DISTRIBUTION INC':            'ESTADOS UNIDOS',
  'UREN NORTH AMERICA LLC':              'ESTADOS UNIDOS',
  'VIVA TIERRA ORGANIC INC':             'ESTADOS UNIDOS',
  'THOMAS FRESH INC':                    'CANADÁ',
  'SOL FRUIT IMPORTS LTD':              'REINO UNIDO',
};

// Marítimo: importadores para detección de jengibre
export const IMPORTADORES_ESPECIFICOS_MARITIMO = [
  'GLOBAL FARMS ENTERPRISES', 'IPOKI', 'TRINITY DISTRIBUTION INC',
  'VISION INTERNATIONAL', 'MIAMI AGRO IMPORT', 'FRESH DIRECT',
  'HEATH AND LEJEUNE', 'VIVA TIERRA ORGANIC', 'DELINA', 'I LOVE PRODUCE',
  'ECORIPE TROPICALS', 'JLZ PRODUCE',
];

// Marítimo: código de puerto (2 chars) → país
export const PUERTOS_PAISES = {
  'CN': 'CHINA',    'KR': 'KOREA SUR',  'CL': 'CHILE',   'BO': 'BOLIVIA',
  'NZ': 'NUEVA ZELANDA', 'JP': 'JAPON', 'US': 'ESTADOS UNIDOS', 'CO': 'COLOMBIA',
  'CR': 'COSTA RICA', 'EC': 'ECUADOR',  'PA': 'PANAMA',  'DE': 'ALEMANIA',
  'BE': 'BELGICA',  'CA': 'CANADA',     'TW': 'TAIWAN',  'TH': 'TAILANDIA',
  'HK': 'HONG KONG', 'MY': 'MALASIA',  'AU': 'SYDNEY',  'BR': 'BRAZIL',
  'MX': 'MEXICO',   'GB': 'REINO UNIDO', 'NL': 'PAISES BAJOS',
  'DO': 'REPUBLICA DOMINICANA', 'JM': 'JAMAICA', 'IT': 'ITALIA',
  'AW': 'ARUBA',    'ES': 'ESPAÑA',     'PR': 'PUERTO RICO', 'NI': 'NICARAGUA',
  'SV': 'EL SALVADOR', 'CU': 'CUBA',   'TT': 'TRINIDAD Y TOBAGO',
  'TR': 'TURQUIA',  'GR': 'GRECIA',    'VE': 'VENEZUELA', 'EE': 'ESTONIA',
  'PY': 'PARAGUAY', 'IN': 'INDIA',     'FR': 'FRANCIA',  'RU': 'RUSIA',
  'ID': 'INDONESIA', 'DZ': 'ALGERIA',  'PL': 'POLONIA',  'NO': 'NORUEGA',
};

// Marítimo: código de puerto (últimos 3 chars) → ciudad
export const CIUDAD_DESTINO = {
  'JEA': 'Dubai',          'SJO': 'Antigua y Barbuda', 'BUE': 'Buenos Aires',
  'ROS': 'Rosario',        'MEL': 'Melbourne',          'SYD': 'Sidney',
  'TSV': 'Townsville',     'ORJ': 'Oranjestad',         'BGI': 'Bridgetown',
  'ANR': 'Antwerp',        'CBB': 'Cochabamba',         'LPB': 'La Paz',
  'SRZ': 'Santa Cruz',     'IOA': 'Itapoa',             'NVT': 'Navegantes',
  'PEC': 'Pecem',          'PNG': 'Paranagua',          'RIG': 'Río Grande',
  'RIO': 'Río de Janeiro', 'SSA': 'Salvador',           'SSZ': 'Santos',
  'SUA': 'Suape',          'HAL': 'Halifax',            'MTR': 'Montreal',
  'TOR': 'Toronto',        'VAN': 'Vancouver',          'ARI': 'Arica',
  'IQQ': 'Iquique',        'MJS': 'Mejillones',         'SAI': 'San Antonio',
  'SVE': 'San Vicente',    'VAP': 'Valparaiso',         'CAN': 'Guangzhou',
  'CSX': 'Changsha',       'DFG': 'Dafeng',             'DLC': 'Dalian',
  'FAN': 'Fangcheng',      'FOC': 'Fuzhou',             'HUA': 'Shangai',
  'LYG': 'Lianyungang',    'NDE': 'Ningde',             'NGB': 'Ningbo',
  'NSA': 'Nansha',         'QZH': 'Qinzhou',            'RZH': 'Rizhao',
  'SHA': 'Shanghai',       'SHK': 'Shekou',             'SJQ': 'Sanshui',
  'TAO': 'Qingdao',        'TOL': 'Tongling',           'TXG': 'Tianjin Xingang',
  'WEI': 'Weihai',         'XMN': 'Puerto Xiamen',      'YTN': 'Yantian',
  'ZJG': 'Zhangjiagang',   'ZOS': 'Zhoushan',           'BAQ': 'Baranquilla',
  'BUN': 'Buenaventura',   'CTG': 'Cartagena',          'CAL': 'Caldera',
  'HER': 'Heredia',        'LIO': 'Limón',              'PTC': 'Caldera',
  'WIL': 'Willemstad',     'PRG': 'Praga',              'BRE': 'Bremen',
  'HAM': 'Hamburgo',       'AAR': 'Aarhus',             'CAU': 'Caucedo',
  'GYE': 'Guayaquil',      'PSJ': 'Posorja',            'TLL': 'Tallinn',
  'AGP': 'Malaga',         'ALG': 'Algeciras',          'BCN': 'Barcelona',
  'BIO': 'Bilbao',         'GIJ': 'Gijon',              'VGO': 'Vigo',
  'VLC': 'Valencia',       'HEL': 'Helsinki',           'FOS': 'Fos sur Mer',
  'LEH': 'Le Havre',       'LGP': 'Thames',             'LON': 'London',
  'SOU': 'Southampton',    'SKG': 'Thessaloniki',        'PRQ': 'Puerto Quetzal',
  'GEO': 'Georgetown',     'HKG': 'Hong Kong',          'PCR': 'Puerto Cortes',
  'JKT': 'Yakarta',        'PJG': 'Panjang',            'SRG': 'Semarang',
  'SUB': 'Subraya',        'HFA': 'Haifa',              'ENR': 'Ennore',
  'JAI': 'Injai',          'MAA': 'Chennai',            'MUN': 'Mundra',
  'LIV': 'Livorno',        'SAL': 'Salerno',            'VDL': 'Vado Ligure',
  'KIN': 'Kingston',       'MOJ': 'Moji',               'NGO': 'Nagoya',
  'OSA': 'Osaka',          'SBS': 'Shibushi',           'TYO': 'Tokyo',
  'UKB': 'Kobe',           'YOK': 'Yokohama',           'ICN': 'Incheon',
  'PUS': 'Busan',          'USN': 'Ulsan',              'BEY': 'Beirut',
  'KLJ': 'Klaipeda',       'ATM': 'Altamira',           'ESE': 'Ensenada',
  'LZC': 'Lazaro Cardenas', 'VER': 'Veracruz',          'ZLO': 'Manzanillo',
  'AMS': 'Amsterdam',      'RTM': 'Rotterdam',           'HAU': 'Haroy',
  'KRS': 'Kristiansand',   'OSL': 'Oslo',               'BLB': 'Balboa',
  'MIT': 'Manzanillo',     'ROD': 'Rodman',             'MNL': 'Manila',
  'GDN': 'Gdansk',         'GDY': 'Gdynia',             'SJU': 'San Juan',
  'LIS': 'Lisboa',         'SIE': 'Sines',              'ASU': 'Asuncion',
  'LED': 'Saint Petersburg', 'JED': 'Jeddah',           'GOT': 'Goteborg',
  'KOP': 'Koper',          'AQJ': 'Acajutla',           'LCH': 'Laem Chabang',
  'MER': 'Mersin',         'KEL': 'Keelung',            'KHH': 'Kaohsiung',
  'ILK': 'Chornomorsk',    'ATL': 'Atlanta',            'BAL': 'Baltimore',
  'CHI': 'Chicago',        'CHS': 'Charleston',         'EWR': 'Newark',
  'HNL': 'Honolulu',       'HOU': 'Houston',            'IND': 'Indianapolis',
  'JAX': 'Jacksonville',   'LAX': 'Los Angeles',        'LGB': 'Long Beach',
  'MEM': 'Memphis',        'MSY': 'New Orleans',        'NTD': 'Port Hueneme',
  'NYC': 'New York',       'OAK': 'Oakland',            'ORF': 'Norfolk',
  'PEF': 'Everglades',     'PHL': 'Philadelphia',       'SAV': 'Savannah',
  'SEA': 'Seattle',        'SLC': 'Salt Lake',          'STL': 'St Louis',
  'MVD': 'Montevideo',     'LAG': 'La Guaira',          'MAR': 'Maracaibo',
  'PBL': 'Puerto Cabello', 'HPH': 'Haiphong',           'DUR': 'Durban',
  'MIA': 'Miami',          'BOS': 'Boston',             'CRQ': 'Charlotte',
  'DET': 'Detroit',        'ORH': 'Worcester',          'TIW': 'Tacoma',
  'XMX': 'Southport',
};

// País → continente (combinado aereo + marítimo)
export const CONTINENTES = {
  'CHINA': 'Asia',    'KOREA SUR': 'Asia',  'CHILE': 'América',  'BOLIVIA': 'América',
  'NUEVA ZELANDA': 'Oceanía', 'JAPON': 'Asia', 'ESTADOS UNIDOS': 'América',
  'COLOMBIA': 'América', 'COSTA RICA': 'América', 'ECUADOR': 'América',
  'PANAMA': 'América', 'ALEMANIA': 'Europa', 'BELGICA': 'Europa', 'CANADA': 'América',
  'CANADÁ': 'América', 'TAIWAN': 'Asia', 'TAILANDIA': 'Asia', 'HONG KONG': 'Asia',
  'MALASIA': 'Asia', 'SYDNEY': 'Oceanía', 'BRAZIL': 'América', 'MEXICO': 'América',
  'REINO UNIDO': 'Europa', 'PAISES BAJOS': 'Europa', 'REPUBLICA DOMINICANA': 'América',
  'ITALIA': 'Europa', 'ESPAÑA': 'Europa', 'FRANCIA': 'Europa', 'RUSIA': 'Europa',
  'INDIA': 'Asia', 'AUSTRALIA': 'Oceanía', 'NORUEGA': 'Europa', 'POLONIA': 'Europa',
  'ESTONIA': 'Europa', 'TURQUIA': 'Asia', 'GRECIA': 'Europa', 'INDONESIA': 'Asia',
  'ALGERIA': 'África', 'JAMAICA': 'América', 'VENEZUELA': 'América', 'PARAGUAY': 'América',
  'ARUBA': 'América', 'PUERTO RICO': 'América', 'NICARAGUA': 'América',
  'EL SALVADOR': 'América', 'CUBA': 'América', 'TRINIDAD Y TOBAGO': 'América',
};

// ── Helpers ───────────────────────────────────────────────────────────────────

/**
 * Calcula número de semana ISO y año ISO a partir de "dd/mm/yyyy".
 */
export function getISOWeek(fechaDDMMYYYY) {
  if (!fechaDDMMYYYY) return { semana: null, año: null };
  const parts = fechaDDMMYYYY.trim().split('/');
  if (parts.length !== 3) return { semana: null, año: null };
  const [d, m, y] = parts.map(Number);
  if (!d || !m || !y) return { semana: null, año: null };

  // Algoritmo canónico ISO 8601: el jueves de la semana determina el año
  const date = new Date(Date.UTC(y, m - 1, d));
  const dayNum = date.getUTCDay() || 7; // 1=Lun … 7=Dom
  date.setUTCDate(date.getUTCDate() + 4 - dayNum); // mover al jueves
  const yearStart = new Date(Date.UTC(date.getUTCFullYear(), 0, 1));
  const weekNum = Math.ceil((((date - yearStart) / 86_400_000) + 1) / 7);
  return { semana: weekNum, año: date.getUTCFullYear() };
}

/**
 * Convierte "dd/mm/yyyy" → "YYYY-MM-DD" para PostgreSQL DATE.
 */
export function parseFechaToISO(fechaDDMMYYYY) {
  if (!fechaDDMMYYYY) return null;
  const parts = fechaDDMMYYYY.trim().split('/');
  if (parts.length !== 3) return null;
  const [d, m, y] = parts;
  if (!d || !m || !y) return null;
  return `${y.padStart(4, '0')}-${m.padStart(2, '0')}-${d.padStart(2, '0')}`;
}

// ── Lógica de clasificación ───────────────────────────────────────────────────

function identificarProducto(descripcion, embarcador, consignatario, importadores) {
  const desc = (descripcion ?? '').toLowerCase();
  for (const [producto, palabras] of Object.entries(PALABRAS_CLAVE_PRODUCTOS)) {
    if (palabras.some(p => desc.includes(p.toLowerCase()))) return producto;
  }
  if (typeof embarcador === 'string') {
    for (const exp of EXPORTADORES_ESPECIFICOS) {
      if (embarcador.toLowerCase().includes(exp)) return '¿Jengibre?';
    }
  }
  if (typeof consignatario === 'string') {
    for (const imp of importadores) {
      if (consignatario.toLowerCase().includes(imp.toLowerCase())) return '¿Jengibre?';
    }
  }
  return 'No asociado';
}

function segmentarJengibre(descripcion, producto) {
  if (producto === 'Jengibre fresco' && typeof descripcion === 'string') {
    const d = descripcion.toLowerCase();
    if (d.includes('juice') || d.includes('jugo'))                          return 'Jugo de jengibre';
    if (d.includes('bags') || d.includes('sacos') || d.includes('bolsas')) return 'Jengibre deshidratado';
  }
  return producto;
}

/**
 * Clasificación para manifiestos aéreos.
 * Pais y Continente se deducen del consignatario.
 */
export function clasificarAereo(descripcion, embarcador, consignatario) {
  const importadores = Object.keys(IMPORTADORES_PAISES_AEREO);
  const productoBase = identificarProducto(descripcion, embarcador, consignatario, importadores);
  const producto = segmentarJengibre(descripcion, productoBase);

  let pais = '';
  if (typeof consignatario === 'string') {
    for (const [imp, p] of Object.entries(IMPORTADORES_PAISES_AEREO)) {
      if (consignatario.toLowerCase().includes(imp.toLowerCase())) { pais = p; break; }
    }
  }

  return {
    'Producto':      producto,
    'Tipo':          '',
    'Pais':          pais,
    'Ciudad destino': '',
    'Continente':    CONTINENTES[pais] ?? '',
  };
}

/**
 * Clasificación para manifiestos marítimos.
 * Pais y Ciudad destino se deducen del código de puerto.
 */
export function clasificarMaritimo(descripcion, embarcador, consignatario, puerto) {
  const productoBase = identificarProducto(descripcion, embarcador, consignatario, IMPORTADORES_ESPECIFICOS_MARITIMO);
  const producto = segmentarJengibre(descripcion, productoBase);

  const p = String(puerto ?? '');
  const pais         = PUERTOS_PAISES[p.slice(0, 2).toUpperCase()]  ?? 'No asociado';
  const ciudadDestino = CIUDAD_DESTINO[p.slice(-3).toUpperCase()]    ?? 'No asociado';

  return {
    'Producto':       producto,
    'Tipo':           '',
    'Pais':           pais,
    'Ciudad destino': ciudadDestino,
    'Continente':     CONTINENTES[pais] ?? 'No asociado',
  };
}
