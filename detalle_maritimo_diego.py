import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl.styles import Alignment

URL_BASE = "http://www.aduanet.gob.pe/cl-ad-itconsmanifiesto/manifiestoITS01Alias"
URL_PAG_NIVEL_1 = "http://www.aduanet.gob.pe/cl-ad-itconsmanifiesto/ConsultaManifExpMarFecha.jsp"
URL_PAG_NIVEL_2 = "http://www.aduanet.gob.pe/cl-ad-itconsmanifiesto/ConsultaManifExpMarDetalle.jsp"

MAX_WORKERS = 4  # peticiones de nivel 3 en paralelo (ajusta según tolerancia del servidor)

# Columnas EXACTAS del Excel de salida (14)
COLUMNAS_EXCEL = [
    "Manifiesto", "Fecha de Zarpe", "Nombre de Nave", "Detalle", "Puerto", "B/L",
    "Fecha de Transmisión del Documento", "Bultos", "Peso Bruto", "Empaques",
    "Embarcador", "Consignatario", "Marcas y Números", "Descripción de Mercadería"
]

# ============================================================
# CLASIFICACIÓN DE PRODUCTOS
# ============================================================
PALABRAS_CLAVE_PRODUCTOS = {
    "Jengibre fresco": ["jengibre", "ginger", "organic ginger", "fresh organic ginger", "organic fresh ginger"],
    "Cúrcuma": ["turmeric", "curcuma", "cúrcuma", "organic turmeric"],
    "Palta": ["palta", "paltas", "avocado", "aguacate", "aguacates"],
    "Arándano": ["arándano", "arándanos", "blueberries"],
    "Naranja": ["naranja", "naranjas", "orange", "oranges"],
    "Café": ["cafe", "café", "cafes", "coffee"],
    "Cacao": ["cacao", "cocoa"],
    "Uva": ["uva", "uvas", "grape", "grapes"],
    "Mango": ["mango", "mangos"],
    "Achiote": ["achiote", "annatto"],
    "Ajo": ["ajo", "garlic"],
    "Limón": ["limon", "lemon", "limón", "lemons"],
    "Esparrago": ["esparrago", "esparragos"],
    "Fresas": ["fresa", "strawberries", "fresas"],
    "Quinua": ["quinua", "quinoa"],
    "Mandarina": ["mandarina", "clementinas", "clementine"],
    "Menestras": ["beans", "lentejas", "menestras", "frejoles", "alberja"],
    "Cebolla": ["cebolla", "onion", "onions", "cebollas"]
}

EXPORTADORES_ESPECIFICOS = [
    "vancard", "elisur", "fruitxchange", "hamillton", "la grama", "fruticola selva",
    "namaskar", "masojo", "anawi", "jungle fresh organic", "la campiña",
    "jch exportaciones", "blue pacific oils", "aseptic peruvian fruit"
]

IMPORTADORES_ESPECIFICOS = [
    "GLOBAL FARMS ENTERPRISES", "IPOKI", "TRINITY DISTRIBUTION INC",
    "VISION INTERNATIONAL", "MIAMI AGRO IMPORT", "FRESH DIRECT",
    "HEATH AND LEJEUNE", "VIVA TIERRA ORGANIC", "DELINA", "I LOVE PRODUCE",
    "ECORIPE TROPICALS", "JLZ PRODUCE"
]

PUERTOS_PAISES = {
    "CN": "CHINA", "KR": "KOREA SUR", "CL": "CHILE", "BO": "BOLIVIA", "NZ": "NUEVA ZELANDA",
    "JP": "JAPON", "US": "ESTADOS UNIDOS", "CO": "COLOMBIA", "CR": "COSTA RICA", "EC": "ECUADOR",
    "PA": "PANAMA", "DE": "ALEMANIA", "BE": "BELGICA", "CA": "CANADA", "TW": "TAIWAN",
    "TH": "TAILANDIA", "HK": "HONG KONG", "MY": "MALASIA", "AU": "SYDNEY", "BR": "BRAZIL",
    "MX": "MEXICO", "GB": "REINO UNIDO", "NL": "PAISES BAJOS", "DO": "REPUBLICA DOMINICANA",
    "JM": "JAMAICA", "IT": "ITALIA", "AW": "ARUBA", "ES": "ESPAÑA", "PR": "PUERTO RICO",
    "NI": "NICARAGUA", "SV": "EL SALVADOR", "CU": "CUBA", "TT": "TRINIDAD Y TOBAGO",
    "TR": "TURQUIA", "GR": "GRECIA", "VE": "VENEZUELA", "EE": "ESTONIA", "PY": "PARAGUAY",
    "IN": "INDIA", "FR": "FRANCIA", "RU": "RUSIA", "ID": "INDONESIA", "DZ": "ALGERIA", "PL": "POLONIA",
    "NO": "NORUEGA"
}

CIUDAD_DESTINO = {
    "JEA": "Dubai", "SJO": "Antigua y Barbuda", "BUE": "Buenos Aires", "ROS": "Rosario", "MEL": "Melbourne",
    "SYD": "Sidney", "TSV": "Townsville", "ORJ": "Oranjestad", "BGI": "Bridgetown", "ANR": "Antwerp",
    "CBB": "Cochabamba", "LPB": "La Paz", "SRZ": "Santa Cruz", "IOA": "Itapoa", "NVT": "Navegantes", "PEC": "Pecem",
    "PNG": "Paranagua", "RIG": "Río Grande", "RIO": "Río de Janeiro", "SSA": "Salvador", "SSZ": "Santos",
    "SUA": "Suape", "HAL": "Halifax", "MTR": "Montreal", "TOR": "Toronto", "VAN": "Vancouver",
    "ARI": "Arica", "IQQ": "Iquique", "MJS": "Mejillones", "SAI": "San Antonio", "SVE": "San Vicente",
    "VAP": "Valparaiso", "CAN": "Guangzhou", "CSX": "Changsha", "DFG": "Dafeng", "DLC": "Dalian",
    "FAN": "Fangcheng", "FOC": "Fuzhou", "HUA": "Shangai", "LYG": "Lianyungang", "NDE": "Ningde",
    "NGB": "Ningbo", "NSA": "Nansha", "QZH": "Qinzhou", "RZH": "Rizhao", "SHA": "Shanghai",
    "SHK": "Shekou", "SJQ": "Sanshui", "TAO": "Qingdao", "TOL": "Tongling", "TXG": "Tianjin Xingang",
    "WEI": "Weihai", "XMN": "Puerto Xiamen", "YTN": "Yantian", "ZJG": "Zhangjiagang", "ZOS": "Zhoushan",
    "BAQ": "Baranquilla", "BUN": "Buenaventura", "CTG": "Cartagena", "CAL": "Caldera", "HER": "Heredia",
    "LIO": "Limón", "PTC": "Caldera", "WIL": "Willemstad", "PRG": "Praga", "BRE": "Bremen",
    "HAM": "Hamburgo", "AAR": "Aarhus", "CAU": "Caucedo", "GYE": "Guayaquil", "PSJ": "Posorja",
    "TLL": "Tallinn", "AGP": "Malaga", "ALG": "Algeciras", "BCN": "Barcelona", "BIO": "Bilbao",
    "GIJ": "Gijon", "VGO": "Vigo", "VLC": "Valencia", "HEL": "Helsinki", "FOS": "Fos sur Mer",
    "LEH": "Le Havre", "LGP": "Thames", "LON": "London", "SOU": "Southampton", "SKG": "Thessaloniki",
    "PRQ": "Puerto Quetzal", "GEO": "Georgetown", "HKG": "Hong Kong", "PCR": "Puerto Cortes",
    "JKT": "Yakarta", "PJG": "Panjang", "SRG": "Semarang", "SUB": "Subraya", "HFA": "Haifa",
    "ENR": "Ennore", "JAI": "Injai", "MAA": "Chennai", "MUN": "Mundra", "NSA": "Nava Sheva",
    "LIV": "Livorno", "SAL": "Salerno", "VDL": "Vado Ligure", "KIN": "Kingston", "MOJ": "Moji",
    "NGO": "Nagoya", "OSA": "Osaka", "SBS": "Shibushi", "TYO": "Tokyo", "UKB": "Kobe",
    "YOK": "Yokohama", "ICN": "Incheon", "PUS": "Busan", "USN": "Ulsan", "BEY": "Beirut",
    "KLJ": "Klaipeda", "ATM": "Altamira", "ESE": "Ensenada", "LZC": "Lazaro Cardenas", "VER": "Veracruz",
    "ZLO": "Manzanillo", "AMS": "Amsterdam", "RTM": "Rotterdam", "HAU": "Haroy", "KRS": "Kristiansand",
    "OSL": "Oslo", "BLB": "Balboa", "MIT": "Manzanillo", "ROD": "Rodman", "MNL": "Manila",
    "GDN": "Gdansk", "GDY": "Gdynia", "SJU": "San Juan", "LIS": "Lisboa", "SIE": "Sines",
    "ASU": "Asuncion", "LED": "Saint Petersburg", "JED": "Jeddah", "GOT": "Goteborg",
    "KOP": "Koper", "AQJ": "Acajutla", "LCH": "Laem Chabang", "MER": "Mersin", "KEL": "Keelung",
    "KHH": "Kaohsiung", "ILK": "Chornomorsk", "ATL": "Atlanta", "BAL": "Baltimore", "CHI": "Chicago",
    "CHS": "Charleston", "EWR": "Newark", "HNL": "Honolulu", "HOU": "Houston", "IND": "Indianapolis",
    "JAX": "Jacksonville", "LAX": "Los Angeles", "LGB": "Long Beach", "MEM": "Memphis",
    "MSY": "New Orleans", "NTD": "Port Hueneme", "NYC": "New York", "OAK": "Oakland",
    "ORF": "Norfolk", "PEF": "Everglades", "PHL": "Philadelphia", "SAV": "Savannah",
    "SEA": "Seattle", "SLC": "Salt Lake", "STL": "St Louis", "MVD": "Montevideo",
    "LAG": "La Guaira", "MAR": "Maracaibo", "PBL": "Puerto Cabello", "HPH": "Haiphong",
    "DUR": "Durban", "MIA": "Miami", "BOS": "Boston", "CRQ": "Charlotte", "DET": "Detroit",
    "ORH": "Worcester", "TIW": "Tacoma", "XMX": "Southport"
}

CONTINENTES = {
    "CHINA": "Asia", "KOREA SUR": "Asia", "CHILE": "América", "BOLIVIA": "América",
    "NUEVA ZELANDA": "Oceanía", "JAPON": "Asia", "ESTADOS UNIDOS": "América",
    "COLOMBIA": "América", "COSTA RICA": "América", "ECUADOR": "América",
    "PANAMA": "América", "ALEMANIA": "Europa", "BELGICA": "Europa", "CANADA": "América",
    "TAIWAN": "Asia", "TAILANDIA": "Asia", "HONG KONG": "Asia", "MALASIA": "Asia",
    "SYDNEY": "Oceanía", "BRAZIL": "América", "MEXICO": "América", "REINO UNIDO": "Europa",
    "PAISES BAJOS": "Europa", "REPUBLICA DOMINICANA": "América", "ITALIA": "Europa",
    "ESPAÑA": "Europa", "FRANCIA": "Europa", "RUSIA": "Europa", "INDIA": "Asia",
    "AUSTRALIA": "Oceanía", "NORUEGA": "Europa", "POLONIA": "Europa", "ESTONIA": "Europa",
    "TURQUIA": "Asia", "GRECIA": "Europa", "INDONESIA": "Asia", "ALGERIA": "África",
    "JAMAICA": "América", "VENEZUELA": "América", "PARAGUAY": "América", "ARUBA": "América",
    "PUERTO RICO": "América", "NICARAGUA": "América", "EL SALVADOR": "América",
    "CUBA": "América", "TRINIDAD Y TOBAGO": "América"
}


def _fetch_nivel3(session, headers, data_payload, reg_det,
                  val_manifiesto, val_fecha_zarpe, val_nombre_nave,
                  val_detalle, val_puerto, val_bl, val_fecha_transmision, pagina_det):
    """Descarga y parsea el nivel 3 (conocimiento) para un detalle dado."""
    filas_resultado = []
    try:
        resp_conoc = session.post(
            f"{URL_BASE}?accion=consultarConocimientoDetalle&reg={reg_det}",
            data=data_payload, headers=headers, timeout=30
        )
        soup_conoc = BeautifulSoup(resp_conoc.text, "html.parser")
        filas_mercaderia = soup_conoc.find_all("tr", class_="bg")
        for fila_merc in filas_mercaderia:
            cols_merc = fila_merc.find_all("td")
            if len(cols_merc) >= 7:
                val_bultos = cols_merc[0].text.strip()
                if "Bultos" in val_bultos or val_bultos.upper() == "BULTOS":
                    continue
                filas_resultado.append([
                    val_manifiesto, val_fecha_zarpe, val_nombre_nave,
                    val_detalle, val_puerto, val_bl, val_fecha_transmision,
                    cols_merc[0].text.strip(), cols_merc[1].text.strip(),
                    cols_merc[2].text.strip(), cols_merc[3].text.strip(),
                    cols_merc[4].text.strip(), cols_merc[5].text.strip(),
                    cols_merc[6].text.strip()
                ])
        print(f"    [+] Pag.Detalles({pagina_det}) -> Detalle: {val_detalle} | Mercadería extraída")
    except Exception as e:
        print(f"    [X] Error en CE {val_detalle}: {e}")
    return filas_resultado


def _identificar_producto(descripcion, embarcador, consignatario):
    if pd.isna(descripcion) or not isinstance(descripcion, str):
        descripcion = ""
    descripcion = descripcion.lower()
    for producto, palabras_clave in PALABRAS_CLAVE_PRODUCTOS.items():
        if any(palabra in descripcion for palabra in palabras_clave):
            return producto
    if pd.notna(embarcador) and isinstance(embarcador, str):
        for exportador in EXPORTADORES_ESPECIFICOS:
            if exportador.lower() in embarcador.lower():
                return "¿Jengibre?"
    if pd.notna(consignatario) and isinstance(consignatario, str):
        for importador in IMPORTADORES_ESPECIFICOS:
            if importador.lower() in consignatario.lower():
                return "¿Jengibre?"
    return "No asociado"


def _segmentar_jengibre(descripcion, producto):
    if producto == "Jengibre fresco":
        descripcion = descripcion.lower() if isinstance(descripcion, str) else ""
        if "juice" in descripcion or "jugo" in descripcion:
            return "Jugo de jengibre"
        if "bags" in descripcion or "sacos" in descripcion or "bolsas" in descripcion:
            return "Jengibre deshidratado"
    return producto


def _clasificar_df(df, directorio_base, nombre_archivo_base):
    """Agrega columnas de clasificación y guarda el archivo completo con tabla de Excel."""
    print("\n[*] Clasificando mercadería...")

    # Columnas derivadas de Fecha de Zarpe
    fechas = pd.to_datetime(df["Fecha de Zarpe"], dayfirst=True, errors="coerce")
    df.insert(1, "Envio", "maritimo")
    df.insert(2, "Semana", fechas.dt.isocalendar().week.astype("Int64"))
    df.insert(3, "Año", fechas.dt.year.astype("Int64"))

    df["Producto"] = df.apply(
        lambda row: _segmentar_jengibre(
            row.get("Descripción de Mercadería", ""),
            _identificar_producto(
                row.get("Descripción de Mercadería", ""),
                row.get("Embarcador", ""),
                row.get("Consignatario", "")
            )
        ), axis=1
    )
    df["Pais"] = df["Puerto"].apply(
        lambda p: PUERTOS_PAISES.get(str(p)[:2].upper(), "No asociado") if pd.notna(p) else None
    )
    df["Ciudad destino"] = df["Puerto"].apply(
        lambda p: CIUDAD_DESTINO.get(str(p)[-3:].upper(), "No asociado") if pd.notna(p) else None
    )
    df["Continente"] = df["Pais"].apply(
        lambda pais: CONTINENTES.get(str(pais).upper(), "No asociado") if pd.notna(pais) else None
    )

    nombre_completo = nombre_archivo_base.replace("detalle_mercaderia_", "detalle_mercaderia_completo_")
    archivo_completo = os.path.join(directorio_base, nombre_completo)

    with pd.ExcelWriter(archivo_completo, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
        ws = writer.sheets["Sheet1"]
        # Filtros y formato
        num_cols = len(df.columns)
        col_letra = chr(64 + num_cols) if num_cols <= 26 else "A" + chr(64 + num_cols - 26)
        tabla = Table(
            displayName="DetalleMercaderia",
            ref=f"A1:{col_letra}{len(df) + 1}"
        )
        tabla.tableStyleInfo = TableStyleInfo(
            name="TableStyleMedium9", showFirstColumn=False, showLastColumn=False,
            showRowStripes=True, showColumnStripes=False
        )
        ws.add_table(tabla)
        for row in ws.iter_rows():
            for cell in row:
                cell.alignment = Alignment(wrap_text=False)
            ws.row_dimensions[row[0].row].height = 15

    print(f"[*] Archivo clasificado guardado en: {archivo_completo}")

def procesar_manifiestos(fecha_inicio, fecha_fin):
    datos_acumulados = []
    
    directorio_base = os.path.join(os.path.dirname(__file__), "data")
    os.makedirs(directorio_base, exist_ok=True)
    fecha_inicio_fmt = fecha_inicio.replace("/", "-")
    fecha_fin_fmt = fecha_fin.replace("/", "-")
    if fecha_inicio_fmt == fecha_fin_fmt:
        nombre_archivo = f"detalle_mercaderia_{fecha_inicio_fmt}.xlsx"
    else:
        nombre_archivo = f"detalle_mercaderia_{fecha_inicio_fmt}_al_{fecha_fin_fmt}.xlsx"
    archivo_excel = os.path.join(directorio_base, nombre_archivo)

    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    print(f"[*] Obteniendo cookies de sesión inicial...")
    try:
        session.get(f"{URL_BASE}?accion=cargaConsultaManifiesto&tipoConsulta=fechaSalida", headers=headers, timeout=15)
    except Exception as e:
        print(f"[!] Error conectándose a la SUNAT: {e}")
        return

    data_payload = {
        "fech_llega_ini": fecha_inicio,
        "fech_llega_fin": fecha_fin,
        "matr_nave": ""
    }

    print(f"[*] Consultando Manifiestos de Exportación Marítima del {fecha_inicio} al {fecha_fin}...")
    
    resp_manifiestos = session.post(f"{URL_BASE}?accion=consultarManifiesto", data=data_payload, headers=headers, timeout=30)
    
    pagina_manif = 1
    while True: # LOOP NIVEL 1
        soup = BeautifulSoup(resp_manifiestos.text, "html.parser")
        filas_manifiestos = soup.find_all("tr", class_="bg")
        filas_validas = [f for f in filas_manifiestos if f.find("a")]
        
        print(f"\n[+] --------------- PÁGINA {pagina_manif} DE MANIFIESTOS ({len(filas_validas)} encontrados) ---------------")

        for idx_manifiesto, fila_manif in enumerate(filas_validas):
            cols_manif = fila_manif.find_all("td")
            if len(cols_manif) < 3: continue
                
            btn_a = cols_manif[0].find("a")
            if not btn_a: continue
                
            val_manifiesto = btn_a.text.strip()
            val_fecha_zarpe = cols_manif[1].text.strip()
            val_nombre_nave = cols_manif[2].text.strip()
            
            match = re.search(r"detalle\('(\d+)'\)", btn_a.get("href", ""))
            reg_manif = match.group(1) if match else str(idx_manifiesto)
            
            print(f" => Procesando Manifiesto: {val_manifiesto} | Nave: {val_nombre_nave}")
            
            # =============== NIVEL 2 ================
            try:
                resp_detalles = session.post(
                    f"{URL_BASE}?accion=consultarManifiestoDetalle&reg={reg_manif}", 
                    data=data_payload, headers=headers, timeout=30
                )
            except Exception as e:
                print(f"  [X] Error en manifiesto {val_manifiesto}: {e}")
                continue
                
            pagina_det = 1
            while True: # LOOP NIVEL 2
                soup_detalles = BeautifulSoup(resp_detalles.text, "html.parser")
                filas_detalles = soup_detalles.find_all("tr", class_="bg")
                filas_det_validas = [f for f in filas_detalles if f.find("a")]

                # Recopilar trabajos de nivel 3 para esta página
                jobs_nivel3 = []
                for idx_det, fila_det in enumerate(filas_det_validas):
                    cols_det = fila_det.find_all("td")
                    if len(cols_det) < 14: continue

                    val_puerto = cols_det[0].text.strip()
                    val_bl = cols_det[2].text.strip()
                    btn_det = cols_det[3].find("a")
                    if not btn_det: continue
                    val_detalle = btn_det.text.strip()
                    val_fecha_transmision = cols_det[13].text.strip()

                    match_det = re.search(r"detalle\('(\d+)'\)", btn_det.get("href", ""))
                    reg_det = match_det.group(1) if match_det else str(idx_det)

                    jobs_nivel3.append((
                        reg_det, val_manifiesto, val_fecha_zarpe, val_nombre_nave,
                        val_detalle, val_puerto, val_bl, val_fecha_transmision, pagina_det
                    ))

                # =============== NIVEL 3 EN PARALELO ================
                if jobs_nivel3:
                    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                        futures = {
                            executor.submit(_fetch_nivel3, session, headers, data_payload, *job): job
                            for job in jobs_nivel3
                        }
                        for future in as_completed(futures):
                            datos_acumulados.extend(future.result())

                # Guardar incrementalmente luego de cada página de nivel 2
                if datos_acumulados:
                    df = pd.DataFrame(datos_acumulados, columns=COLUMNAS_EXCEL)
                    with pd.ExcelWriter(archivo_excel, engine="openpyxl") as writer:
                        df.to_excel(writer, index=False, sheet_name="Sheet1")
                        ws = writer.sheets["Sheet1"]
                        ws.auto_filter.ref = ws.dimensions
                        for row in ws.iter_rows():
                            for cell in row:
                                cell.alignment = Alignment(wrap_text=False)
                            ws.row_dimensions[row[0].row].height = 15
                
                # REVISAR PAGINACION DE NIVEL 2
                # El Siguiente usa javascript:paginacion(tamanioPagina, pagina) — NO NavigFormNext
                link_sgte_det = soup_detalles.find("a", string=re.compile(r"Siguiente", re.I))
                if link_sgte_det:
                    href = link_sgte_det.get("href", "")
                    match_pag = re.search(r"paginacion\((\d+),(\d+)\)", href)
                    if match_pag:
                        payload_next = data_payload.copy()
                        payload_next["tamanioPagina"] = match_pag.group(1)
                        payload_next["pagina"] = match_pag.group(2)
                        resp_detalles = session.post(URL_PAG_NIVEL_2, data=payload_next, headers=headers)
                        pagina_det += 1
                    else:
                        break
                else:
                    break

        # REVISAR PAGINACION DE NIVEL 1 — misma lógica paginacion()
        link_sgte_manif = soup.find("a", string=re.compile(r"Siguiente", re.I))
        if link_sgte_manif:
            href = link_sgte_manif.get("href", "")
            match_pag = re.search(r"paginacion\((\d+),(\d+)\)", href)
            if match_pag:
                payload_next = data_payload.copy()
                payload_next["tamanioPagina"] = match_pag.group(1)
                payload_next["pagina"] = match_pag.group(2)
                resp_manifiestos = session.post(URL_PAG_NIVEL_1, data=payload_next, headers=headers)
                pagina_manif += 1
            else:
                break
        else:
            break

    print(f"\n[*] Proceso Terminado. Archivo guardado correctamente en: {archivo_excel}")

    if datos_acumulados:
        df_final = pd.DataFrame(datos_acumulados, columns=COLUMNAS_EXCEL)
        _clasificar_df(df_final, directorio_base, nombre_archivo)

if __name__ == "__main__":
    print("=" * 60)
    print("SCRAPER DE MANIFIESTOS MARITIMOS (vía Requests con Paginación)")
    print("=" * 60)
    f_inicio = input(">> Ingrese fecha de inicio (dd/mm/aaaa) Ej: 12/05/2026: ").strip()
    f_fin = input(">> Ingrese fecha fin (dd/mm/aaaa) Ej: 12/05/2026: ").strip()
    
    if f_inicio and f_fin:
        t_start = time.time()
        procesar_manifiestos(f_inicio, f_fin)
        print(f"\n[TIEMPO] Escaneo finalizado en {time.time() - t_start:.2f} segundos.")
    else:
        print("[!] Fechas no válidas.")
