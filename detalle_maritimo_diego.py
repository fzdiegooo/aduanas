import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import os

URL_BASE = "http://www.aduanet.gob.pe/cl-ad-itconsmanifiesto/manifiestoITS01Alias"
URL_PAG_NIVEL_1 = "http://www.aduanet.gob.pe/cl-ad-itconsmanifiesto/ConsultaManifExpMarFecha.jsp"
URL_PAG_NIVEL_2 = "http://www.aduanet.gob.pe/cl-ad-itconsmanifiesto/ConsultaManifExpMarDetalle.jsp"

# Columnas EXACTAS del Excel de salida (14)
COLUMNAS_EXCEL = [
    "Manifiesto", "Fecha de Zarpe", "Nombre de Nave", "Detalle", "Puerto", "B/L",
    "Fecha de Transmisión del Documento", "Bultos", "Peso Bruto", "Empaques",
    "Embarcador", "Consignatario", "Marcas y Números", "Descripción de Mercadería"
]

def procesar_manifiestos(fecha_inicio, fecha_fin):
    datos_acumulados = []
    
    directorio_base = os.path.join(os.path.dirname(__file__), "data")
    os.makedirs(directorio_base, exist_ok=True)
    archivo_excel = os.path.join(directorio_base, "detalle_mercaderia_incremental.xlsx")

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
                    
                    # =============== NIVEL 3 ================
                    try:
                        resp_conoc = session.post(
                            f"{URL_BASE}?accion=consultarConocimientoDetalle&reg={reg_det}", 
                            data=data_payload, headers=headers, timeout=30
                        )
                        soup_conoc = BeautifulSoup(resp_conoc.text, "html.parser")
                    except Exception as e:
                        print(f"    [X] Error en CE {val_detalle}: {e}")
                        continue
                        
                    filas_mercaderia = soup_conoc.find_all("tr", class_="bg")
                    
                    for fila_merc in filas_mercaderia:
                        cols_merc = fila_merc.find_all("td")
                        
                        if len(cols_merc) >= 7:
                            val_bultos = cols_merc[0].text.strip()
                            
                            # SOLUCIÓN: IGNORAR CABECERAS PARA QUE NO SE CORRAN LAS COLUMNAS EN EXCEL
                            if "Bultos" in val_bultos or val_bultos.upper() == "BULTOS":
                                continue
                                
                            val_peso = cols_merc[1].text.strip()
                            val_empaques = cols_merc[2].text.strip()
                            val_embarcador = cols_merc[3].text.strip()
                            val_consignatario = cols_merc[4].text.strip()
                            val_marcas = cols_merc[5].text.strip()
                            val_descripcion = cols_merc[6].text.strip()
                            
                            fila_excel = [
                                val_manifiesto, val_fecha_zarpe, val_nombre_nave, val_detalle, val_puerto, val_bl,
                                val_fecha_transmision, val_bultos, val_peso, val_empaques,
                                val_embarcador, val_consignatario, val_marcas, val_descripcion
                            ]
                            
                            datos_acumulados.append(fila_excel)
                            print(f"    [+] Pag.Detalles({pagina_det}) -> Detalle: {val_detalle} | Mercadería extraída")
                            
                # Guardar incrementalmente luego de cada página
                if datos_acumulados:
                    df = pd.DataFrame(datos_acumulados, columns=COLUMNAS_EXCEL)
                    with pd.ExcelWriter(archivo_excel, engine="openpyxl") as writer:
                        df.to_excel(writer, index=False, sheet_name="Sheet1")
                        ws = writer.sheets["Sheet1"]
                        ws.auto_filter.ref = ws.dimensions
                        for row in ws.iter_rows():
                            for cell in row:
                                cell.alignment = __import__("openpyxl").styles.Alignment(wrap_text=False)
                        for row in ws.iter_rows():
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
