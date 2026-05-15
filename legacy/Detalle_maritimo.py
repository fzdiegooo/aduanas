import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# =============== Configuración del driver ===============
chrome_options = Options()
chrome_options.add_argument("--start-maximized")
chrome_service = Service(ChromeDriverManager().install())

# =============== Datos acumulados ===============
datos_acumulados = []

# Columnas EXACTAS del Excel de salida (14)
COLUMNAS_EXCEL = [
    "Manifiesto", "Fecha de Zarpe", "Nombre de Nave", "Detalle", "Puerto", "B/L",
    "Fecha de Transmisión del Documento", "Bultos", "Peso Bruto", "Empaques",
    "Embarcador", "Consignatario", "Marcas y Números", "Descripción de Mercadería"
]

EXPECTED_COLS = len(COLUMNAS_EXCEL)


# ----------------- Utilidades robustas -----------------
def _has_element(driver, by, value, timeout=0):
    """True/False si existe el elemento. Si timeout > 0, espera hasta timeout."""
    try:
        if timeout > 0:
            WebDriverWait(driver, timeout).until(EC.presence_of_element_located((by, value)))
        else:
            driver.find_element(by, value)
        return True
    except Exception:
        return False


def _safe_text(el):
    try:
        return el.text.strip()
    except Exception:
        return ""


def _normalize_row(row, expected=EXPECTED_COLS):
    """Ajusta una fila a 'expected' columnas (corta o rellena con vacío)."""
    if len(row) > expected:
        return row[:expected]
    if len(row) < expected:
        return row + [""] * (expected - len(row))
    return row


# ----------------- Persistencia incremental -----------------
def guardar_datos_incrementales():
    if not datos_acumulados:
        return
    filas_norm = [_normalize_row(r, EXPECTED_COLS) for r in datos_acumulados]
    df = pd.DataFrame(filas_norm, columns=COLUMNAS_EXCEL)
    df.to_excel("detalle_mercaderia_incremental.xlsx", index=False)
    print("Datos guardados incrementalmente en 'detalle_mercaderia_incremental.xlsx'.")


# ----------------- Extracción de tabla de detalle -----------------
def extraer_tabla_correcta(driver):
    """
    Devuelve lista de filas (cada fila = lista de celdas).
    Si no hay tabla o está vacía (solo cabecera), devuelve [].
    """
    try:
        # La tabla de documentos/detalle tiene clase 'beta'
        if not _has_element(driver, By.XPATH, "//table[@class='beta']", timeout=5):
            return []

        tabla = driver.find_element(By.XPATH, "//table[@class='beta']")
        filas = tabla.find_elements(By.TAG_NAME, "tr")

        # Sin filas o solo cabecera => vacío
        if len(filas) <= 1:
            return []

        detalles = []
        for fila in filas[1:]:  # saltar cabecera
            celdas = fila.find_elements(By.TAG_NAME, "td")
            detalle = [_safe_text(td) for td in celdas]
            if detalle:
                detalles.append(detalle)
        return detalles
    except Exception as e:
        print(f"Error al extraer la tabla correcta: {e}")
        return []


# ----------------- Procesamiento de un DETALLE -----------------
def procesar_detalle(driver, detalle_numero, manifiesto, puerto, bl, fecha_zarpe, nombre_nave, fecha_transmision):
    """
    Entra al link del detalle (columna 'Detalle'), extrae su tabla y vuelve.
    Si no existe el link o la tabla está vacía, se salta sin romper el flujo.
    """
    try:
        # Intentar encontrar y abrir el enlace del detalle
        if not _has_element(driver, By.XPATH, f"//a[normalize-space(text())='{detalle_numero}']", timeout=3):
            print(f"  · Detalle {detalle_numero}: enlace no encontrado, se omite.")
            return

        driver.find_element(By.XPATH, f"//a[normalize-space(text())='{detalle_numero}']").click()
        time.sleep(0.8)  # breve respiro

        print(f"  · Extrayendo tabla del detalle {detalle_numero}...")
        datos_detalle = extraer_tabla_correcta(driver)

        if not datos_detalle:
            print(f"    (sin filas en el detalle {detalle_numero})")

        for fila in datos_detalle:
            fila_completa = [
                manifiesto, fecha_zarpe, nombre_nave, detalle_numero,
                puerto, bl, fecha_transmision
            ] + fila
            datos_acumulados.append(_normalize_row(fila_completa, EXPECTED_COLS))

        guardar_datos_incrementales()

    except Exception as e:
        print(f"Error al procesar el detalle {detalle_numero}: {e}")
    finally:
        # Siempre intentamos volver a la tabla de documentos del manifiesto
        try:
            driver.execute_script("window.history.go(-1)")
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.XPATH, "//table[contains(@class,'beta')]"))
            )
            time.sleep(0.5)
        except Exception:
            # Si no encuentra la tabla, igual continuamos (volverá a la lista arriba)
            pass


# ----------------- Procesamiento de la tabla de documentos de un MANIFIESTO -----------------
def procesar_tabla_manifiesto(driver, manifiesto, fecha_zarpe, nombre_nave):
    """
    Dentro del manifiesto: recorre la tabla 'beta' paginada (si existe).
    Si el manifiesto está vacío (sin tabla o solo cabecera), regresa sin error.
    """
    try:
        # Esperar hasta 5s por una tabla 'beta' o asumir vacío
        if not _has_element(driver, By.XPATH, "//table[contains(@class,'beta')]", timeout=5):
            print("  (manifiesto sin tabla de documentos; se regresa)")
            return

        # Si la tabla existe pero está vacía (solo cabecera), también regresamos
        tabla = driver.find_element(By.XPATH, "//table[contains(@class,'beta')]")
        if len(tabla.find_elements(By.TAG_NAME, "tr")) <= 1:
            print("  (manifiesto sin filas de documentos; se regresa)")
            return

        # Paginación dentro del manifiesto
        while True:
            # Leer filas actuales (saltando cabecera)
            filas = driver.find_elements(By.XPATH, "//table[contains(@class,'beta')]//tr")[1:]
            detalles_a_procesar = []

            for fila in filas:
                cols = fila.find_elements(By.TAG_NAME, "td")
                # Requerimos al menos 14 celdas (donde 0=Puerto, 2=BL, 3=Detalle, 13=Fecha Transmisión)
                if len(cols) >= 14:
                    puerto = _safe_text(cols[0])
                    bl = _safe_text(cols[2])
                    detalle_numero = _safe_text(cols[3])
                    fecha_transmision = _safe_text(cols[13])
                    if detalle_numero:
                        detalles_a_procesar.append((detalle_numero, puerto, bl, fecha_transmision))

            # Procesar cada detalle detectado en esta página
            for det, puerto, bl, ftx in detalles_a_procesar:
                procesar_detalle(driver, det, manifiesto, puerto, bl, fecha_zarpe, nombre_nave, ftx)

            # Intentar ir a "Siguiente" (solo dentro del bloque del manifiesto)
            # Usamos el último "Siguiente" por seguridad.
            links_sgte = driver.find_elements(By.XPATH, "(//a[normalize-space(text())='Siguiente'])[last()]")
            if links_sgte:
                try:
                    links_sgte[0].click()
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, "//table[contains(@class,'beta')]"))
                    )
                    time.sleep(0.5)
                except Exception:
                    # Si falla la paginación, salimos del bucle
                    break
            else:
                break

    except Exception as e:
        print(f"  (aviso) Problema dentro del manifiesto: {e}")
    finally:
        # Volver siempre a la lista de manifiestos
        try:
            driver.execute_script("window.history.go(-1)")
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "/html/body/form[1]/table[4]"))
            )
            time.sleep(0.5)
        except Exception:
            pass

    
# ----------------- Bucle principal: lista de manifiestos -----------------
def procesar_manifiestos(driver):
    try:
        driver.get("http://www.aduanet.gob.pe/cl-ad-itconsmanifiesto/manifiestoITS01Alias?accion=cargarFrmConsultaManifExpMarFecha")
        print("Página cargada. Ingresa las fechas manualmente y haz clic en 'Consultar'.")
        input("Presiona Enter cuando los resultados estén listos...")

        pagina_actual = 1
        while True:
            print(f"Procesando manifiestos en la página {pagina_actual}...")

            filas = driver.find_elements(
                By.XPATH,
                "/html/body/form[1]/table[4]/tbody/tr[2]/td/table/tbody/tr[2]/td/table[3]/tbody/tr"
            )
            if not filas or len(filas) <= 1:
                print("No se encontraron filas en la tabla de manifiestos.")
                break

            # Recorremos por índice; cada vuelta re-leemos la tabla (para evitar stale elements)
            idx = 1  # saltar cabecera
            while True:
                try:
                    filas_actualizadas = driver.find_elements(
                        By.XPATH,
                        "/html/body/form[1]/table[4]/tbody/tr[2]/td/table/tbody/tr[2]/td/table[3]/tbody/tr"
                    )
                    if idx >= len(filas_actualizadas):
                        break

                    fila = filas_actualizadas[idx]
                    columnas = fila.find_elements(By.TAG_NAME, "td")
                    idx += 1

                    if len(columnas) < 3:
                        continue

                    manifiesto = _safe_text(columnas[0])
                    fecha_zarpe = _safe_text(columnas[1])
                    nombre_nave = _safe_text(columnas[2])

                    a_tags = fila.find_elements(By.TAG_NAME, "a")
                    if not a_tags:
                        # Fila sin enlace (posible separación o total); se omite
                        continue

                    print(f"Procesando manifiesto: {manifiesto} (Fecha de Zarpe: {fecha_zarpe}, Nave: {nombre_nave})...")
                    a_tags[0].click()

                    # Esperar: o aparece la tabla de documentos (beta) o si no aparece,
                    # igual procesar_tabla_manifiesto maneja el vacío.
                    # Ponemos una espera corta para que cambie de pantalla.
                    time.sleep(0.6)

                    procesar_tabla_manifiesto(driver, manifiesto, fecha_zarpe, nombre_nave)

                except Exception as e:
                    print(f"Error al procesar el manifiesto en la fila {idx-1}: {e}")
                    # Si por alguna razón quedamos dentro, intentamos volver
                    try:
                        driver.execute_script("window.history.go(-1)")
                        WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.XPATH, "/html/body/form[1]/table[4]"))
                        )
                    except Exception:
                        pass

            # Paginación de la lista de manifiestos
            links_sgte = driver.find_elements(By.XPATH, "(//a[normalize-space(text())='Siguiente'])[last()]")
            if links_sgte:
                try:
                    links_sgte[0].click()
                    pagina_actual += 1
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, "/html/body/form[1]/table[4]"))
                    )
                    time.sleep(0.5)
                except Exception:
                    print("No se pudo avanzar a la siguiente página de manifiestos.")
                    break
            else:
                print("No hay más páginas de manifiestos disponibles.")
                break

    except Exception as e:
        print(f"Error general al procesar los manifiestos: {e}")


# ----------------- Arranque -----------------
if __name__ == "__main__":
    driver = webdriver.Chrome(service=chrome_service, options=chrome_options)
    try:
        procesar_manifiestos(driver)
    finally:
        driver.quit()
        print("Navegador cerrado.")

