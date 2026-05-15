import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

chrome_options = Options()
chrome_options.add_argument("--start-maximized")
chrome_service = Service(ChromeDriverManager().install())

datos_acumulados = []


def guardar_datos_incrementales():
    columnas_excel = [
        "Manifiesto", "Fecha de Salida", "Vuelo", "Aerolínea", "Puerto de Embarque",
        "Bultos", "Peso Bruto", "Embarcador", "Consignatario",
        "Marcas y Números", "Descripción de Mercadería"
    ]

    df = pd.DataFrame(datos_acumulados, columns=columnas_excel)
    df.sort_values(by=["Manifiesto", "Fecha de Salida"], inplace=True)
    df.to_excel("detalles_manifiestos_aereos.xlsx", index=False)
    print("? Datos guardados incrementalmente")


def extraer_info_detalle(driver, manifiesto, fecha_salida, vuelo, aerolinea, puerto_embarque):
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "/html/body/form/table[4]"))
        )

        fila = driver.find_element(
            By.XPATH,
            "/html/body/form/table[4]/tbody/tr/td/table/tbody/tr/td/table/tbody/tr[3]"
        )

        columnas = fila.find_elements(By.TAG_NAME, "td")

        if len(columnas) >= 7:
            bultos = columnas[0].text.strip()
            peso_bruto = columnas[1].text.strip()
            embarcador = columnas[3].text.strip()
            consignatario = columnas[4].text.strip()
            marcas_numeros = columnas[5].text.strip()
            descripcion = columnas[6].text.strip()

            datos_acumulados.append([
                manifiesto, fecha_salida, vuelo, aerolinea, puerto_embarque,
                bultos, peso_bruto, embarcador, consignatario,
                marcas_numeros, descripcion
            ])

            print(f"? Extraído {manifiesto} | {vuelo} | {descripcion}")
            guardar_datos_incrementales()

    except Exception as e:
        print(f"? Error al extraer detalle: {e}")


def procesar_detalles(driver, manifiesto, fecha_salida, vuelo, aerolinea, puerto_embarque):
    try:
        i = 0
        while True:
            filas = driver.find_elements(
                By.XPATH,
                "/html/body/form[1]/table[5]/tbody/tr/td/table/tbody/tr[1]/td/table/tbody/tr"
            )[1:]

            if i >= len(filas):
                break

            try:
                fila = filas[i]
                enlace = fila.find_element(By.XPATH, "./td[3]/a")
                detalle = enlace.text.strip()

                print(f"   ? Detalle {detalle}")
                enlace.click()
                time.sleep(2)

                extraer_info_detalle(
                    driver, manifiesto, fecha_salida, vuelo, aerolinea, puerto_embarque
                )

                driver.execute_script("window.history.go(-1)")
                time.sleep(2)

                i += 1

            except Exception as e:
                print(f"? Error en detalle índice {i}: {e}")
                i += 1

    except Exception as e:
        print(f"? Error general en detalles: {e}")


def procesar_todos_los_manifiestos(driver):
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "/html/body/form[1]/table[4]"))
        )

        i = 0
        while True:
            filas = driver.find_elements(
                By.XPATH,
                "/html/body/form[1]/table[4]/tbody/tr/td/table/tbody/tr[1]/td/table/tbody/tr"
            )[1:]

            if i >= len(filas):
                break

            try:
                fila = filas[i]

                enlace = fila.find_element(By.XPATH, "./td[1]/a")
                manifiesto = enlace.text.strip()
                fecha_salida = fila.find_element(By.XPATH, "./td[2]").text.strip()
                aerolinea = fila.find_element(By.XPATH, "./td[4]").text.strip()
                puerto_embarque = fila.find_element(By.XPATH, "./td[5]").text.strip()
                vuelo = fila.find_element(By.XPATH, "./td[6]").text.strip()

                print(f"\n? Manifiesto {manifiesto}")
                enlace.click()
                time.sleep(2)

                procesar_detalles(
                    driver, manifiesto, fecha_salida, vuelo, aerolinea, puerto_embarque
                )

                driver.execute_script("window.history.go(-1)")
                time.sleep(2)

                i += 1

            except Exception as e:
                print(f"? Error en manifiesto índice {i}: {e}")
                i += 1

    except Exception as e:
        print(f"? Error general en manifiestos: {e}")


# ================== EJECUCIÓN ==================

driver = webdriver.Chrome(service=chrome_service, options=chrome_options)

try:
    driver.get(
        "http://www.aduanet.gob.pe/cl-ad-itconsmanifiesto/"
        "manifiestoITS01Alias?accion=cargaConsultaManifiesto&tipoConsulta=fechaSalida"
    )

    print("?? Ingresa las fechas y presiona CONSULTAR")
    input("? Presiona Enter cuando los resultados estén listos...")

    procesar_todos_los_manifiestos(driver)

finally:
    driver.quit()
    print("?? Navegador cerrado correctamente")
