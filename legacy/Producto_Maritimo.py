import pandas as pd
import os
from openpyxl import load_workbook
from openpyxl.worksheet.table import Table, TableStyleInfo

directorio_base = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(directorio_base, exist_ok=True)
archivo_excel = os.path.join(directorio_base, "detalle_mercaderia_incremental.xlsx")
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
    "Fresas": ["Fresa", "Strawberries", "Fresas"],
    "Quinua": ["Quinua", "Quinoa"],
    "Mandarina": ["Mandarina", "Clementinas", "Clementine"],
    "Menestras": ["Beans", "lentejas", "menestras", "frejoles", "alberja"],
    "Cebolla": ["Cebolla", "Onion", "Onions", "Cebollas"]
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
    "ORH": "Worcester", "TIW": "Tacoma", "XMX": "Southport", "ALG": "Alger", "GDY": "Gdynia"
}

CONTINENTES = {
    "CHINA": "Asia", "KOREA SUR": "Asia", "CHILE": "América", "BOLIVIA": "América",
    "NUEVA ZELANDA": "Oceanía", "JAPON": "Asia", "ESTADOS UNIDOS": "América",
    "COLOMBIA": "América", "COSTA RICA": "América", "ECUADOR": "América",
    "PANAMA": "América", "ALEMANIA": "Europa", "BELGICA": "Europa", "CANADA": "América",
    "TAIWAN": "Asia", "TAILANDIA": "Asia", "HONG KONG": "Asia", "MALASIA": "Asia",
    "SYDNEY": "Oceanía", "BRAZIL": "América", "MEXICO": "América", "REINO UNIDO": "Europa",
    "PAISES BAJOS": "Europa", "REPÚBLICA DOMINICANA": "América", "ITALIA": "Europa",
    "ESPAÑA": "Europa", "FRANCIA": "Europa", "RUSIA": "Europa", "INDIA": "Asia",
    "AUSTRALIA": "Oceanía"
}

if not os.path.exists(archivo_excel):
    print(f"El archivo {archivo_excel} no existe.")
else:
    df = pd.read_excel(archivo_excel)

    def identificar_producto(descripcion, embarcador, consignatario):
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

    def segmentar_jengibre(descripcion, producto):
        if producto == "Jengibre fresco":
            descripcion = descripcion.lower()
            if "juice" in descripcion or "jugo" in descripcion:
                return "Jugo de jengibre"
            if "bags" in descripcion or "sacos" in descripcion or "bolsas" in descripcion:
                return "Jengibre deshidratado"
        return producto

    def identificar_pais(puerto):
        if pd.isna(puerto):
            return None
        codigo_pais = puerto[:2].upper()
        return PUERTOS_PAISES.get(codigo_pais, "No asociado")

    def identificar_ciudad_destino(puerto):
        if pd.isna(puerto):
            return None
        codigo_ciudad = puerto[-3:].upper()
        return CIUDAD_DESTINO.get(codigo_ciudad, "No asociado")

    def identificar_continente(pais):
        if pd.isna(pais):
            return None
        return CONTINENTES.get(pais.upper(), "No asociado")

    df["Producto"] = df.apply(
        lambda row: segmentar_jengibre(
            row.get("Descripción de Mercadería", ""),
            identificar_producto(
                row.get("Descripción de Mercadería", ""),
                row.get("Embarcador", ""),
                row.get("Consignatario", "")
            )
        ), axis=1
    )
    df["Pais"] = df["Puerto"].apply(identificar_pais)
    df["Ciudad destino"] = df["Puerto"].apply(identificar_ciudad_destino)
    df["Continente"] = df["Pais"].apply(identificar_continente)

    archivo_actualizado = os.path.join(directorio_base, "detalle_mercaderia_completo.xlsx")
    df.to_excel(archivo_actualizado, index=False)

    wb = load_workbook(archivo_actualizado)
    ws = wb.active
    tabla = Table(
        displayName="DetalleMercaderia",
        ref=f"A1:{chr(65 + len(df.columns) - 1)}{len(df) + 1}"
    )
    estilo = TableStyleInfo(
        name="TableStyleMedium9", showFirstColumn=False, showLastColumn=False,
        showRowStripes=True, showColumnStripes=True
    )
    tabla.tableStyleInfo = estilo
    ws.add_table(tabla)
    wb.save(archivo_actualizado)

    print(f"Archivo actualizado y convertido a tabla guardado como '{archivo_actualizado}'.")