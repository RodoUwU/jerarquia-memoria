"""
config.py
=========
Archivo de configuración central de la aplicación.
Aquí puedes ajustar las rutas de tus archivos CSV y los puertos del servidor Kax
sin necesidad de tocar el resto del código.
"""

from pathlib import Path

# ==============================================================================
# 1. RUTAS DE CARPETAS (Consumo de archivos y Almacenamiento de resultados)
# ==============================================================================
# Carpeta base donde se encuentra este proyecto
BASE_DIR = Path(__file__).resolve().parent

# Carpeta donde debes colocar los 4 archivos CSV (ruta de consumo)
# La documentación de Kax recomienda una carpeta tipo /home/usuario/files
# cuyo tamaño total no supere la memoria RAM disponible (~61 MB en este proyecto).
FILES_DIR = BASE_DIR / "files"

# Carpeta donde se guardarán los reportes y archivos recuperados de memoria compartida
# (ruta de almacenamiento, equivalente a /home/usuario/store).
STORE_DIR = BASE_DIR / "store"

# Nombres esperados de los archivos CSV del proyecto X60-X84
ARCHIVOS_EGRESOS = [
    "Egresos2023_X60_X84.csv",
    "Egresos2024_X60_X84.csv",
    "Egresos2025_X60_X84.csv",
]
ARCHIVO_MORTALIDAD = "mortality_suicidios_X60_X84.csv"

# ==============================================================================
# 2. CONFIGURACIÓN DEL SERVIDOR DE LA BIBLIOTECA KAX (Requiere VPN institucional)
# ==============================================================================
# Según "NUEVO DOC BIBLIOTECA.pdf":
#   - Control de metadata: IP 148.247.201.144, puertos [5061, ..., 5069]
#   - Almacenamiento:      IP 148.247.201.144, puertos [5071, ..., 5079]
#   - Broker Pub/Sub:      IP 148.247.201.144, puertos [1881, ..., 1889]
#
# Si un puerto está ocupado por otro compañero, cambia el último dígito (del 1 al 9)
# en los tres puertos por igual (por ejemplo: 5062, 5072, 1882).
KAX_HOST = "148.247.201.144"
KAX_REGISTRY_PORT = 5061
KAX_STORAGE_PORT = 5071
KAX_BROKER_PORT = 1881

KAX_CONFIG = {
    "registry_url": f"http://{KAX_HOST}:{KAX_REGISTRY_PORT}/api",
    "storage_url": f"http://{KAX_HOST}:{KAX_STORAGE_PORT}/api",
    "broker": KAX_HOST,
    "port": KAX_BROKER_PORT,
    "workers": 1,
    "chunk_size": 2,
}

# ==============================================================================
# 3. PARÁMETROS DE JERARQUÍA DE MEMORIA Y BUFFERING
# ==============================================================================
# Capacidad máxima de la Caché L1 (número de bloques/consultas frecuentes)
L1_CACHE_CAPACIDAD = 8

# Parámetros del patrón Productor-Consumidor (Tema 5.4 Buffering)
BUFFER_CAPACIDAD_MAX = 5          # Máximo de lotes en espera dentro del buffer
LOTE_TAMANO_FILAS = 2500          # Cantidad de filas CSV por cada lote enviado
MAX_LOTES_DEMO = 16               # Número de lotes a procesar en la demostración
TASA_PRODUCTOR_SEG = 0.015        # Tiempo entre envíos del productor (más bajo = más rápido)
TASA_CONSUMIDOR_EQUILIBRADO = 0.015
TASA_CONSUMIDOR_LENTO = 0.080     # Consumidor más lento para provocar overflow real


def asegurar_carpetas() -> None:
    """Crea las carpetas files/ y store/ si todavía no existen."""
    FILES_DIR.mkdir(parents=True, exist_ok=True)
    STORE_DIR.mkdir(parents=True, exist_ok=True)
