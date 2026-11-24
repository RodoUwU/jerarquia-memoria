# Aplicación Integral de Gestión de la Jerarquía de Memoria (CIE-10: X60–X84)

Proyecto modular en Python que implementa tres servicios integrados sobre registros epidemiológicos de egresos hospitalarios y mortalidad por lesiones autoinfligidas (X60–X84) utilizando la biblioteca institucional **`kax`**:

1. **Jerarquía de Memoria (`servicios/jerarquia_memoria.py`):** Caché L1 LRU (localidad temporal y *prefetching* espacial), memoria compartida local (`kax`), disco local y almacenamiento en memoria en la nube (`kax`).
2. **Memoria Compartida e IPC (`servicios/memoria_compartida.py`):** comunicación entre procesos (`multiprocessing.Process` + `Lock`) sin duplicar lecturas de disco y manejo de segmentos dinámicos locales y en la nube.
3. **Buffering Productor-Consumidor (`servicios/buffering_pipeline.py`):** lectura por bloques (*chunks*) de `mortality_suicidios_X60_X84.csv` con buffer acotado y comparación de flujo equilibrado frente a escenarios de *overflow*.

## Estructura del proyecto

```text
proyecto_jerarquia_memoria_x60_x84/
├── app.py                          # Punto de entrada (menú interactivo y CLI)
├── config.py                       # Rutas, puertos del servidor Kax y parámetros
├── kax_adapter.py                  # Adaptador para la biblioteca Kax + respaldo local
├── requirements.txt                # Dependencias (pandas, numpy, kax)
├── servicios/
│   ├── __init__.py
│   ├── jerarquia_memoria.py        # Servicio 1: L1 -> SHM -> Disco -> Nube
│   ├── memoria_compartida.py       # Servicio 2: IPC entre procesos y segmentos dinámicos
│   └── buffering_pipeline.py       # Servicio 3: Productor-Consumidor y Overflow
├── files/                          # Colocar aquí los 4 archivos CSV
└── store/                          # Reportes JSON generados al ejecutar
```

## Requisitos previos

- **Sistema operativo:** Linux (Ubuntu) o Windows 10/11 con **WSL (Ubuntu)** instalado.
- **Editor:** Visual Studio Code con las extensiones oficiales de Microsoft **WSL** y **Python**.
- **Red:** VPN institucional activo para conectarse al servidor `kax` (`148.247.201.144`).
- **Archivos de datos:** copiar los 4 CSV dentro de la carpeta `files/`:
  - `Egresos2023_X60_X84.csv`
  - `Egresos2024_X60_X84.csv`
  - `Egresos2025_X60_X84.csv`
  - `mortality_suicidios_X60_X84.csv`

## Cómo abrir el proyecto en Visual Studio Code (WSL / Ubuntu)

1. Haz clic en el botón azul de la esquina inferior izquierda de VS Code (`><`) y elige **Connect to WSL** (espera a que diga **`WSL: Ubuntu`**).
2. Ve a **File → Open Folder** (no hagas clic en *Show Local*).
3. En la barra de ruta escribe `/mnt/c/Users/` y navega hasta la carpeta `proyecto_jerarquia_memoria_x60_x84`, luego haz clic en **OK**.
4. Abre la terminal integrada (**Terminal → New Terminal**). Confirma que la línea termine en `$` (Ubuntu) y no en `PS C:\...>`.

## Instalación de dependencias (primera vez)

Ejecuta estos comandos uno por uno en la terminal de Ubuntu dentro de VS Code:

```bash
sudo apt update
sudo apt install -y python3 python3-pip python3-venv
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### Ajuste de compatibilidad de `kax` (solo la primera vez)

Si al instalar `kax` aparece un error `NameError: name 'self' is not defined` en `kax/mem.py`, corrígelo con este comando (reemplaza `python3.14` por tu versión de Python si es distinta, por ejemplo `python3.10`):

```bash
sed -i 's/register(self, /register(/g' .venv/lib/python3.*/site-packages/kax/mem.py
```

## Configuración de puertos del servidor `kax`

En `config.py` están configurados los puertos de prueba del servidor institucional (`148.247.201.144`):

- Control de metadata (`KAX_REGISTRY_PORT`): `5061` (rango disponible: `5061–5069`)
- Almacenamiento (`KAX_STORAGE_PORT`): `5071` (rango disponible: `5071–5079`)
- Broker Pub/Sub (`KAX_BROKER_PORT`): `1881` (rango disponible: `1881–1889`)

Si algún puerto está ocupado, cambia el último dígito en los tres puertos por igual (por ejemplo: `5062`, `5072` y `1882`) y guarda el archivo.

## Cómo ejecutar los programas

Asegúrate de tener el **VPN institucional conectado** y el entorno virtual activo (`source .venv/bin/activate`, verás `(.venv)` al inicio de la terminal):

- **Ejecutar los 3 servicios completos con `kax` (salida limpia sin avisos de sistema):**
  ```bash
  python3 -W ignore app.py --todo --modo kax
  ```
- **Abrir el menú interactivo paso a paso (elegir cada servicio individualmente):**
  ```bash
  python3 -W ignore app.py
  ```
  - Opción `1`: Jerarquía de Memoria (L1 → SHM → Disco → Nube)
  - Opción `2`: Memoria Compartida e IPC entre Procesos
  - Opción `3`: Buffering Productor-Consumidor y Overflow
  - Opción `4`: Ejecutar los 3 servicios completos y generar el reporte consolidado
  - Opción `5`: Cambiar entre modo `kax`, `local` o `auto`
- **Ejecutar en modo local sin conexión al servidor (pruebas sin VPN):**
  ```bash
  python3 -W ignore app.py --todo --modo local
  ```

## Resultados generados

Al finalizar, la aplicación libera automáticamente los segmentos de memoria compartida (`removeShm` y `removeShmFromCloud`) y guarda los resultados en la carpeta `store/`:

- `store/resumen_egresos_x60_x84_shm.json` — Estadísticas de egresos 2023–2025 calculadas por el proceso hijo desde memoria compartida.
- `store/reporte_buffering_x60_x84.json` — Métricas del Productor-Consumidor en escenario equilibrado y escenario de *overflow*.
- `store/reporte_integral_jerarquia_memoria.json` — Consolidado completo de los tres servicios con latencias, *hit rate*, conteos IPC y *throughput*.
