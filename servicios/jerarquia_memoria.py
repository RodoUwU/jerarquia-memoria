"""
servicios/jerarquia_memoria.py
==============================
TEMA 5.2 — JERARQUÍA DE MEMORIA
Implementa y mide el paso de los datos epidemiológicos X60–X84 a través de
cuatro niveles de memoria:
  - Nivel 1 (L1): Caché LRU en RAM del proceso (ultra baja latencia, baja capacidad).
                  Demuestra localidad temporal y localidad espacial (prefetching).
  - Nivel 2 (SHM): Memoria Compartida Local gestionada con `kax` (`load_res` / `getRes`).
  - Nivel 3 (Disco): Sistema de archivos local (`files/` y `store/`).
  - Nivel 4 (Nube): Servicio en la nube de almacenamiento en memoria (`kax`:
                    `loadAndUploadToCloud` / `download_from_cloud`).
"""

from __future__ import annotations

import io
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

from config import ARCHIVOS_EGRESOS, FILES_DIR, L1_CACHE_CAPACIDAD, STORE_DIR
from kax_adapter import KaxManager


class CacheL1LRU:
    """
    Caché L1 de capacidad limitada con política LRU (Least Recently Used).
    Cuando se llena, expulsa el bloque que lleva más tiempo sin consultarse.
    """

    def __init__(self, capacidad: int = L1_CACHE_CAPACIDAD) -> None:
        self.capacidad = capacidad
        self._datos: OrderedDict[str, Any] = OrderedDict()
        self.hits = 0
        self.misses = 0
        self.expulsiones = 0

    def obtener(self, clave: str) -> Any:
        if clave in self._datos:
            self.hits += 1
            # Localidad temporal: al usarlo, pasa al final como el más reciente
            self._datos.move_to_end(clave)
            return self._datos[clave]
        self.misses += 1
        return None

    def insertar(self, clave: str, valor: Any) -> None:
        if clave in self._datos:
            self._datos.move_to_end(clave)
        self._datos[clave] = valor
        if len(self._datos) > self.capacidad:
            self._datos.popitem(last=False)
            self.expulsiones += 1

    @property
    def tasa_aciertos(self) -> float:
        total = self.hits + self.misses
        return (self.hits / total * 100.0) if total > 0 else 0.0


def _medir_ms(inicio: float) -> float:
    return round((time.perf_counter() - inicio) * 1000.0, 3)


def ejecutar_servicio_jerarquia(kax_mgr: KaxManager) -> Dict[str, Any]:
    """
    Recorre los archivos de Egresos Hospitalarios X60–X84 (2023, 2024 y 2025)
    simulando el ciclo completo por los 4 niveles de la jerarquía de memoria
    y midiendo latencia vs capacidad, localidad temporal y localidad espacial.
    """
    print("\n" + "=" * 72)
    print(" SERVICIO 1 — JERARQUÍA DE MEMORIA (Tema 5.2)")
    print(" Niveles: L1 (Caché LRU) <-> SHM Local (Kax) <-> Disco <-> Cloud (Kax)")
    print("=" * 72)

    client = kax_mgr.client
    cache_l1 = CacheL1LRU(capacidad=L1_CACHE_CAPACIDAD)

    rutas_egresos: List[Path] = []
    for nombre in ARCHIVOS_EGRESOS:
        ruta = FILES_DIR / nombre
        if not ruta.exists():
            raise FileNotFoundError(
                f"Falta el archivo '{nombre}' en la carpeta {FILES_DIR}. "
                "Copia los 4 CSV dentro de la carpeta 'files/' antes de ejecutar."
            )
        rutas_egresos.append(ruta)

    tiempos_niveles: Dict[str, List[float]] = {
        "L1_cache_hit": [],
        "L2_shm_local": [],
        "L3_disco_local": [],
        "L4_cloud_subida": [],
        "L4_cloud_descarga": [],
    }
    hashes_por_anio: Dict[str, str] = {}
    resumen_archivos: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------
    # PASO A: Medir Nivel 3 (Disco) -> Nivel 2 (SHM Local) -> Nivel 4 (Nube)
    # ------------------------------------------------------------------
    print("\n[Paso 1/3] Midiendo latencias de Disco, Memoria Compartida Local y Nube (Kax)...")
    for ruta in rutas_egresos:
        anio = ruta.stem.replace("Egresos", "").split("_")[0]

        # 1) Lectura desde Nivel 3: Disco Local
        t0 = time.perf_counter()
        raw_disco = ruta.read_bytes()
        lat_disco = _medir_ms(t0)
        tiempos_niveles["L3_disco_local"].append(lat_disco)

        # 2) Carga a Nivel 2 (SHM Local) + subida a Nivel 4 (Cloud) usando Kax
        t0 = time.perf_counter()
        meta_cloud = client.loadAndUploadToCloud(str(ruta))
        lat_cloud_up = _medir_ms(t0)
        tiempos_niveles["L4_cloud_subida"].append(lat_cloud_up)

        hash_id = kax_mgr.extraer_hash(meta_cloud)
        kax_mgr.registrar_hash_local(hash_id)
        kax_mgr.registrar_hash_nube(hash_id)
        if not hash_id:
            raise RuntimeError(f"No se pudo obtener el hash de metadata para {ruta.name}: {meta_cloud}")
        hashes_por_anio[anio] = hash_id

        # 3) Lectura desde Nivel 2: Memoria Compartida Local (`getRes`)
        t0 = time.perf_counter()
        raw_shm = client.getRes(hash_id)
        lat_shm = _medir_ms(t0)
        tiempos_niveles["L2_shm_local"].append(lat_shm)

        # 4) Simulamos que se libera SHM local y se recupera desde Nivel 4 (Cloud)
        client.removeShm(hash_id)
        t0 = time.perf_counter()
        client.download_from_cloud(hash_id)
        raw_cloud = client.getRes(hash_id)
        lat_cloud_down = _medir_ms(t0)
        tiempos_niveles["L4_cloud_descarga"].append(lat_cloud_down)

        df_tmp = pd.read_csv(io.BytesIO(raw_cloud if isinstance(raw_cloud, (bytes, bytearray)) else raw_disco))
        resumen_archivos.append(
            {
                "archivo": ruta.name,
                "anio": anio,
                "hash_kax": hash_id,
                "tamano_kb": round(len(raw_disco) / 1024, 1),
                "registros": int(len(df_tmp)),
                "lat_disco_ms": lat_disco,
                "lat_shm_ms": lat_shm,
                "lat_cloud_subida_ms": lat_cloud_up,
                "lat_cloud_descarga_ms": lat_cloud_down,
            }
        )
        print(
            f"  • {ruta.name} ({len(df_tmp):,} filas, {len(raw_disco)/1024:.1f} KB) | "
            f"SHM: {lat_shm:.2f} ms | Disco: {lat_disco:.2f} ms | "
            f"Nube↑: {lat_cloud_up:.2f} ms | Nube↓: {lat_cloud_down:.2f} ms"
        )

    # ------------------------------------------------------------------
    # PASO B: Demostrar Localidad Temporal y Localidad Espacial en Caché L1
    # ------------------------------------------------------------------
    print("\n[Paso 2/3] Demostrando Localidad Temporal y Espacial en Caché L1 (consultas X60–X84)...")
    # Cargamos los DataFrames desde la memoria compartida local (Nivel 2)
    dfs_por_anio: Dict[str, pd.DataFrame] = {}
    for anio, hash_id in hashes_por_anio.items():
        contenido = client.getRes(hash_id)
        dfs_por_anio[anio] = pd.read_csv(io.BytesIO(contenido))

    def consultar_causa_en_jerarquia(anio: str, prefijo_cie10: str, prefetch_vecinos: bool = True) -> Tuple[int, str, float]:
        """
        Busca el conteo de egresos para un año y causa CIE-10 (ej. 'X64').
        1) Revisa Caché L1 (hit -> ultra rápido).
        2) Si hay fallo (miss), consulta Nivel 2 (SHM) y carga en L1.
        3) Si `prefetch_vecinos=True`, aplica LOCALIDAD ESPACIAL precargando
           los otros años del mismo código en L1.
        """
        clave = f"{anio}:{prefijo_cie10}"
        t0 = time.perf_counter()
        en_l1 = cache_l1.obtener(clave)
        if en_l1 is not None:
            lat = _medir_ms(t0)
            tiempos_niveles["L1_cache_hit"].append(lat)
            return en_l1, "L1_HIT", lat

        # Fallo en L1 -> resolvemos desde Nivel 2 (Memoria Compartida)
        df = dfs_por_anio[anio]
        conteo = int(df["CAUSAEXT"].astype(str).str.startswith(prefijo_cie10).sum())
        cache_l1.insertar(clave, conteo)

        # Localidad espacial: precargamos años vecinos de la misma causa X60–X84
        if prefetch_vecinos:
            for otro_anio, otro_df in dfs_por_anio.items():
                if otro_anio != anio:
                    clave_vecina = f"{otro_anio}:{prefijo_cie10}"
                    if clave_vecina not in cache_l1._datos:
                        conteo_vecino = int(
                            otro_df["CAUSAEXT"].astype(str).str.startswith(prefijo_cie10).sum()
                        )
                        cache_l1.insertar(clave_vecina, conteo_vecino)

        lat = _medir_ms(t0)
        return conteo, "L1_MISS -> L2_SHM (+prefetch espacial)", lat

    # Secuencia de consultas que simula un analista revisando causas frecuentes
    consultas = [
        ("2023", "X64"),  # Miss -> carga 2023 y hace prefetch espacial de 2024 y 2025
        ("2024", "X64"),  # Hit por localidad espacial!
        ("2025", "X64"),  # Hit por localidad espacial!
        ("2023", "X64"),  # Hit por localidad temporal!
        ("2023", "X70"),  # Miss -> carga X70 + prefetch
        ("2024", "X70"),  # Hit por localidad espacial!
        ("2025", "X70"),  # Hit por localidad espacial!
        ("2023", "X78"),  # Miss -> fuerza expulsión LRU al superar capacidad=8
        ("2024", "X78"),  # Hit
        ("2025", "X78"),  # Hit
        ("2025", "X64"),  # Verifica estado tras expulsión LRU
    ]

    traza_consultas = []
    for anio, codigo in consultas:
        conteo, origen, lat = consultar_causa_en_jerarquia(anio, codigo, prefetch_vecinos=True)
        traza_consultas.append(
            {"anio": anio, "codigo": codigo, "egresos": conteo, "origen": origen, "lat_ms": lat}
        )
        print(f"  • Consulta {anio} [{codigo}] -> {conteo:4d} egresos | {origen:38s} | {lat:.3f} ms")

    # ------------------------------------------------------------------
    # PASO C: Resumen Latencia vs Capacidad y persistencia en `store/`
    # ------------------------------------------------------------------
    def _promedio(lista: List[float]) -> float:
        return round(sum(lista) / len(lista), 3) if lista else 0.0

    resumen = {
        "servicio": "5.2 Jerarquía de Memoria",
        "modo_kax": kax_mgr.modo_activo,
        "capacidad_l1_bloques": cache_l1.capacidad,
        "l1_hits": cache_l1.hits,
        "l1_misses": cache_l1.misses,
        "l1_expulsiones_lru": cache_l1.expulsiones,
        "l1_tasa_aciertos_pct": round(cache_l1.tasa_aciertos, 2),
        "latencia_promedio_ms": {
            "L1_cache_hit": _promedio(tiempos_niveles["L1_cache_hit"]),
            "L2_shm_local": _promedio(tiempos_niveles["L2_shm_local"]),
            "L3_disco_local": _promedio(tiempos_niveles["L3_disco_local"]),
            "L4_cloud_subida": _promedio(tiempos_niveles["L4_cloud_subida"]),
            "L4_cloud_descarga": _promedio(tiempos_niveles["L4_cloud_descarga"]),
        },
        "archivos": resumen_archivos,
        "consultas_localidad": traza_consultas,
    }

    print("\n[Paso 3/3] Resumen de Latencia vs Capacidad (Jerarquía de Memoria):")
    print(f"  • Caché L1 (cap={cache_l1.capacidad} bloques): {resumen['latencia_promedio_ms']['L1_cache_hit']:.3f} ms | "
          f"Hit Rate: {resumen['l1_tasa_aciertos_pct']}% (Hits={cache_l1.hits}, Misses={cache_l1.misses}, Expulsiones LRU={cache_l1.expulsiones})")
    print(f"  • SHM Local (Kax):            {resumen['latencia_promedio_ms']['L2_shm_local']:.3f} ms")
    print(f"  • Disco Local (CSV):          {resumen['latencia_promedio_ms']['L3_disco_local']:.3f} ms")
    print(f"  • Cloud Storage (Kax subida): {resumen['latencia_promedio_ms']['L4_cloud_subida']:.3f} ms")
    print(f"  • Cloud Storage (Kax bajada): {resumen['latencia_promedio_ms']['L4_cloud_descarga']:.3f} ms")

    return resumen
