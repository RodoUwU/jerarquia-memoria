"""
servicios/memoria_compartida.py
===============================
TEMA 5.3 — TIPOS DE MANEJO: MEMORIA COMPARTIDA Y COMUNICACIÓN ENTRE PROCESOS (IPC)
Implementa dos contextos complementarios con procesos independientes (`multiprocessing`):
  - Contexto A (Alto nivel con Kax):
      El Proceso Cargador sube los archivos de egresos X60–X84 a memoria compartida
      (`load_res` / `uploadToCloudFromMemory`) y comparte únicamente los metadatos (hashes)
      con el Proceso Analizador, que lee directo desde memoria compartida (`getRes`),
      calcula estadísticas epidemiológicas y guarda el resultado con `saveFile`.
  - Contexto B (Bajo nivel con segmentos manuales de Kax):
      Múltiples procesos escriben y leen métricas dinámicas en segmentos de memoria
      compartida creados manualmente (`makeShm`, `putDataToShm`, `getDataFromShm`
      y su contraparte remota `makeShmInCloud`, `putDataToShmInCloud`, `getDataFromShmInCloud`)
      usando exclusión mutua (`Lock`) para evitar condiciones de carrera.
"""

from __future__ import annotations

import io
import json
import multiprocessing as mp
import os
from multiprocessing import shared_memory
from typing import Any, Dict, List

import pandas as pd

from config import ARCHIVOS_EGRESOS, FILES_DIR, STORE_DIR
from kax_adapter import KaxManager


def _proceso_analizador_ipc(
    cola_metadatos: mp.Queue,
    cola_resultados: mp.Queue,
    shm_estado_nombre: str,
    lock: mp.Lock,
) -> None:
    """
    Proceso independiente (PID distinto) que recibe los nombres de segmentos de
    memoria compartida del proceso principal, lee los CSV directamente desde RAM
    compartida sin volver a leer del disco, calcula indicadores epidemiológicos
    y actualiza un segmento de estado compartido con Lock.
    """
    pid_actual = os.getpid()
    estadisticas_por_anio: List[Dict[str, Any]] = []

    while True:
        mensaje = cola_metadatos.get()
        if mensaje is None:  # Señal de fin (sentinel)
            break

        anio = mensaje["anio"]
        shm_name = mensaje["shm_name"]
        tamano = mensaje["size_bytes"]

        # Nos conectamos al segmento de memoria compartida creado por el proceso productor
        shm = shared_memory.SharedMemory(name=shm_name)
        try:
            datos_bytes = bytes(shm.buf[:tamano])
        finally:
            shm.close()

        df = pd.read_csv(io.BytesIO(datos_bytes))
        total = int(len(df))
        hombres = int((df["SEXO"] == 1).sum())
        mujeres = int((df["SEXO"] == 2).sum())
        edad_prom = round(float(df["EDAD"].replace(999, pd.NA).dropna().mean()), 2)
        estancia_prom = round(float(df["DIAS_ESTA"].mean()), 2)
        top_causas = (
            df["CAUSAEXT"]
            .astype(str)
            .str[:3]
            .value_counts()
            .head(3)
            .to_dict()
        )

        resumen_anio = {
            "anio": anio,
            "pid_analizador": pid_actual,
            "total_egresos": total,
            "hombres": hombres,
            "mujeres": mujeres,
            "edad_promedio": edad_prom,
            "dias_estancia_promedio": estancia_prom,
            "top_3_causas_cie10": {str(k): int(v) for k, v in top_causas.items()},
        }
        estadisticas_por_anio.append(resumen_anio)

        # Actualizamos el segmento de estado dinámico compartido usando Lock (exclusión mutua)
        with lock:
            shm_estado = shared_memory.SharedMemory(name=shm_estado_nombre)
            try:
                actual_raw = bytes(shm_estado.buf[:]).rstrip(b"\x00").decode("utf-8")
                estado = json.loads(actual_raw) if actual_raw else {"procesados": [], "total_acumulado": 0}
                estado["procesados"].append(anio)
                estado["total_acumulado"] += total
                estado["ultimo_pid"] = pid_actual
                nuevo_bytes = json.dumps(estado, ensure_ascii=False).encode("utf-8")
                shm_estado.buf[:] = b"\x00" * shm_estado.size
                shm_estado.buf[: len(nuevo_bytes)] = nuevo_bytes
            finally:
                shm_estado.close()

    cola_resultados.put(estadisticas_por_anio)


def ejecutar_servicio_memoria_compartida(kax_mgr: KaxManager) -> Dict[str, Any]:
    """
    Ejecuta la demostración completa de Memoria Compartida (Tema 5.3) en dos contextos:
      1) Comunicación real entre 2 procesos (IPC) compartiendo los CSV de Egresos X60–X84.
      2) Gestión de segmentos dinámicos locales y en la nube con la API de `kax`.
    """
    print("\n" + "=" * 72)
    print(" SERVICIO 2 — MEMORIA COMPARTIDA Y COMUNICACIÓN ENTRE PROCESOS (Tema 5.3)")
    print("=" * 72)

    client = kax_mgr.client
    pid_principal = os.getpid()
    print(f"\n[Contexto A] Comunicación entre procesos (PID Principal: {pid_principal})...")

    # Creamos un segmento dinámico de estado (4 KB) para sincronización entre procesos
    shm_estado_nombre = f"x60_estado_{pid_principal}"
    shm_estado = shared_memory.SharedMemory(name=shm_estado_nombre, create=True, size=4096)
    shm_estado.buf[:] = b"\x00" * 4096

    cola_metadatos: mp.Queue = mp.Queue()
    cola_resultados: mp.Queue = mp.Queue()
    lock = mp.Lock()

    proceso_hijo = mp.Process(
        target=_proceso_analizador_ipc,
        args=(cola_metadatos, cola_resultados, shm_estado_nombre, lock),
    )
    proceso_hijo.start()

    segmentos_ipc_temporales: List[shared_memory.SharedMemory] = []
    hashes_kax_archivos: List[Dict[str, Any]] = []

    try:
        for nombre in ARCHIVOS_EGRESOS:
            ruta = FILES_DIR / nombre
            if not ruta.exists():
                raise FileNotFoundError(f"Falta {nombre} en {FILES_DIR}")

            anio = ruta.stem.replace("Egresos", "").split("_")[0]

            # 1) Cargamos el archivo con Kax en memoria compartida local y lo subimos a la nube
            meta_local = client.load_res(str(ruta))
            hash_kax = kax_mgr.extraer_hash(meta_local)
            kax_mgr.registrar_hash_local(hash_kax)
            client.uploadToCloudFromMemory(meta_local)
            kax_mgr.registrar_hash_nube(hash_kax)
            hashes_kax_archivos.append({"archivo": nombre, "hash": hash_kax})

            # 2) Exponemos el contenido en un segmento SHM del SO para el proceso hijo
            contenido = client.getRes(hash_kax)
            if isinstance(contenido, str):
                contenido = contenido.encode("utf-8")

            seg_name = f"x60_egresos_{anio}_{pid_principal}"
            seg = shared_memory.SharedMemory(name=seg_name, create=True, size=len(contenido))
            seg.buf[: len(contenido)] = contenido
            segmentos_ipc_temporales.append(seg)

            # Solo enviamos metadatos (nombre y tamaño), NO copiamos el archivo por la cola
            cola_metadatos.put({"anio": anio, "shm_name": seg_name, "size_bytes": len(contenido)})

        # Señal de terminación al proceso analizador
        cola_metadatos.put(None)
        estadisticas_ipc = cola_resultados.get(timeout=30)
        proceso_hijo.join(timeout=10)

        # Leemos el segmento de estado dinámico actualizado por el proceso hijo
        estado_final_raw = bytes(shm_estado.buf[:]).rstrip(b"\x00").decode("utf-8")
        estado_compartido = json.loads(estado_final_raw)

        for item in estadisticas_ipc:
            print(
                f"  • Año {item['anio']} (PID Hijo {item['pid_analizador']}): "
                f"{item['total_egresos']:,} egresos | Hombres: {item['hombres']:,}, "
                f"Mujeres: {item['mujeres']:,} | Edad prom: {item['edad_promedio']} años | "
                f"Top causas: {item['top_3_causas_cie10']}"
            )
    finally:
        for seg in segmentos_ipc_temporales:
            try:
                seg.close()
                seg.unlink()
            except FileNotFoundError:
                pass
        try:
            shm_estado.close()
            shm_estado.unlink()
        except FileNotFoundError:
            pass

    # ------------------------------------------------------------------
    # CONTEXTO B: Segmentos dinámicos con API manual de Kax (Local y Nube)
    # ------------------------------------------------------------------
    print("\n[Contexto B] Segmentos dinámicos con Kax (makeShm / putDataToShm / Cloud)...")
    seg_dinamico_local = f"resumen_x60_local_{pid_principal}"
    seg_dinamico_nube = f"resumen_x60_nube_{pid_principal}"

    payload_resumen = {
        "proyecto": "Epidemiología X60-X84 (Suicidios y Egresos Hospitalarios)",
        "total_egresos_2023_2025": estado_compartido["total_acumulado"],
        "anios_procesados": estado_compartido["procesados"],
        "por_anio": estadisticas_ipc,
    }
    payload_bytes = json.dumps(payload_resumen, ensure_ascii=False, indent=2).encode("utf-8")

    # 1) Segmento manual en memoria compartida LOCAL de Kax
    client.makeShm(seg_dinamico_local, len(payload_bytes) + 64)
    kax_mgr.registrar_hash_local(seg_dinamico_local)
    client.putDataToShm(seg_dinamico_local, payload_bytes.decode("utf-8"))
    leido_local = client.getDataFromShm(seg_dinamico_local)

    # 2) Segmento manual en memoria compartida en la NUBE de Kax
    res_reserva = client.makeShmInCloud(seg_dinamico_nube, len(payload_bytes) + 64)
    kax_mgr.registrar_hash_nube(seg_dinamico_nube)
    res_llenado = client.putDataToShmInCloud(seg_dinamico_nube, payload_bytes.decode("utf-8"))
    leido_nube = client.getDataFromShmInCloud(seg_dinamico_nube)

    # 3) Guardamos el resultado recuperado en la carpeta `store/` usando `client.saveFile`
    ruta_guardada = client.saveFile(
        str(STORE_DIR),
        leido_local if isinstance(leido_local, (bytes, bytearray)) else payload_bytes,
        "resumen_egresos_x60_x84_shm.json",
    )

    print(f"  • Segmento local Kax '{seg_dinamico_local}': {len(leido_local)} bytes leídos correctamente.")
    print(f"  • Segmento nube Kax '{seg_dinamico_nube}': reserva={res_reserva} | llenado={res_llenado} | {len(leido_nube)} bytes recuperados.")
    print(f"  • Archivo persistido con saveFile() en: {ruta_guardada}")

    return {
        "servicio": "5.3 Memoria Compartida e IPC",
        "modo_kax": kax_mgr.modo_activo,
        "pid_principal": pid_principal,
        "estado_compartido_ipc": estado_compartido,
        "estadisticas_egresos": estadisticas_ipc,
        "hashes_kax_archivos": hashes_kax_archivos,
        "archivo_guardado": str(ruta_guardada),
    }
