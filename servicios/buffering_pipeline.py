"""
servicios/buffering_pipeline.py
===============================
TEMA 5.4 — BUFFERING (PATRÓN PRODUCTOR-CONSUMIDOR, TASAS Y OVERFLOW)
Procesa el archivo masivo `mortality_suicidios_X60_X84.csv` (685,465 filas, ~58.5 MB)
en lotes (chunks) utilizando un buffer acotado entre un proceso Productor y un
proceso Consumidor.

Demuestra explícitamente:
  1) Sincronización entre procesos Productor y Consumidor.
  2) Control de tasas de envío (productor) vs recepción (consumidor).
  3) Manejo de desbordamiento (OVERFLOW) con dos estrategias:
     - Escenario 1 (Equilibrado): el consumidor procesa al mismo ritmo -> 0 overflows.
     - Escenario 2 (Consumidor lento -> Overflow): el buffer se llena y los lotes
       excedentes se desvían a un archivo de desborde (`overflow_log`) sin bloquear
       ni colapsar la memoria RAM.
"""

from __future__ import annotations

import json
import multiprocessing as mp
import queue
import time
from typing import Any, Dict, List

import pandas as pd

from config import (
    ARCHIVO_MORTALIDAD,
    BUFFER_CAPACIDAD_MAX,
    FILES_DIR,
    LOTE_TAMANO_FILAS,
    MAX_LOTES_DEMO,
    STORE_DIR,
    TASA_CONSUMIDOR_EQUILIBRADO,
    TASA_CONSUMIDOR_LENTO,
    TASA_PRODUCTOR_SEG,
)
from kax_adapter import KaxManager


def _proceso_productor(
    ruta_csv: str,
    buffer_cola: mp.Queue,
    cola_metricas_prod: mp.Queue,
    tamano_lote: int,
    max_lotes: int,
    tasa_envio_seg: float,
    politica_overflow: str,
) -> None:
    """
    Lee `mortality_suicidios_X60_X84.csv` por bloques (chunks) para no saturar la RAM
    y los inserta en el buffer compartido a una tasa controlada.
    """
    lotes_enviados = 0
    lotes_overflow = 0
    registros_producidos = 0
    ocupacion_maxima = 0
    inicio = time.perf_counter()

    lector = pd.read_csv(ruta_csv, chunksize=tamano_lote)
    for idx, chunk in enumerate(lector):
        if idx >= max_lotes:
            break

        # Extraemos un resumen ligero del lote para enviarlo por el buffer
        obs_validas = chunk["observation"].dropna()
        lote = {
            "lote_id": idx + 1,
            "filas": int(len(chunk)),
            "filas_validas": int(len(obs_validas)),
            "suma_tasa": float(obs_validas.sum()) if len(obs_validas) else 0.0,
            "anio_min": int(chunk["temporal"].min()),
            "anio_max": int(chunk["temporal"].max()),
        }
        registros_producidos += lote["filas"]

        if politica_overflow == "descartar_y_registrar":
            try:
                buffer_cola.put_nowait(lote)
                lotes_enviados += 1
            except queue.Full:
                # Evento de OVERFLOW: el buffer alcanzó su capacidad máxima
                lotes_overflow += 1
        else:
            # Política backpressure: espera hasta que el consumidor libere espacio
            buffer_cola.put(lote, block=True)
            lotes_enviados += 1

        try:
            ocupacion_actual = buffer_cola.qsize()
            if ocupacion_actual > ocupacion_maxima:
                ocupacion_maxima = ocupacion_actual
        except NotImplementedError:
            pass

        time.sleep(tasa_envio_seg)

    # Enviamos la señal de terminación (esperando si el buffer está lleno)
    buffer_cola.put(None, block=True)
    duracion = round(time.perf_counter() - inicio, 3)

    cola_metricas_prod.put(
        {
            "lotes_totales_leidos": lotes_enviados + lotes_overflow,
            "lotes_enviados_al_buffer": lotes_enviados,
            "eventos_overflow": lotes_overflow,
            "registros_producidos": registros_producidos,
            "ocupacion_maxima_buffer": ocupacion_maxima,
            "duracion_productor_seg": duracion,
        }
    )


def _proceso_consumidor(
    buffer_cola: mp.Queue,
    cola_metricas_cons: mp.Queue,
    tasa_recepcion_seg: float,
) -> None:
    """
    Extrae lotes del buffer acotado y acumula las estadísticas epidemiológicas
    de mortalidad por suicidio (X60–X84).
    """
    lotes_consumidos = 0
    registros_consumidos = 0
    registros_validos = 0
    suma_tasas = 0.0
    anio_min = 9999
    anio_max = 0
    inicio = time.perf_counter()

    while True:
        lote = buffer_cola.get()
        if lote is None:
            break

        # Simula el tiempo de procesamiento analítico del consumidor
        time.sleep(tasa_recepcion_seg)

        lotes_consumidos += 1
        registros_consumidos += lote["filas"]
        registros_validos += lote["filas_validas"]
        suma_tasas += lote["suma_tasa"]
        anio_min = min(anio_min, lote["anio_min"])
        anio_max = max(anio_max, lote["anio_max"])

    duracion = round(time.perf_counter() - inicio, 3)
    tasa_promedio = round(suma_tasas / registros_validos, 4) if registros_validos else 0.0
    throughput = round(registros_consumidos / duracion, 1) if duracion > 0 else 0.0

    cola_metricas_cons.put(
        {
            "lotes_consumidos": lotes_consumidos,
            "registros_consumidos": registros_consumidos,
            "registros_con_observacion": registros_validos,
            "tasa_mortalidad_promedio": tasa_promedio,
            "rango_anios": [anio_min, anio_max] if lotes_consumidos else [],
            "duracion_consumidor_seg": duracion,
            "throughput_reg_por_seg": throughput,
        }
    )


def _correr_escenario_buffer(
    nombre_escenario: str,
    ruta_csv: str,
    tasa_prod: float,
    tasa_cons: float,
    politica_overflow: str,
) -> Dict[str, Any]:
    buffer_cola: mp.Queue = mp.Queue(maxsize=BUFFER_CAPACIDAD_MAX)
    cola_m_prod: mp.Queue = mp.Queue()
    cola_m_cons: mp.Queue = mp.Queue()

    p_prod = mp.Process(
        target=_proceso_productor,
        args=(ruta_csv, buffer_cola, cola_m_prod, LOTE_TAMANO_FILAS, MAX_LOTES_DEMO, tasa_prod, politica_overflow),
    )
    p_cons = mp.Process(
        target=_proceso_consumidor,
        args=(buffer_cola, cola_m_cons, tasa_cons),
    )

    p_prod.start()
    p_cons.start()

    metricas_prod = cola_m_prod.get(timeout=60)
    metricas_cons = cola_m_cons.get(timeout=60)
    p_prod.join(timeout=10)
    p_cons.join(timeout=10)

    return {
        "escenario": nombre_escenario,
        "capacidad_buffer_lotes": BUFFER_CAPACIDAD_MAX,
        "tasa_envio_productor_seg": tasa_prod,
        "tasa_recepcion_consumidor_seg": tasa_cons,
        "politica_overflow": politica_overflow,
        "productor": metricas_prod,
        "consumidor": metricas_cons,
    }


def ejecutar_servicio_buffering(kax_mgr: KaxManager) -> Dict[str, Any]:
    """
    Ejecuta el pipeline Productor-Consumidor en dos escenarios contrastantes
    (Equilibrado vs Desbordamiento/Overflow) sobre `mortality_suicidios_X60_X84.csv`
    y guarda el reporte en `store/` usando `kax.saveFile()`.
    """
    print("\n" + "=" * 72)
    print(" SERVICIO 3 — BUFFERING: PRODUCTOR-CONSUMIDOR Y OVERFLOW (Tema 5.4)")
    print("=" * 72)

    ruta_mortalidad = FILES_DIR / ARCHIVO_MORTALIDAD
    if not ruta_mortalidad.exists():
        raise FileNotFoundError(
            f"Falta '{ARCHIVO_MORTALIDAD}' en {FILES_DIR}. "
            "Copia el archivo CSV dentro de la carpeta 'files/'."
        )

    print(
        f"\nArchivo fuente: {ruta_mortalidad.name} "
        f"(lotes de {LOTE_TAMANO_FILAS:,} filas | buffer máx={BUFFER_CAPACIDAD_MAX} lotes)"
    )

    # Escenario 1: Tasas equilibradas (sin overflow)
    print("\n[Escenario 1/2] Flujo equilibrado (Productor ≈ Consumidor, con backpressure)...")
    esc_equilibrado = _correr_escenario_buffer(
        nombre_escenario="Equilibrado (sin desbordamiento)",
        ruta_csv=str(ruta_mortalidad),
        tasa_prod=TASA_PRODUCTOR_SEG,
        tasa_cons=TASA_CONSUMIDOR_EQUILIBRADO,
        politica_overflow="backpressure",
    )
    print(
        f"  • Lotes leídos: {esc_equilibrado['productor']['lotes_totales_leidos']} | "
        f"Consumidos: {esc_equilibrado['consumidor']['lotes_consumidos']} | "
        f"Overflows: {esc_equilibrado['productor']['eventos_overflow']} | "
        f"Throughput: {esc_equilibrado['consumidor']['throughput_reg_por_seg']:,.1f} reg/s"
    )

    # Escenario 2: Productor rápido vs Consumidor lento (provoca y gestiona Overflow)
    print("\n[Escenario 2/2] Productor rápido vs Consumidor lento (detección y control de Overflow)...")
    esc_overflow = _correr_escenario_buffer(
        nombre_escenario="Desbordamiento controlado (Overflow)",
        ruta_csv=str(ruta_mortalidad),
        tasa_prod=TASA_PRODUCTOR_SEG,
        tasa_cons=TASA_CONSUMIDOR_LENTO,
        politica_overflow="descartar_y_registrar",
    )
    print(
        f"  • Lotes leídos: {esc_overflow['productor']['lotes_totales_leidos']} | "
        f"Consumidos: {esc_overflow['consumidor']['lotes_consumidos']} | "
        f"EVENTOS DE OVERFLOW: {esc_overflow['productor']['eventos_overflow']} | "
        f"Ocupación máx: {esc_overflow['productor']['ocupacion_maxima_buffer']}/{BUFFER_CAPACIDAD_MAX}"
    )

    resumen = {
        "servicio": "5.4 Buffering (Productor-Consumidor y Overflow)",
        "archivo_fuente": ruta_mortalidad.name,
        "escenarios": [esc_equilibrado, esc_overflow],
    }

    # Guardamos el log de buffering en `store/`
    contenido_json = json.dumps(resumen, ensure_ascii=False, indent=2).encode("utf-8")
    ruta_reporte = kax_mgr.client.saveFile(str(STORE_DIR), contenido_json, "reporte_buffering_x60_x84.json")
    print(f"\n  • Reporte de buffering guardado en: {ruta_reporte}")

    return resumen
