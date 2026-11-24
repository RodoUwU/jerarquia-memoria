"""
app.py
======
Punto de entrada principal de la Aplicación Integral de Gestión de la Jerarquía
de Memoria aplicada a datos epidemiológicos de suicidios y egresos hospitalarios
(códigos CIE-10: X60–X84).

Cómo ejecutar desde la terminal de Visual Studio Code:
  - Modo interactivo (menú paso a paso):
        python3 app.py
  - Ejecutar toda la demostración de una vez con Kax real (requiere VPN + Linux):
        python3 app.py --todo --modo kax
  - Ejecutar toda la demostración en modo local sin conexión al servidor:
        python3 app.py --todo --modo local
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime

from config import ARCHIVO_MORTALIDAD, ARCHIVOS_EGRESOS, FILES_DIR, STORE_DIR, asegurar_carpetas
from kax_adapter import KaxManager
from servicios.buffering_pipeline import ejecutar_servicio_buffering
from servicios.jerarquia_memoria import ejecutar_servicio_jerarquia
from servicios.memoria_compartida import ejecutar_servicio_memoria_compartida


def verificar_archivos_csv() -> bool:
    """Comprueba que los 4 archivos CSV estén dentro de la carpeta `files/`."""
    esperados = ARCHIVOS_EGRESOS + [ARCHIVO_MORTALIDAD]
    faltantes = [nombre for nombre in esperados if not (FILES_DIR / nombre).exists()]
    if faltantes:
        print("\n[ATENCIÓN] Faltan los siguientes archivos CSV en la carpeta 'files/':")
        for f in faltantes:
            print(f"   - {f}")
        print(f"\nPor favor, copia esos archivos en: {FILES_DIR}")
        return False
    return True


def ejecutar_demostracion_completa(modo: str) -> None:
    """Corre los 3 servicios en orden y genera un reporte final consolidado en `store/`."""
    if not verificar_archivos_csv():
        sys.exit(1)

    kax_mgr = KaxManager(modo=modo)
    try:
        r1 = ejecutar_servicio_jerarquia(kax_mgr)
        r2 = ejecutar_servicio_memoria_compartida(kax_mgr)
        r3 = ejecutar_servicio_buffering(kax_mgr)

        reporte_final = {
            "fecha_ejecucion": datetime.now().isoformat(timespec="seconds"),
            "modo_kax_utilizado": kax_mgr.modo_activo,
            "tema_5_2_jerarquia_memoria": r1,
            "tema_5_3_memoria_compartida": r2,
            "tema_5_4_buffering": r3,
        }

        ruta_consolidado = STORE_DIR / "reporte_integral_jerarquia_memoria.json"
        ruta_consolidado.write_text(
            json.dumps(reporte_final, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print("\n" + "=" * 72)
        print(" DEMOSTRACIÓN INTEGRAL COMPLETADA CON ÉXITO")
        print(f" Reporte consolidado guardado en: {ruta_consolidado}")
        print("=" * 72)
    finally:
        # Limpieza recomendada por la documentación de Kax (removeShm / removeShmFromCloud)
        print("\n[Limpieza] Liberando segmentos de memoria compartida local y en la nube...")
        kax_mgr.limpiar_todo()
        print("[Limpieza] Memoria liberada correctamente.")


def menu_interactivo(modo_inicial: str) -> None:
    """Menú sencillo en consola para ejecutar cada servicio paso a paso."""
    modo_actual = modo_inicial
    while True:
        print("\n" + "=" * 72)
        print(" APLICACIÓN INTEGRAL — JERARQUÍA DE MEMORIA (DATOS X60–X84)")
        print(f" Modo actual de Kax: {modo_actual.upper()} | Carpeta CSV: {FILES_DIR.name}/")
        print("=" * 72)
        print("  [1] Tema 5.2 — Jerarquía de Memoria (L1 -> SHM -> Disco -> Nube)")
        print("  [2] Tema 5.3 — Memoria Compartida e IPC entre Procesos (Kax)")
        print("  [3] Tema 5.4 — Buffering Productor-Consumidor y Overflow")
        print("  [4] Ejecutar los 3 servicios completos + Reporte Final")
        print("  [5] Cambiar modo (auto / kax / local)")
        print("  [0] Salir")

        opcion = input("\nElige una opción (0-5): ").strip()
        if opcion == "0":
            print("¡Hasta luego!")
            break
        elif opcion == "5":
            nuevo = input("Escribe el modo deseado ('kax', 'local' o 'auto'): ").strip().lower()
            if nuevo in ("kax", "local", "auto"):
                modo_actual = nuevo
                print(f"Modo cambiado a: {modo_actual.upper()}")
            else:
                print("Opción no válida. Usa 'kax', 'local' o 'auto'.")
        elif opcion in ("1", "2", "3", "4"):
            if not verificar_archivos_csv():
                continue
            if opcion == "4":
                ejecutar_demostracion_completa(modo_actual)
            else:
                kax_mgr = KaxManager(modo=modo_actual)
                try:
                    if opcion == "1":
                        ejecutar_servicio_jerarquia(kax_mgr)
                    elif opcion == "2":
                        ejecutar_servicio_memoria_compartida(kax_mgr)
                    elif opcion == "3":
                        ejecutar_servicio_buffering(kax_mgr)
                finally:
                    kax_mgr.limpiar_todo()
        else:
            print("Opción no reconocida. Por favor elige un número del 0 al 5.")


def main() -> None:
    asegurar_carpetas()
    parser = argparse.ArgumentParser(
        description="Aplicación de Jerarquía de Memoria, Memoria Compartida y Buffering (X60–X84)."
    )
    parser.add_argument(
        "--todo",
        action="store_true",
        help="Ejecuta los 3 servicios de forma automática sin abrir el menú interactivo.",
    )
    parser.add_argument(
        "--modo",
        choices=["auto", "kax", "local"],
        default="auto",
        help="auto: intenta usar Kax real y si falla usa respaldo local; kax: fuerza Kax; local: sin conexión.",
    )
    args = parser.parse_args()

    if args.todo:
        ejecutar_demostracion_completa(args.modo)
    else:
        menu_interactivo(args.modo)


if __name__ == "__main__":
    main()
