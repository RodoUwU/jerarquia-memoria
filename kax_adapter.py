"""
kax_adapter.py
==============
Adaptador para utilizar la biblioteca `kax` (memoria compartida local y servicio
en la nube de almacenamiento en memoria) siguiendo exactamente la documentación
de "NUEVO DOC BIBLIOTECA.pdf".

Además incluye un simulador local (`LocalKaxClient`) basado en
`multiprocessing.shared_memory` y disco local para que puedas probar y depurar
tu programa incluso si todavía no estás conectado al VPN institucional o si
corres pruebas sin acceso al servidor remoto.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from multiprocessing import shared_memory
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from config import KAX_CONFIG, STORE_DIR


def _extraer_hash_de_metadata(metadata: Any) -> Optional[str]:
    """
    Extrae de forma segura el identificador (hash) que devuelven
    `load_res`, `loadAndUploadToCloud`, `uploadToCloudFromDisk` o `download_from_cloud`.
    """
    if isinstance(metadata, list) and metadata:
        metadata = metadata[0]
    if isinstance(metadata, dict):
        for clave in ("hash", "id", "key", "file_hash", "shm_id", "name"):
            if clave in metadata and metadata[clave]:
                return str(metadata[clave])
        # Si viene anidado por nombre de archivo: {"archivo.csv": {"hash": "..."}}
        for valor in metadata.values():
            if isinstance(valor, (dict, list)):
                sub = _extraer_hash_de_metadata(valor)
                if sub:
                    return sub
    if isinstance(metadata, str):
        return metadata
    return None


class LocalKaxClient:
    """
    Respaldo local con la misma interfaz que `from kax import Kax`.
    Usa memoria compartida real del sistema operativo (`multiprocessing.shared_memory`)
    para los segmentos locales y una subcarpeta `store/_cloud_simulada` para emular
    el almacenamiento en la nube con latencia de red realista.
    """

    def __init__(self, **kwargs: Any) -> None:
        self.config = kwargs
        self._shm_handles: Dict[str, shared_memory.SharedMemory] = {}
        self._shm_sizes: Dict[str, int] = {}
        self._cloud_dir = STORE_DIR / "_cloud_simulada"
        self._cloud_dir.mkdir(parents=True, exist_ok=True)
        self._cloud_segments: Dict[str, bytes] = {}

    @staticmethod
    def _hash_bytes(contenido: bytes, nombre: str) -> str:
        h = hashlib.sha256()
        h.update(nombre.encode("utf-8"))
        h.update(contenido[:4096])
        h.update(str(len(contenido)).encode("utf-8"))
        return f"kax_{h.hexdigest()[:16]}"

    def _resolver_archivos(self, ruta: Union[str, Path]) -> List[Path]:
        p = Path(ruta)
        if p.is_dir():
            return sorted([f for f in p.iterdir() if f.is_file()])
        if p.is_file():
            return [p]
        raise FileNotFoundError(f"No se encontró la ruta: {ruta}")

    def _escribir_shm_local(self, shm_id: str, data: bytes) -> None:
        # Si ya existe un segmento previo con ese ID, lo liberamos primero
        if shm_id in self._shm_handles:
            self.removeShm(shm_id)
        try:
            shm = shared_memory.SharedMemory(name=shm_id, create=True, size=max(1, len(data)))
        except FileExistsError:
            viejo = shared_memory.SharedMemory(name=shm_id)
            viejo.close()
            viejo.unlink()
            shm = shared_memory.SharedMemory(name=shm_id, create=True, size=max(1, len(data)))
        shm.buf[: len(data)] = data
        self._shm_handles[shm_id] = shm
        self._shm_sizes[shm_id] = len(data)

    # --- FUNCIONES DE ALTO NIVEL (1 a 9 del PDF) ---

    def load_res(self, F: Union[str, Path]) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """1. Carga archivo(s) del disco local a memoria compartida local."""
        archivos = self._resolver_archivos(F)
        resultados = []
        for archivo in archivos:
            contenido = archivo.read_bytes()
            shm_id = self._hash_bytes(contenido, archivo.name)
            self._escribir_shm_local(shm_id, contenido)
            resultados.append(
                {
                    "hash": shm_id,
                    "filename": archivo.name,
                    "size_bytes": len(contenido),
                    "location": "local_shm",
                }
            )
        return resultados[0] if len(resultados) == 1 else resultados

    def loadAndUploadToCloud(self, F: Union[str, Path]) -> List[Dict[str, Any]]:
        """2. Carga en memoria compartida local y sube al servicio en la nube."""
        meta = self.load_res(F)
        metas = meta if isinstance(meta, list) else [meta]
        return self.uploadToCloudFromMemory(metas)

    def getRes(self, hash_id: str) -> bytes:
        """3. Obtiene los bytes del archivo desde la memoria compartida local."""
        if hash_id in self._shm_handles:
            shm = self._shm_handles[hash_id]
            tam = self._shm_sizes[hash_id]
            return bytes(shm.buf[:tam])
        # Permite que otro proceso distinto se conecte al mismo segmento SHM por nombre
        shm = shared_memory.SharedMemory(name=hash_id)
        try:
            return bytes(shm.buf[:]).rstrip(b"\x00")
        finally:
            shm.close()

    def uploadToCloudFromDisk(self, F: Union[str, Path]) -> List[Dict[str, Any]]:
        """4. Sube archivo(s) directo a la nube sin pasar por memoria compartida local."""
        archivos = self._resolver_archivos(F)
        resultados = []
        for archivo in archivos:
            contenido = archivo.read_bytes()
            shm_id = self._hash_bytes(contenido, archivo.name)
            time.sleep(0.025)  # Simula latencia de red hacia la nube
            (self._cloud_dir / f"{shm_id}.bin").write_bytes(contenido)
            resultados.append(
                {
                    "hash": shm_id,
                    "filename": archivo.name,
                    "size_bytes": len(contenido),
                    "location": "cloud",
                }
            )
        return resultados

    def uploadToCloudFromMemory(self, M: Union[Dict[str, Any], List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """5. Sube a la nube datos que ya se encuentran en memoria compartida local."""
        metas = M if isinstance(M, list) else [M]
        resultados = []
        for item in metas:
            shm_id = _extraer_hash_de_metadata(item)
            if not shm_id:
                continue
            data = self.getRes(shm_id)
            time.sleep(0.020)  # Simula latencia de transferencia a la nube
            (self._cloud_dir / f"{shm_id}.bin").write_bytes(data)
            nuevo = dict(item)
            nuevo["location"] = "local_shm+cloud"
            resultados.append(nuevo)
        return resultados

    def download_from_cloud(self, hash_id: str) -> Dict[str, Any]:
        """6. Descarga desde la nube hacia la memoria compartida local."""
        ruta_cloud = self._cloud_dir / f"{hash_id}.bin"
        if not ruta_cloud.exists():
            raise FileNotFoundError(f"El hash '{hash_id}' no existe en la nube simulada.")
        time.sleep(0.030)  # Simula latencia de descarga de red
        data = ruta_cloud.read_bytes()
        self._escribir_shm_local(hash_id, data)
        return {"hash": hash_id, "size_bytes": len(data), "location": "local_shm"}

    def saveFile(self, path: Union[str, Path], data: bytes, filename: str) -> str:
        """7. Guarda en el sistema de archivos local datos recuperados de SHM."""
        destino_dir = Path(path)
        destino_dir.mkdir(parents=True, exist_ok=True)
        destino = destino_dir / filename
        if isinstance(data, str):
            destino.write_text(data, encoding="utf-8")
        else:
            destino.write_bytes(data)
        return str(destino)

    def removeShm(self, hash_id: str) -> str:
        """8. Remueve un segmento de memoria compartida del entorno local."""
        shm = self._shm_handles.pop(hash_id, None)
        self._shm_sizes.pop(hash_id, None)
        if shm is not None:
            try:
                shm.close()
                shm.unlink()
            except FileNotFoundError:
                pass
        else:
            try:
                temp = shared_memory.SharedMemory(name=hash_id)
                temp.close()
                temp.unlink()
            except FileNotFoundError:
                pass
        return f"Segmento local '{hash_id}' eliminado correctamente."

    def removeShmFromCloud(self, hash_id: str) -> str:
        """9. Remueve un segmento de la nube y del registro."""
        ruta_cloud = self._cloud_dir / f"{hash_id}.bin"
        if ruta_cloud.exists():
            ruta_cloud.unlink()
        self._cloud_segments.pop(hash_id, None)
        return f"Segmento en nube '{hash_id}' eliminado correctamente."

    # --- FUNCIONES DE BAJO NIVEL (8 a 15 del PDF: segmentos dinámicos) ---

    def makeShm(self, name: str, size: int) -> str:
        """Crea un segmento de memoria compartida local con nombre y tamaño manual."""
        self._escribir_shm_local(name, b"\x00" * max(1, size))
        return name

    def putDataToShm(
        self,
        name: str,
        data: Union[bytes, str, Dict[str, Any]],
        init: Optional[int] = None,
        end: Optional[int] = None,
    ) -> None:
        """Inserta datos en un segmento de memoria compartida local."""
        raw = self._serializar(data)
        if name not in self._shm_handles:
            shm = shared_memory.SharedMemory(name=name)
            try:
                inicio = init or 0
                fin = end if end is not None else inicio + len(raw)
                shm.buf[inicio:fin] = raw[: fin - inicio]
            finally:
                shm.close()
            return
        shm = self._shm_handles[name]
        inicio = init or 0
        fin = end if end is not None else inicio + len(raw)
        if fin > shm.size:
            raise ValueError(f"Los datos ({len(raw)} B) exceden el tamaño del segmento ({shm.size} B).")
        shm.buf[inicio:fin] = raw[: fin - inicio]

    def getDataFromShm(self, name: str) -> bytes:
        """Lee los datos almacenados en un segmento de memoria compartida local."""
        if name in self._shm_handles:
            return bytes(self._shm_handles[name].buf[:]).rstrip(b"\x00")
        shm = shared_memory.SharedMemory(name=name)
        try:
            return bytes(shm.buf[:]).rstrip(b"\x00")
        finally:
            shm.close()

    def makeShmInCloud(self, name: str, size: int) -> Dict[str, Any]:
        """Crea un segmento de memoria compartida en la nube."""
        time.sleep(0.015)
        self._cloud_segments[name] = b"\x00" * max(1, size)
        return {"reserved": True, "name": name, "size": size}

    def putDataToShmInCloud(self, name: str, data: Union[bytes, str, Dict[str, Any]]) -> Dict[str, Any]:
        """Inserta datos en un segmento de memoria compartida en la nube."""
        time.sleep(0.020)
        self._cloud_segments[name] = self._serializar(data)
        return {"filled": True, "name": name}

    def getDataFromShmInCloud(self, name: str) -> bytes:
        """Obtiene datos de un segmento de memoria compartida en la nube."""
        time.sleep(0.020)
        if name not in self._cloud_segments:
            raise KeyError(f"El segmento en nube '{name}' no existe.")
        return self._cloud_segments[name]

    @staticmethod
    def _serializar(data: Union[bytes, str, Dict[str, Any]]) -> bytes:
        if isinstance(data, bytes):
            return data
        if isinstance(data, str):
            return data.encode("utf-8")
        return json.dumps(data, ensure_ascii=False).encode("utf-8")


class KaxManager:
    """
    Fachada unificada que conecta con la biblioteca real `kax` cuando `modo="kax"`,
    o utiliza `LocalKaxClient` cuando `modo="local"` (o si falla la conexión).
    También lleva el control de los hashes creados para limpiar memoria al finalizar.
    """

    def __init__(self, modo: str = "auto") -> None:
        self.modo_solicitado = modo
        self.modo_activo = "local"
        self.client: Any = None
        self._hashes_locales: List[str] = []
        self._hashes_nube: List[str] = []
        self._inicializar_cliente(modo)

    def _inicializar_cliente(self, modo: str) -> None:
        if modo in ("kax", "auto"):
            try:
                from kax import Kax  # type: ignore

                self.client = Kax(**KAX_CONFIG)
                self.modo_activo = "kax"
                print(
                    f"[KAX] Conectado a la biblioteca real Kax "
                    f"({KAX_CONFIG['registry_url']} | broker {KAX_CONFIG['broker']}:{KAX_CONFIG['port']})."
                )
                return
            except ImportError:
                if modo == "kax":
                    print(
                        "[AVISO] No se encontró la biblioteca 'kax' instalada en este entorno.\n"
                        "        Recuerda instalarla en Linux/Ubuntu (Python 3.10) con: pip install kax\n"
                        "        Se activará el MODO LOCAL de respaldo para que puedas probar todo."
                    )
            except Exception as exc:
                print(
                    f"[AVISO] No se pudo conectar al servidor Kax ({exc}).\n"
                    "        Verifica que tu VPN institucional esté activo y que los puertos en config.py estén libres.\n"
                    "        Se activará el MODO LOCAL de respaldo."
                )

        self.client = LocalKaxClient(**KAX_CONFIG)
        self.modo_activo = "local"
        print("[KAX-LOCAL] Usando motor de memoria compartida local (multiprocessing.shared_memory + nube simulada).")

    def registrar_hash_local(self, hash_id: Optional[str]) -> None:
        if hash_id and hash_id not in self._hashes_locales:
            self._hashes_locales.append(hash_id)

    def registrar_hash_nube(self, hash_id: Optional[str]) -> None:
        if hash_id and hash_id not in self._hashes_nube:
            self._hashes_nube.append(hash_id)

    def extraer_hash(self, metadata: Any) -> Optional[str]:
        return _extraer_hash_de_metadata(metadata)

    def limpiar_todo(self) -> None:
        """
        Libera los segmentos locales y en la nube creados durante la sesión.
        Cumple con la recomendación del PDF: usar `removeShmFromCloud(hash)` para
        evitar exceder el límite de memoria del servidor institucional.
        """
        for h in list(self._hashes_locales):
            try:
                self.client.removeShm(h)
            except Exception:
                pass
        for h in list(self._hashes_nube):
            try:
                self.client.removeShmFromCloud(h)
            except Exception:
                pass
        self._hashes_locales.clear()
        self._hashes_nube.clear()
