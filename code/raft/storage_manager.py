#storage_manager.py
import os
from math import ceil
import Pyro5.api as rpc

from backend.settings import CHUNK_SIZE, STORAGE_ROOT, CHUNK_RANGES, RPC_TIMEOUT, CHUNK_SIZE


@rpc.expose
class StorageManager:
    """
    Clase que maneja archivos de audio en el almacenamiento local.
    Se usará como objeto remoto en un servidor Pyro5.
    """

    def __init__(self, storage_path: str = STORAGE_ROOT):
        self.storage_path = storage_path
        os.makedirs(self.storage_path, exist_ok=True)

    def list_files(self):
        """Devuelve la lista de archivos almacenados."""
        return sorted(os.listdir(self.storage_path))

    def exists(self, filename: str) -> bool:
        """Verifica si un archivo existe."""
        return os.path.isfile(self._file_path(filename))

    def create_file(self, filename: str, data: bytes):
        """Crea o sobrescribe un archivo con los datos binarios dados."""
        filepath = self._file_path(filename)
        with open(filepath, "wb") as f:
            f.write(data)
        return True

    def delete_file(self, filename: str) -> bool:
        """Elimina un archivo del almacenamiento local."""
        filepath = self._file_path(filename)
        if os.path.isfile(filepath):
            os.remove(filepath)
            return True
        return False

    def get_file_info(self, filename: str) -> dict:
        """Devuelve tamaño total y cantidad de chunks del archivo."""
        filepath = self._file_path(filename)
        if not os.path.isfile(filepath):
            raise FileNotFoundError(f"Archivo no encontrado: {filename}")
        size = os.path.getsize(filepath)
        chunks = ceil(size / CHUNK_SIZE)
        return {"filename": filename, "size": size, "chunks": chunks}

    def get_chunk(self, filename: str, chunk_index: int) -> bytes:
        """
        Devuelve un chunk específico del archivo (comienza en 0).
        """
        filepath = self._file_path(filename)
        if not os.path.isfile(filepath):
            raise FileNotFoundError(f"Archivo no encontrado: {filename}")

        with open(filepath, "rb") as f:
            f.seek(chunk_index * CHUNK_SIZE)
            chunk = f.read(CHUNK_SIZE)
        return chunk

    def _file_path(self, filename: str) -> str:
        """Devuelve la ruta absoluta del archivo dentro del storage."""
        return os.path.join(self.storage_path, filename)
    
    # HELPERS PARA CHUNK RANGE LOGIC
    
    def create_file_range(self, filename: str, data: bytes, range_id: str):
        """
        Crea un archivo con un rango específico de chunks.
        range_id: identificador del rango, ej: "0-78"
        """
        # Crear subdirectorio para el archivo si no existe
        file_dir = os.path.join(self.storage_path, filename)
        os.makedirs(file_dir, exist_ok=True)
        
        # Guardar el rango
        range_path = os.path.join(file_dir, f"range_{range_id}")
        with open(range_path, "wb") as f:
            f.write(data)
        
        return True

    def get_chunk_range(self, filename: str, range_id: str) -> bytes:
        """
        Obtiene todos los chunks de un rango específico.
        """
        file_dir = os.path.join(self.storage_path, filename)
        range_path = os.path.join(file_dir, f"range_{range_id}")
        
        if not os.path.isfile(range_path):
            raise FileNotFoundError(f"Rango {range_id} no encontrado para {filename}")
        
        with open(range_path, "rb") as f:
            return f.read()

    def delete_file_range(self, filename: str, range_id: str) -> bool:
        """
        Elimina un rango específico de chunks de un archivo.
        """
        file_dir = os.path.join(self.storage_path, filename)
        range_path = os.path.join(file_dir, f"range_{range_id}")
        
        if os.path.isfile(range_path):
            os.remove(range_path)
            
            # Si no quedan más rangos, eliminar el directorio
            if not os.listdir(file_dir):
                os.rmdir(file_dir)
            
            return True
        
        return False

    def has_file_range(self, filename: str, range_id: str) -> bool:
        """
        Verifica si existe un rango específico de un archivo.
        """
        file_dir = os.path.join(self.storage_path, filename)
        range_path = os.path.join(file_dir, f"range_{range_id}")
        return os.path.isfile(range_path)

    def get_file_ranges(self, filename: str) -> list:
        """
        Retorna la lista de rangos disponibles para un archivo.
        """
        file_dir = os.path.join(self.storage_path, filename)
        
        if not os.path.isdir(file_dir):
            return []
        
        ranges = []
        for file in os.listdir(file_dir):
            if file.startswith("range_"):
                range_id = file.replace("range_", "")
                ranges.append(range_id)
        
        return sorted(ranges)
    
    def read_chunks_delegated(
        self, filename: str, start_chunk: int, chunk_count: int, global_index: dict
    ) -> list:
        """
        Método expuesto para que el líder delegue lectura completa.
        Este nodo busca chunks locales y pide faltantes a otros nodos.
        """
        end_chunk = start_chunk + chunk_count
        all_chunks = [None] * chunk_count
        
        # Obtener info del archivo del índice global
        file_metadata = global_index.get("files_metadata", {}).get(filename)
        if not file_metadata:
            raise FileNotFoundError(f"Archivo {filename} no encontrado en índice")
        
        chunk_distribution = file_metadata.get("chunk_distribution", {})
        
        # 1. BUSCAR CHUNKS LOCALES
        from .utils import get_raft_server
        raft_server = get_raft_server()
        current_node_id = raft_server.host
        
        for range_key, nodes_with_range in chunk_distribution.items():
            if current_node_id not in nodes_with_range:
                continue
            
            range_start, range_end = map(int, range_key.split("-"))
            intersection_start = max(start_chunk, range_start)
            intersection_end = min(end_chunk, range_end)
            
            if intersection_start >= intersection_end:
                continue
            
            try:
                range_data = self.get_chunk_range(filename, range_key)
                
                for chunk_idx in range(intersection_start, intersection_end):
                    offset_in_range = chunk_idx - range_start
                    start_byte = offset_in_range * CHUNK_SIZE
                    end_byte = start_byte + CHUNK_SIZE
                    
                    chunk_data = range_data[start_byte:end_byte]
                    relative_idx = chunk_idx - start_chunk
                    all_chunks[relative_idx] = chunk_data
            
            except Exception:
                pass
        
        # 2. IDENTIFICAR Y PEDIR CHUNKS FALTANTES
        missing_chunks = [
            start_chunk + i 
            for i in range(chunk_count) 
            if all_chunks[i] is None
        ]
        
        if missing_chunks:
            # Agrupar por rango y pedir a otros nodos
            chunks_by_range = {}
            
            for chunk_idx in missing_chunks:
                for range_key, nodes_with_range in chunk_distribution.items():
                    range_start, range_end = map(int, range_key.split("-"))
                    
                    if range_start <= chunk_idx < range_end:
                        if range_key not in chunks_by_range:
                            chunks_by_range[range_key] = {
                                "chunks": [],
                                "nodes": [n for n in nodes_with_range if n != current_node_id]
                            }
                        chunks_by_range[range_key]["chunks"].append(chunk_idx)
                        break
            
            # Pedir rangos a otros nodos
            import Pyro5.api as rpc
            
            for range_key, info in chunks_by_range.items():
                chunks_needed = info["chunks"]
                remote_nodes = info["nodes"]
                
                if not remote_nodes:
                    continue
                
                # Seleccionar primer nodo disponible (ya optimizado por líder)
                selected_node = remote_nodes[0]
                
                try:
                    uri = f"PYRO:raft.storage.{selected_node}@{selected_node}:{raft_server.port}"
                    proxy = rpc.Proxy(uri)
                    proxy._pyroTimeout = 5.0
                    
                    range_data = proxy.get_chunk_range(filename, range_key)
                    range_start, range_end = map(int, range_key.split("-"))
                    
                    for chunk_idx in chunks_needed:
                        offset_in_range = chunk_idx - range_start
                        start_byte = offset_in_range * CHUNK_SIZE
                        end_byte = start_byte + CHUNK_SIZE
                        
                        chunk_data = range_data[start_byte:end_byte]
                        relative_idx = chunk_idx - start_chunk
                        all_chunks[relative_idx] = chunk_data
                
                except Exception:
                    pass
        
        # Verificar integridad
        if None in all_chunks:
            raise Exception(f"No se pudieron obtener todos los chunks")
        
        return all_chunks
