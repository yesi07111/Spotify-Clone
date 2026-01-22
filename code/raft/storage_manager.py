#storage_manager.py
import os
from math import ceil
from venv import logger
import Pyro5.api as rpc

from backend.settings import CHUNK_SIZE, STORAGE_ROOT, CHUNK_RANGES, RPC_TIMEOUT, CHUNK_SIZE
from raft.log_utils import log_info, log_error, log_warning, log_success

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
        import logging
        # Crear subdirectorio para el archivo si no existe
        if isinstance(data, dict):
            logger.info(data.keys())
        file_dir = os.path.join(self.storage_path, filename)
        os.makedirs(file_dir, exist_ok=True)
        
        log_info("STORAGE MANAGER","Se va a escribir el shard...", color="green")
        # Guardar el rango
        range_path = os.path.join(file_dir, f"range_{range_id}")
        with open(range_path, "wb") as f:
            f.write(data)
        
        log_info("STORAGE MANAGER", f"Se creo el shard de nombre {filename} con el range_id: {range_id}", color="green")
        
        return True

    def get_chunk_range(self, filename: str, range_id: str) -> bytes:
        """
        Obtiene todos los chunks de un rango específico.
        """
        file_dir = os.path.join(self.storage_path, filename)
        range_path = os.path.join(file_dir, f"range_{range_id}")
        
        if not os.path.isfile(range_path):
            log_error("STORAGE MANAGER", f"Rango {range_id} no encontrado para {filename}")
            raise FileNotFoundError(f"Rango {range_id} no encontrado para {filename}")
        
        with open(range_path, "rb") as f:
            return f.read()

    def delete_file_range(self, filename: str, range_id: str) -> bool:
        """
        Elimina un rango específico de chunks de un archivo.
        """
        from raft.log_utils import log_info, log_warning
        file_dir = os.path.join(self.storage_path, filename)
        range_path = os.path.join(file_dir, f"range_{range_id}")
        
        log_info("STORAGE MANAGER", f"Intentando eliminar shard {range_id} de {filename}...")
        if os.path.isfile(range_path):
            os.remove(range_path)
            
            # Si no quedan más rangos, eliminar el directorio
            if not os.listdir(file_dir):
                os.rmdir(file_dir)
            
            log_info("STORAGE MANAGER", f"Se eliminó con éxito el shard {range_id} de {filename}.")

            return True
        
        log_warning("STORAGE MANAGER", f"No se encontro el shard {range_id} de {filename}")
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
        try:
            end_chunk = start_chunk + chunk_count
            all_chunks = [None] * chunk_count
            
            # Obtener info del archivo del índice global
            file_metadata = global_index.get("files_metadata", {}).get(filename)
            if not file_metadata:
                raise FileNotFoundError(f"Archivo {filename} no encontrado en índice")
            
            chunk_distribution = file_metadata.get("chunk_distribution", {})
            
            if not chunk_distribution:
                raise Exception(f"No hay distribución de chunks para archivo {filename}")
            
            # 1. BUSCAR CHUNKS LOCALES
            from .utils import get_raft_server
            raft_server = get_raft_server()
            current_node_id = raft_server.host
            
            local_count = 0
            
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
                        local_count += 1
                
                except Exception as e:
                    log_warning("STORAGE DELEGATED", 
                            f"Error leyendo rango local {range_key}: {type(e).__name__}: {e}", 
                            color="yellow")
            
            log_info("STORAGE DELEGATED", 
                    f"[{current_node_id}] Chunks locales: {local_count}/{chunk_count}", 
                    color="light_green")
            
            # 2. IDENTIFICAR CHUNKS FALTANTES
            missing_chunks = [
                start_chunk + i 
                for i in range(chunk_count) 
                if all_chunks[i] is None
            ]
            
            if not missing_chunks:
                log_success("STORAGE DELEGATED", "Todos los chunks disponibles localmente", color="light_green")
                return all_chunks
            
            log_info("STORAGE DELEGATED", 
                    f"Chunks faltantes: {len(missing_chunks)}/{chunk_count}", 
                    color="yellow")
            
            # 3. AGRUPAR CHUNKS FALTANTES POR RANGO
            chunks_by_range = {}
            
            for chunk_idx in missing_chunks:
                for range_key, nodes_with_range in chunk_distribution.items():
                    range_start, range_end = map(int, range_key.split("-"))
                    
                    if range_start <= chunk_idx < range_end:
                        if range_key not in chunks_by_range:
                            remote_nodes = [n for n in nodes_with_range if n != current_node_id]
                            chunks_by_range[range_key] = {
                                "range_start": range_start,
                                "chunks": [],
                                "nodes": remote_nodes
                            }
                        chunks_by_range[range_key]["chunks"].append(chunk_idx)
                        break
            
            # 4. PEDIR CHUNKS REMOTOS
            import Pyro5.api as rpc
            import base64
            
            for range_key, info in chunks_by_range.items():
                chunks_needed = info["chunks"]
                remote_nodes = info["nodes"]
                range_start = info["range_start"]
                
                if not remote_nodes:
                    log_warning("STORAGE DELEGATED", 
                            f"Rango {range_key}: sin nodos remotos", 
                            color="yellow")
                    continue
                
                # Intentar con cada nodo hasta éxito
                for selected_node in remote_nodes:
                    try:
                        uri = f"PYRO:raft.storage.{selected_node}@{selected_node}:{raft_server.port}"
                        proxy = rpc.Proxy(uri)
                        proxy._pyroTimeout = 2.0
                        
                        range_data = proxy.get_chunk_range(filename, range_key)
                        
                        # 🔥 FIX: Manejar dict con base64 de Pyro5
                        if isinstance(range_data, dict):
                            if range_data.get('encoding') == 'base64' and 'data' in range_data:
                                range_data = base64.b64decode(range_data['data'])
                            else:
                                log_error("STORAGE DELEGATED", 
                                        f"Dict inválido de {selected_node}: keys={list(range_data.keys())}", 
                                        color="red")
                                continue
                        elif isinstance(range_data, bytearray):
                            range_data = bytes(range_data)
                        elif not isinstance(range_data, bytes):
                            log_error("STORAGE DELEGATED", 
                                    f"Tipo incorrecto de {selected_node}: {type(range_data).__name__}", 
                                    color="red")
                            continue
                        
                        if not range_data or len(range_data) == 0:
                            log_warning("STORAGE DELEGATED", 
                                    f"Rango {range_key} vacío desde {selected_node}", 
                                    color="yellow")
                            continue
                        
                        # Extraer chunks del rango
                        for chunk_idx in chunks_needed:
                            offset_in_range = chunk_idx - range_start
                            start_byte = offset_in_range * CHUNK_SIZE
                            end_byte = start_byte + CHUNK_SIZE
                            
                            if start_byte >= len(range_data):
                                log_warning("STORAGE DELEGATED", 
                                        f"Chunk {chunk_idx} fuera de límites en rango {range_key}", 
                                        color="yellow")
                                continue
                            
                            chunk_data = range_data[start_byte:end_byte]
                            relative_idx = chunk_idx - start_chunk
                            all_chunks[relative_idx] = chunk_data
                        
                        log_success("STORAGE DELEGATED", 
                                f"Rango {range_key} obtenido desde {selected_node}", 
                                color="light_green")
                        break
                        
                    except Exception as e:
                        log_warning("STORAGE DELEGATED", 
                                f"Fallo con {selected_node} para {range_key}: {type(e).__name__}: {str(e)[:100]}", 
                                color="yellow")
                        continue
            
            # 5. VERIFICACIÓN FINAL
            still_missing = sum(1 for c in all_chunks if c is None)
            
            if still_missing > 0:
                missing_indices = [start_chunk + i for i, c in enumerate(all_chunks) if c is None]
                log_error("STORAGE DELEGATED", 
                        f"Faltan {still_missing} chunks: {missing_indices[:10]}", 
                        color="red")
                raise Exception(f"No se pudieron obtener {still_missing}/{chunk_count} chunks")
            
            log_success("STORAGE DELEGATED", f"✓ {chunk_count} chunks obtenidos", color="light_green")
            return all_chunks
            
        except Exception as e:
            log_error("STORAGE DELEGATED", 
                    f"EXCEPCIÓN FATAL: {type(e).__name__}: {e}", 
                    color="red")
            raise
    
    # def read_chunks_delegated(
    #     self, filename: str, start_chunk: int, chunk_count: int, global_index: dict
    # ) -> list:
    #     """
    #     Método expuesto para que el líder delegue lectura completa.
    #     Este nodo busca chunks locales y pide faltantes a otros nodos.
    #     """
    #     end_chunk = start_chunk + chunk_count
    #     all_chunks = [None] * chunk_count
        
    #     # Obtener info del archivo del índice global
    #     file_metadata = global_index.get("files_metadata", {}).get(filename)
    #     if not file_metadata:
    #         raise FileNotFoundError(f"Archivo {filename} no encontrado en índice")
        
    #     chunk_distribution = file_metadata.get("chunk_distribution", {})
        
    #     # 1. BUSCAR CHUNKS LOCALES
    #     from .utils import get_raft_server
    #     raft_server = get_raft_server()
    #     current_node_id = raft_server.host
        
    #     for range_key, nodes_with_range in chunk_distribution.items():
    #         if current_node_id not in nodes_with_range:
    #             continue
            
    #         range_start, range_end = map(int, range_key.split("-"))
    #         intersection_start = max(start_chunk, range_start)
    #         intersection_end = min(end_chunk, range_end)
            
    #         if intersection_start >= intersection_end:
    #             continue
            
    #         try:
    #             range_data = self.get_chunk_range(filename, range_key)
                
    #             for chunk_idx in range(intersection_start, intersection_end):
    #                 offset_in_range = chunk_idx - range_start
    #                 start_byte = offset_in_range * CHUNK_SIZE
    #                 end_byte = start_byte + CHUNK_SIZE
                    
    #                 chunk_data = range_data[start_byte:end_byte]
    #                 relative_idx = chunk_idx - start_chunk
    #                 all_chunks[relative_idx] = chunk_data
            
    #         except Exception:
    #             pass
        
    #     # 2. IDENTIFICAR Y PEDIR CHUNKS FALTANTES
    #     missing_chunks = [
    #         start_chunk + i 
    #         for i in range(chunk_count) 
    #         if all_chunks[i] is None
    #     ]
        
    #     if missing_chunks:
    #         # Agrupar por rango y pedir a otros nodos
    #         chunks_by_range = {}
            
    #         for chunk_idx in missing_chunks:
    #             for range_key, nodes_with_range in chunk_distribution.items():
    #                 range_start, range_end = map(int, range_key.split("-"))
                    
    #                 if range_start <= chunk_idx < range_end:
    #                     if range_key not in chunks_by_range:
    #                         chunks_by_range[range_key] = {
    #                             "chunks": [],
    #                             "nodes": [n for n in nodes_with_range if n != current_node_id]
    #                         }
    #                     chunks_by_range[range_key]["chunks"].append(chunk_idx)
    #                     break
            
    #         # Pedir rangos a otros nodos
    #         import Pyro5.api as rpc
            
    #         for range_key, info in chunks_by_range.items():
    #             chunks_needed = info["chunks"]
    #             remote_nodes = info["nodes"]
                
    #             if not remote_nodes:
    #                 continue
                
    #             # Seleccionar primer nodo disponible (ya optimizado por líder)
    #             selected_node = remote_nodes[0]
                
    #             try:
    #                 uri = f"PYRO:raft.storage.{selected_node}@{selected_node}:{raft_server.port}"
    #                 proxy = rpc.Proxy(uri)
    #                 proxy._pyroTimeout = 5.0
                    
    #                 range_data = proxy.get_chunk_range(filename, range_key)
    #                 range_start, range_end = map(int, range_key.split("-"))
                    
    #                 for chunk_idx in chunks_needed:
    #                     offset_in_range = chunk_idx - range_start
    #                     start_byte = offset_in_range * CHUNK_SIZE
    #                     end_byte = start_byte + CHUNK_SIZE
                        
    #                     chunk_data = range_data[start_byte:end_byte]
    #                     relative_idx = chunk_idx - start_chunk
    #                     all_chunks[relative_idx] = chunk_data
                
    #             except Exception:
    #                 pass
        
    #     # Verificar integridad
    #     if None in all_chunks:
    #         raise Exception(f"No se pudieron obtener todos los chunks")
        
    #     return all_chunks
