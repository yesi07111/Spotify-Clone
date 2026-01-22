#leader_manager.py
import threading
import logging
import traceback
import time
from typing import List, Dict, Set
from math import ceil
import uuid
from Pyro5 import api as rpc
from raft.raft import RaftServer
from raft.storage_manager import StorageManager
from raft.log_utils import log_info, log_error, log_warning, log_success

from .utils import get_raft_server, get_raft_instance, next_term

class Colors:
    # Colores básicos
    BLACK = "\033[30m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"
    
    # Colores brillantes (high intensity)
    BRIGHT_BLACK = "\033[90m"
    BRIGHT_RED = "\033[91m"
    BRIGHT_GREEN = "\033[92m"
    BRIGHT_YELLOW = "\033[93m"
    BRIGHT_BLUE = "\033[94m"
    BRIGHT_MAGENTA = "\033[95m"
    BRIGHT_CYAN = "\033[96m"
    BRIGHT_WHITE = "\033[97m"
    
    # Colores de fondo
    BG_BLACK = "\033[40m"
    BG_RED = "\033[41m"
    BG_GREEN = "\033[42m"
    BG_YELLOW = "\033[43m"
    BG_BLUE = "\033[44m"
    BG_MAGENTA = "\033[45m"
    BG_CYAN = "\033[46m"
    BG_WHITE = "\033[47m"
    
    # Fondos brillantes
    BG_BRIGHT_BLACK = "\033[100m"
    BG_BRIGHT_RED = "\033[101m"
    BG_BRIGHT_GREEN = "\033[102m"
    BG_BRIGHT_YELLOW = "\033[103m"
    BG_BRIGHT_BLUE = "\033[104m"
    BG_BRIGHT_MAGENTA = "\033[105m"
    BG_BRIGHT_CYAN = "\033[106m"
    BG_BRIGHT_WHITE = "\033[107m"
    
    # Estilos de texto
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    ITALIC = "\033[3m"
    UNDERLINE = "\033[4m"
    BLINK = "\033[5m"
    REVERSE = "\033[7m"
    HIDDEN = "\033[8m"
    STRIKETHROUGH = "\033[9m"
    
    # Combinaciones útiles
    @staticmethod
    def success(text):
        return f"{Colors.BRIGHT_GREEN}{Colors.BOLD}{text}{Colors.RESET}"
    
    @staticmethod
    def error(text):
        return f"{Colors.BRIGHT_RED}{Colors.BOLD}{text}{Colors.RESET}"
    
    @staticmethod
    def warning(text):
        return f"{Colors.BRIGHT_YELLOW}{Colors.BOLD}{text}{Colors.RESET}"
    
    @staticmethod
    def info(text):
        return f"{Colors.BRIGHT_CYAN}{Colors.BOLD}{text}{Colors.RESET}"
    
    @staticmethod
    def header(text):
        return f"{Colors.BRIGHT_YELLOW}{Colors.BOLD}{Colors.UNDERLINE}{text}{Colors.RESET}"
    
    @staticmethod
    def debug(text):
        return f"{Colors.DIM}{Colors.BRIGHT_BLACK}{text}{Colors.RESET}"
    
    @staticmethod
    def highlight(text, bg_color=BG_BRIGHT_YELLOW, fg_color=BLACK):
        return f"{bg_color}{fg_color}{text}{Colors.RESET}"


# Logging básico
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("LeaderManager")

NODE_CHECK_INTERVAL = 2.5  # segundos

class LeaderManager:
    """
    LeaderManager:
    Es el coordinador central de TODAS las operaciones distribuidas.
    Solo se activa cuando el nodo es líder.

    Responsabilidades:
    - start() crea un hilo que periódicamente consulta nodos activos y files.
    - Coordinar lectura/escritura de metadata (con 2PC)
    - Coordinar lectura/escritura de archivos (distribución de carga)
    - Detecta nodos caídos y lanza replicaciones para mantener replication_factor.
    - stop() detiene el hilo de forma segura.
    
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        """Singleton: una sola instancia de LeaderManager"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(
        self,
        host: str,
        port: int,
        replication_factor: int = 3,
        pyro_object_id: str = "storage_manager",
        pyro_timeout: float = 5.0,
    ):
        if getattr(self, "_initialized", False):
            return

        self._initialized = True
        self._monitoring_cycle_count = 0  # Contador de ciclos
        self.host = host
        self.port = port

        self.replication_factor = replication_factor
        self.poll_interval = NODE_CHECK_INTERVAL
        self.pyro_object_id = pyro_object_id
        self.pyro_timeout = pyro_timeout

        self._nodes_lock = threading.Lock()
        self._cache_lock = threading.Lock()
        self._monitoring_count_lock = threading.Lock()
        self.logger = logging.getLogger("LeaderManager")

        # Inicializar estado

        with self._nodes_lock:
            self.nodes = {hash(ip): 0 for ip in self._get_client_nodes()}

        # Se obtienen al inicializar por problemas de referencias circulares
        # self.raft = get_raft_instance()
        # self.raft_server = get_raft_server()

        # Control interno del hilo
        self._thread = None
        self._stop_event = threading.Event()

        # Cache local de operaciones (para idempotencia)
        with self._cache_lock:
            self.local_cache = {}
        logger.info("LeaderManager inicializado")

    def set_raft(self, raft_server: RaftServer):
        self.raft_server = raft_server
        self.raft = raft_server.raft_instance

    def start(self):
        """Inicia el thread de monitoreo si no está activo"""
        if self._thread and self._thread.is_alive():
            logger.info("Thread de monitoreo ya está activo")
            return

        logger.info("Iniciando thread de monitoreo del LeaderManager")
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self._thread.start()

    def stop(self):
        """Detiene el thread de monitoreo"""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
            logger.info("Thread detenido")

    # ============================================================================
    # HILO DE MONITOREO
    # ============================================================================
    def _monitoring_loop(self):
        """
        Loop de monitorización:
         - consulta nodos activos,
         - detecta nodos caídos,
         - mantiene replication_factor.
        """
        logger.info("Monitoring loop iniciado.")

        self.remote_storage_manager = RemoteStorageManager()

        while not self._stop_event.is_set():
            try:
                current_term = next_term()
                current_nodes = self._get_client_nodes()
                with self._nodes_lock:
                    self.nodes = {hash(ip): 0 for ip in current_nodes}
                
                
                # Incrementar contador de ciclos
                with self._monitoring_count_lock:
                    logger.info(Colors.header(f"[LEADER ON {self.host} MONITORING LOOP {self._monitoring_cycle_count}]Sending heartbeat on term {current_term}"))
                    self._monitoring_cycle_count += 1
                
                self._check_leader_change_notification()

                self._update_index()

                self._detect_node_state_changes()

                self._process_node_states()

                self._manage_db_nodes()


            except Exception as e:
                logger.error(f"Error en _monitoring_loop: {e}")
                logger.error(traceback.format_exc())


            # Esperar antes de la siguiente iteración
            self._stop_event.wait(self.poll_interval)

        logger.info("Monitoring loop finalizado.")


    # ============================================================================
    # AUXILIARES PARA DESCUBRIMIENTO
    # ============================================================================
    def _get_client_nodes(self):
        """Obtiene la lista de nodos clientes activos (IPs)"""
        from .discovery import discover_active_clients
        client_ips = [ip for ip in discover_active_clients() if ip != self.host]
        clients = [(hash(ip), ip, self.port) for ip in client_ips]

        return clients

    def _filter_active_nodes(self, node_ips: List[str]) -> Set[str]:
        filtered = {self.host}
        for node_ip in node_ips:
            if node_ip == self.host:
                continue

            client = self._get_client_server(node_ip)

            if not client:
                continue

            try:
                response = client.get_state()
                if response.get('node_id') != node_ip:
                    continue
                filtered.add(node_ip)
            except Exception:
                continue
        
        return filtered
  

    # ============================================================================
    # ACTUALIZAR INDICE GLOBAL
    # ============================================================================
    def _update_index(self):
        """
        Reconstruye el índice global de archivos y metadatos.
        Se ejecuta solo en el líder.
        """
        if self.raft.state != "leader":
            return False

        from .discovery import discover_active_clients
        from raft.leader_manager import RemoteStorageManager
        import copy

        # Info - lilac para actualizar índice
        log_info("UPDATE INDEX", "Iniciando actualización del índice global",
                colorize_full=True, color="lilac")

        # Actualizar lista de archivos de cada nodo
        nodes = discover_active_clients()
        nodes = self._filter_active_nodes(nodes)

        log_info("UPDATE INDEX", f"Nodos activos detectados: {nodes}",
                colorize_full=True, color="lilac")

        if self.raft_server is None:
            # Warning - amarillo
            log_warning("UPDATE INDEX", "raft_server es None en update index",
                        colorize_full=True, color="yellow")

        processed_count = 0
        error_count = 0

        for node in nodes:
            try:
                if node == self.raft_server.node_id:
                    # Archivos del nodo local
                    log_info("UPDATE INDEX", f"Obteniendo archivos del nodo local {node}",
                            colorize_full=True, color="lilac")
                    files = self.raft_server.storage_instance.list_files()
                else:
                    # Archivos de nodo remoto
                    log_info("UPDATE INDEX", f"Obteniendo archivos del nodo remoto {node}",
                            colorize_full=True, color="lilac")
                    remote = RemoteStorageManager()
                    files = remote.list_files(node)

                with self.raft._lock:
                    # Asegurar las estructuras existentes
                    if "files" not in self.raft.global_index or not isinstance(self.raft.global_index["files"], dict):
                        self.raft.global_index["files"] = {}
                    self.raft.global_index["files"][node] = files

                    # Asegurar que el nodo tenga entrada en node_versions
                    if "node_versions" not in self.raft.global_index or not isinstance(self.raft.global_index["node_versions"], dict):
                        self.raft.global_index["node_versions"] = {}

                    if node not in self.raft.global_index["node_versions"]:
                        # Mantener la misma forma que el resto del sistema
                        self.raft.global_index["node_versions"][node] = {
                            "read_version": 0,
                            "write_version": 0,
                            "db_version": 0,
                            "db_version_prev": 0,
                            "is_db_node": node in self.raft.global_index.get("db_nodes", set())
                        }
                        log_info("UPDATE INDEX", f"Creada entrada en node_versions para nodo {node}",
                                colorize_full=True, color="lilac")

                processed_count += 1
                log_info("UPDATE INDEX", f"Nodo {node} procesado: {len(files)} archivos",
                        colorize_full=True, color="lilac")

            except Exception as e:
                # Error - rojo
                log_error("UPDATE INDEX", f"Error al obtener archivos del nodo {node}: {e}",
                        colorize_full=True, color="red")
                with self.raft._lock:
                    if "files" not in self.raft.global_index or not isinstance(self.raft.global_index["files"], dict):
                        self.raft.global_index["files"] = {}
                    self.raft.global_index["files"][node] = []
                error_count += 1

        # --------------------------
        # Limpiar canciones eliminadas del índice
        # --------------------------
        try:
            import json
            import copy
            
            # Leer el JSON del líder
            json_data = self.raft_server.db_instance.json_manager.read()

            # Buscar operaciones de delete de Track
            deleted_track_ids = []
            log_entries = json_data.get("log", [])  # Obtener el log primero

            for entry in log_entries:  # ✅ Iterar sobre las entradas del log
                operation_info = json.loads(entry.get("sql_operation", "{}"))
                if (operation_info.get("operation") == "delete" and 
                    operation_info.get("model") == "Track"):
                    # Extraer el ID de la canción de los parámetros
                    params = operation_info.get("params", {})
                    metadata = params.get("metadata_dict", {})
                    track_id = metadata.get("id")
                    if track_id:
                        deleted_track_ids.append(str(track_id))
            
            if deleted_track_ids:
                log_info("UPDATE INDEX", f"Canciones eliminadas detectadas: {deleted_track_ids}",
                        colorize_full=True, color="lilac")
                
                # Hacer copia del global_index para trabajar sin lock
                with self.raft._lock:
                    files_metadata_copy = copy.deepcopy(self.raft.global_index.get("files_metadata", {}))
                    files_copy = copy.deepcopy(self.raft.global_index.get("files", {}))
                
                # Buscar shards que corresponden a canciones eliminadas
                shards_to_delete = []
                for filename, file_info in files_metadata_copy.items():
                    # El filename contiene el ID de la canción
                    for track_id in deleted_track_ids:
                        if track_id in filename:
                            chunk_distribution = file_info.get("chunk_distribution", {})
                            # Verificar si algún nodo tiene estos shards
                            for range_key, nodes_with_shard in chunk_distribution.items():
                                for node_id in nodes_with_shard:
                                    # Verificar si el nodo realmente tiene el archivo
                                    if node_id in files_copy and filename in files_copy[node_id]:
                                        shards_to_delete.append({
                                            "filename": filename,
                                            "node_id": node_id,
                                            "range_key": range_key,
                                            "chunk_distribution": chunk_distribution
                                        })
                            break
                
                # Eliminar shards encontrados
                if shards_to_delete:
                    log_info("UPDATE INDEX", f"Eliminando {len(shards_to_delete)} shards de canciones borradas",
                            colorize_full=True, color="lilac")
                    
                    # Agrupar por filename para eliminar archivos completos
                    files_to_delete = {}
                    for shard in shards_to_delete:
                        filename = shard["filename"]
                        if filename not in files_to_delete:
                            files_to_delete[filename] = {
                                "chunk_distribution": shard["chunk_distribution"],
                                "tasks": []
                            }
                        files_to_delete[filename]["tasks"].append({
                            "node_id": shard["node_id"],
                            "filename": filename,
                            "range_key": shard["range_key"]
                        })
                    
                    # Eliminar físicamente y del índice
                    for filename, info in files_to_delete.items():
                        try:
                            # Ejecutar eliminaciones en paralelo
                            self._execute_delete_tasks_parallel(info["tasks"])
                            
                            # Eliminar del índice global
                            self._remove_file_from_index(filename, info["chunk_distribution"])
                            
                            log_success("UPDATE INDEX", f"Canción {filename} eliminada del sistema",
                                    colorize_full=True, color="lilac")
                        except Exception as e:
                            log_error("UPDATE INDEX", f"Error eliminando canción {filename}: {e}",
                                    colorize_full=True, color="red")
        
        except Exception as e:
            log_error("UPDATE INDEX", f"Error en limpieza de canciones eliminadas: {e}",
                    colorize_full=True, color="red")


        # --------------------------
        # Detectar y actualizar nodos revividos antes de verificar replicación
        # --------------------------
        try:
            
            remote_storage = RemoteStorageManager()
            
            # Obtener nodos activos
            active_nodes_set = set(nodes)
            
            # LOCK 1: Hacer deepcopy del índice global
            with self.raft._lock:
                global_index_copy = copy.deepcopy(self.raft.global_index)
            
            # Obtener información de nodos fuera del lock
            indexed_nodes = set(global_index_copy.get("node_shards", {}).keys())
            
            # Nodos vivos no en el índice
            alive_not_indexed = active_nodes_set - indexed_nodes
            # Nodos en el índice pero muertos
            indexed_but_dead = indexed_nodes - active_nodes_set

            marked_dead = []
            indexed_but_marked_dead_and_respawned = []

            for node in indexed_nodes:
                if node in self.raft_server.node_states and self.raft_server.node_states[node] == "DEAD":
                    marked_dead.append(node)
                    if node in active_nodes_set:
                        indexed_but_marked_dead_and_respawned.append(node)
            
            marked_dead = set(marked_dead) 
            indexed_but_marked_dead_and_respawned = set(indexed_but_marked_dead_and_respawned)

            log_info("UPDATE INDEX", 
                    f"Nodos vivos no indexados: {alive_not_indexed}, Nodos indexados pero muertos: {indexed_but_dead}, Nodos indexados marcados muertos: {marked_dead}, Nodos indexados marcados muertos que revivieron {indexed_but_marked_dead_and_respawned}",
                    colorize_full=True, color="lilac")
            
            # Preparar cambios fuera del lock
            updates_to_apply = []
            
            # Si hay candidatos a ser nodos revividos
            if (alive_not_indexed and indexed_but_dead):
                for alive_node in alive_not_indexed:
                    try:
                        # Obtener shards del nodo vivo
                        alive_files = remote_storage.list_files(alive_node)
                        if not alive_files:
                            continue
                        
                        alive_shards = {}
                        for filename in alive_files:
                            ranges = remote_storage.get_file_ranges(filename, alive_node)
                            if ranges:
                                alive_shards[filename] = set(ranges)
                        
                        if not alive_shards:
                            continue
                        
                        # Buscar nodo muerto que coincida
                        for dead_node in indexed_but_dead:
                            dead_node_info = global_index_copy.get("node_shards", {}).get(dead_node, {})
                            dead_shards = dead_node_info.get("shards", {})
                            
                            if not dead_shards:
                                continue
                            
                            # Verificar si el nodo vivo tiene TODOS los shards del nodo muerto
                            all_shards_match = True
                            for dead_filename, dead_ranges in dead_shards.items():
                                if dead_filename not in alive_shards:
                                    all_shards_match = False
                                    break
                                
                                dead_ranges_set = set(dead_ranges)
                                if not dead_ranges_set.issubset(alive_shards[dead_filename]):
                                    all_shards_match = False
                                    break
                            
                            if all_shards_match:
                                log_info("UPDATE INDEX", 
                                        f"Nodo {alive_node} identificado como {dead_node} (tiene todos sus shards)",
                                        colorize_full=True, color="lilac")
                                
                                # Preparar actualización (fuera del lock)
                                update_info = {
                                    "alive_node": alive_node,
                                    "dead_node": dead_node,
                                    "alive_files": alive_files,
                                    "alive_shards": alive_shards
                                }
                                
                                # Preparar node_versions: combinar versiones del muerto con archivos extras del vivo
                                if dead_node in global_index_copy.get("node_versions", {}):
                                    update_info["node_versions"] = global_index_copy["node_versions"][dead_node].copy()
                                
                                # Preparar files: combinar archivos del muerto con extras del vivo
                                dead_files = global_index_copy.get("files", {}).get(dead_node, [])
                                combined_files = list(set(dead_files + alive_files))
                                update_info["combined_files"] = combined_files
                                
                                # Preparar node_shards: combinar shards del muerto con extras del vivo
                                dead_shards_info = global_index_copy.get("node_shards", {}).get(dead_node, {})
                                combined_shards_info = {
                                    "total_chunks": dead_shards_info.get("total_chunks", 0),
                                    "shards": {}
                                }
                                
                                # Copiar shards del muerto
                                for filename, ranges in dead_shards.items():
                                    combined_shards_info["shards"][filename] = ranges.copy()
                                
                                # Agregar shards extras del vivo que no están en el muerto
                                for filename, ranges in alive_shards.items():
                                    if filename not in combined_shards_info["shards"]:
                                        combined_shards_info["shards"][filename] = list(ranges)
                                        # Calcular chunks adicionales
                                        for range_key in ranges:
                                            range_start, range_end = map(int, range_key.split("-"))
                                            combined_shards_info["total_chunks"] += (range_end - range_start)
                                    else:
                                        # Agregar ranges que no están en el muerto
                                        existing_ranges = set(combined_shards_info["shards"][filename])
                                        for range_key in ranges:
                                            if range_key not in existing_ranges:
                                                combined_shards_info["shards"][filename].append(range_key)
                                                range_start, range_end = map(int, range_key.split("-"))
                                                combined_shards_info["total_chunks"] += (range_end - range_start)
                                
                                update_info["combined_shards_info"] = combined_shards_info
                                
                                # Preparar chunk_distribution: actualizar todas las referencias
                                chunk_distribution_updates = []
                                for filename, metadata in global_index_copy.get("files_metadata", {}).items():
                                    distribution = metadata.get("chunk_distribution", {})
                                    for range_key, nodes_list in distribution.items():
                                        if dead_node in nodes_list:
                                            chunk_distribution_updates.append({
                                                "filename": filename,
                                                "range_key": range_key,
                                                "remove_node": dead_node,
                                                "add_node": alive_node if alive_node not in nodes_list else None
                                            })
                                
                                update_info["chunk_distribution_updates"] = chunk_distribution_updates
                                
                                # Verificar si era nodo DB
                                update_info["was_db_node"] = dead_node in global_index_copy.get("db_nodes", set())
                                
                                updates_to_apply.append(update_info)
                                
                                # Actualizar conjuntos para el resto del procesamiento
                                active_nodes_set.discard(dead_node)
                                active_nodes_set.add(alive_node)
                                indexed_but_dead.discard(dead_node)
                                
                                break  # Ya encontramos match para este nodo vivo
                        
                    except Exception as e:
                        log_error("UPDATE INDEX", 
                                f"Error procesando nodo vivo {alive_node}: {e}",
                                colorize_full=True, color="red")
            
            if indexed_but_marked_dead_and_respawned:
                for alive_node in indexed_but_marked_dead_and_respawned:
                    try:
                        # Obtener shards del nodo vivo
                        alive_files = remote_storage.list_files(alive_node)
                        if not alive_files:
                            continue
                        
                        alive_shards = {}
                        for filename in alive_files:
                            ranges = remote_storage.get_file_ranges(filename, alive_node)
                            if ranges:
                                alive_shards[filename] = set(ranges)
                        
                        if not alive_shards:
                            continue
                        
                        # Comprobar que el nodo revivido tenga al menos todo lo del muerto
                        for dead_node in marked_dead:
                            dead_node_info = global_index_copy.get("node_shards", {}).get(dead_node, {})
                            dead_shards = dead_node_info.get("shards", {})
                            
                            if not dead_shards:
                                continue
                            
                            # Verificar si el nodo vivo tiene TODOS los shards del nodo muerto
                            all_shards_match = True
                            for dead_filename, dead_ranges in dead_shards.items():
                                if dead_filename not in alive_shards:
                                    all_shards_match = False
                                    break
                                
                                dead_ranges_set = set(dead_ranges)
                                if not dead_ranges_set.issubset(alive_shards[dead_filename]):
                                    all_shards_match = False
                                    break
                            
                            if all_shards_match:
                                log_info("UPDATE INDEX", 
                                        f"Nodo {alive_node} identificado como el nodo {dead_node} en el indice (tiene todos sus shards)",
                                        colorize_full=True, color="lilac")
                                
                                # Preparar actualización (fuera del lock)
                                update_info = {
                                    "alive_node": alive_node,
                                    "dead_node": dead_node,
                                    "alive_files": alive_files,
                                    "alive_shards": alive_shards
                                }
                                
                                # Preparar node_versions: combinar versiones del muerto con archivos extras del vivo
                                if dead_node in global_index_copy.get("node_versions", {}):
                                    update_info["node_versions"] = global_index_copy["node_versions"][dead_node].copy()
                                
                                # Preparar files: combinar archivos del muerto con extras del vivo
                                dead_files = global_index_copy.get("files", {}).get(dead_node, [])
                                combined_files = list(set(dead_files + alive_files))
                                update_info["combined_files"] = combined_files
                                
                                # Preparar node_shards: combinar shards del muerto con extras del vivo
                                dead_shards_info = global_index_copy.get("node_shards", {}).get(dead_node, {})
                                combined_shards_info = {
                                    "total_chunks": dead_shards_info.get("total_chunks", 0),
                                    "shards": {}
                                }
                                
                                # Copiar shards del muerto
                                for filename, ranges in dead_shards.items():
                                    combined_shards_info["shards"][filename] = ranges.copy()
                                
                                # Agregar shards extras del vivo que no están en el muerto
                                for filename, ranges in alive_shards.items():
                                    if filename not in combined_shards_info["shards"]:
                                        combined_shards_info["shards"][filename] = list(ranges)
                                        # Calcular chunks adicionales
                                        for range_key in ranges:
                                            range_start, range_end = map(int, range_key.split("-"))
                                            combined_shards_info["total_chunks"] += (range_end - range_start)
                                    else:
                                        # Agregar ranges que no están en el muerto
                                        existing_ranges = set(combined_shards_info["shards"][filename])
                                        for range_key in ranges:
                                            if range_key not in existing_ranges:
                                                combined_shards_info["shards"][filename].append(range_key)
                                                range_start, range_end = map(int, range_key.split("-"))
                                                combined_shards_info["total_chunks"] += (range_end - range_start)
                                
                                update_info["combined_shards_info"] = combined_shards_info
                                
                                # Preparar chunk_distribution: actualizar todas las referencias
                                chunk_distribution_updates = []
                                for filename, metadata in global_index_copy.get("files_metadata", {}).items():
                                    distribution = metadata.get("chunk_distribution", {})
                                    for range_key, nodes_list in distribution.items():
                                        if dead_node in nodes_list:
                                            chunk_distribution_updates.append({
                                                "filename": filename,
                                                "range_key": range_key,
                                                "remove_node": dead_node,
                                                "add_node": alive_node if alive_node not in nodes_list else None
                                            })
                                
                                update_info["chunk_distribution_updates"] = chunk_distribution_updates
                                
                                # Verificar si era nodo DB
                                update_info["was_db_node"] = dead_node in global_index_copy.get("db_nodes", set())
                                
                                updates_to_apply.append(update_info)
                                
                                # Actualizar conjuntos para el resto del procesamiento
                                active_nodes_set.discard(dead_node) if dead_node in active_nodes_set else True
                                active_nodes_set.add(alive_node) if alive_node not in active_nodes_set else True
                                
                                
                                break  # Ya encontramos match para este nodo vivo
                        
                    except Exception as e:
                        log_error("UPDATE INDEX", 
                                f"Error procesando nodo revivido {alive_node}: {e}",
                                colorize_full=True, color="red")
            

            # LOCK 2: Aplicar todas las actualizaciones de una vez
            if updates_to_apply:
                for update_info in updates_to_apply:
                    alive_node = update_info["alive_node"]
                    dead_node = update_info["dead_node"]
                    
                    with self.raft._lock:
                        # Actualizar node_versions
                        if "node_versions" in update_info:
                            self.raft.global_index["node_versions"][alive_node] = update_info["node_versions"]
                            if dead_node in self.raft.global_index["node_versions"]:
                                del self.raft.global_index["node_versions"][dead_node]
                        
                        # Actualizar files
                        self.raft.global_index["files"][alive_node] = update_info["combined_files"]
                        if dead_node in self.raft.global_index["files"]:
                            del self.raft.global_index["files"][dead_node]
                        
                        # Actualizar node_shards
                        self.raft.global_index["node_shards"][alive_node] = update_info["combined_shards_info"]
                        if dead_node in self.raft.global_index["node_shards"]:
                            del self.raft.global_index["node_shards"][dead_node]
                        
                        # Actualizar chunk_distribution
                        for dist_update in update_info["chunk_distribution_updates"]:
                            filename = dist_update["filename"]
                            range_key = dist_update["range_key"]
                            
                            if filename in self.raft.global_index.get("files_metadata", {}):
                                distribution = self.raft.global_index["files_metadata"][filename].get("chunk_distribution", {})
                                if range_key in distribution:
                                    nodes_list = distribution[range_key]
                                    
                                    # Remover nodo muerto
                                    if dist_update["remove_node"] in nodes_list:
                                        nodes_list.remove(dist_update["remove_node"])
                                    
                                    # Agregar nodo vivo si no está
                                    if dist_update["add_node"] and dist_update["add_node"] not in nodes_list:
                                        nodes_list.append(dist_update["add_node"])
                        
                        # Actualizar db_nodes si era nodo DB
                        if update_info["was_db_node"]:
                            self.raft.global_index["db_nodes"].discard(dead_node)
                            self.raft.global_index["db_nodes"].add(alive_node)
                            if alive_node in self.raft.global_index["node_versions"]:
                                self.raft.global_index["node_versions"][alive_node]["is_db_node"] = True
                        
                        # Actualizar estados de nodos
                        if dead_node in self.raft_server.node_states:
                            del self.raft_server.node_states[dead_node]
                        self.raft_server.node_states[alive_node] = "RE-SPAWN"
                        
                    log_success("UPDATE INDEX", 
                                f"IP actualizada en índice global: {dead_node} -> {alive_node}",
                                colorize_full=True, color="lilac")

        except Exception as e:
            log_error("UPDATE INDEX", 
                    f"Error en detección de nodos revividos: {e}",
                    colorize_full=True, color="red")

        # --------------------------
        # Verificar replicación, usando nodos activos actualizados
        # --------------------------
        try:
            with self.raft._lock:
                files_metadata = self.raft.global_index.get("files_metadata", {})
                node_shards_info = self.raft.global_index.get("node_shards", {})

            import json
            files_metadata_str = json.dumps(files_metadata, indent=6)
            node_shards_info_str = json.dumps(node_shards_info, indent=6)

            log_info("UPDATE INDEX", f"Global index files metadata: {files_metadata_str} \nGlobal index node shards info: {node_shards_info_str}", colorize_full=True, color="lilac")
            # Usar el conjunto actualizado de nodos activos
            active_nodes = active_nodes_set

            # Si no hay metadata, no hay shards que comprobar
            if files_metadata:
                missing_found = False
                excess_found = False

                # comprobar cada shard en cada archivo
                for filename, file_info in files_metadata.items():
                    distribution = file_info.get("chunk_distribution", {}) or {}
                    for range_key, nodes_with_shard in distribution.items():
                        if not isinstance(nodes_with_shard, (list, tuple, set)):
                            # si es None o formato inesperado, marcar como missing para reintentar
                            missing_found = True
                            break

                        # FILTRAR SOLO NODOS ACTIVOS para contar réplicas
                        active_nodes_with_shard = [n for n in nodes_with_shard if n in active_nodes]
                        current_count = len(active_nodes_with_shard)
                        
                        # k objetivo: usar self.replication_factor
                        k = getattr(self, "replication_factor", None)
                        if k is None:
                            # si no existe self.replication_factor, intentar obtener de raft (fallback)
                            k = getattr(self.raft, "db_replication_factor", None)

                        # si no hay k definido, no intentamos replicar/limpiar
                        if k is None:
                            continue

                        # COMPARAR solo con nodos ACTIVOS
                        if current_count < k:
                            missing_found = True
                            dead_nodes = [n for n in nodes_with_shard if n not in active_nodes]
                            log_info("UPDATE INDEX", 
                                    f"Shard {filename} rango {range_key}: {current_count}/{k} réplicas activas. "
                                    f"Total en índice: {len(nodes_with_shard)} (nodos muertos que no lo tienen: {dead_nodes})",
                                    colorize_full=True, color="yellow") 
                        elif current_count > k:
                            excess_found = True

                    if missing_found and excess_found:
                        break

                # Si hay problemas de replicación, ejecutar acciones pertinentes.
                # Aseguramos que las funciones internas usen el mismo k que self.replication_factor:
                if missing_found or excess_found:
                    try:
                        if missing_found:
                            log_info("UPDATE INDEX", "Se detectaron shards con réplicas faltantes; iniciando restauración.",
                                    colorize_full=True, color="lilac")
                            # La función maneja reducción de k si hay menos nodos vivos
                            self._restore_replication_factor()

                        if excess_found:
                            log_info("UPDATE INDEX", "Se detectaron shards con réplicas excedentes; iniciando limpieza.",
                                    colorize_full=True, color="lilac")
                            self._cleanup_excess_replicas()

                    except Exception as e:
                        log_error("UPDATE INDEX", f"Error durante restauración/limpieza de replicación: {e}",
                                colorize_full=True, color="red")

        except Exception as e:
            log_error("UPDATE INDEX", f"Error verificando factor de replicación: {e}",
                    colorize_full=True, color="red")
        # Incrementar versión del índice
        with self.raft._lock:
            old_version = self.raft.global_index.get("version", 0)
            self.raft.global_index["version"] = old_version + 1
            new_version = self.raft.global_index["version"]
        
        # Success - lilac
        log_success("UPDATE INDEX",
                    f"Índice global actualizado exitosamente. Versión: {old_version} -> {new_version}",
                    colorize_full=True, color="lilac")
        log_info("UPDATE INDEX",
                f"Resumen: {processed_count} nodos procesados, {error_count} errores",
                colorize_full=True, color="lilac")

        return True

    # ============================================================================
    # DETECTAR ESTADO DE NODOS
    # ============================================================================
    def _detect_node_state_changes(self):
        """Detecta cambios en el estado de los nodos"""
        from raft.discovery import discover_active_clients
        
        if self.raft.state != "leader":
            return
        
        # Info - morado para detección de estados
        log_info("DETECT STATES", "Iniciando detección de cambios en estados de nodos", 
                colorize_full=True, color="purple")
        
        current_active = set(discover_active_clients())
        current_active = self._filter_active_nodes(current_active)
        log_info("DETECT STATES", f"Nodos activos actuales: {', '.join(current_active)}", 
                colorize_full=True, color="purple")
        
        # Leer si es primera vez (lock corto)
        with self.raft_server._lock:
            is_first_time = not hasattr(self.raft_server, 'previous_active_nodes') or len(self.raft_server.previous_active_nodes) == 0
        
        if is_first_time:
            # Primera vez - todos los nodos activos son ALIVE
            log_info("DETECT STATES", "Primera detección: marcando todos los nodos activos como ALIVE", 
                    colorize_full=True, color="purple")
            
            with self.raft_server._lock:
                self.raft_server.node_states[self.host] = "ALIVE"
                for node_ip in current_active:
                    self.raft_server.node_states[node_ip] = "ALIVE"
                self.raft_server.previous_active_nodes = set(self.raft_server.node_states.keys())
            
            log_success("DETECT STATES", f"Detección inicial completada: {len(self.raft_server.node_states)} nodos marcados como ALIVE", 
                        colorize_full=True, color="purple")
            return
        
        # Leer estados actuales y previous_active_nodes (lock corto)
        with self.raft_server._lock:
            previous_active_copy = self.raft_server.previous_active_nodes.copy()
        
        log_info("DETECT STATES", f"Nodos conocidos anteriormente: {', '.join(previous_active_copy)}", 
                colorize_full=True, color="purple")
        
        # Detectar nodos que revivieron o son nuevos
        new_nodes = current_active - previous_active_copy
        
        if new_nodes:
            log_info("DETECT STATES", f"Nodos nuevos o revividos detectados: {', '.join(new_nodes)}", 
                    colorize_full=True, color="purple")
        else:
            log_info("DETECT STATES", "No hay nodos nuevos o revividos", 
                    colorize_full=True, color="purple")
        
        for node_ip in new_nodes:
            # Determinar si es RE-SPAWN o NEW (SIN LOCK - llamadas remotas)
            is_respawn = False
            
            try:
                # Check 1: Tiene JSON de BD?
                # from raft.leader_manager import RemoteDBManager
                remote_db = RemoteDBManager()
                try:
                    json_dump = remote_db.get_json_dump(node_ip)
                    if json_dump:
                        is_respawn = True
                        log_info("DETECT STATES", f"Nodo {node_ip} tiene Json de DB, es RE-SPAWN", 
                                colorize_full=True, color="purple")
                except Exception:
                    pass
                
                # Check 2: Tiene shards?
                if not is_respawn:
                    from raft.leader_manager import RemoteStorageManager
                    remote_storage = RemoteStorageManager()
                    files = remote_storage.list_files(node_ip)
                    if files:
                        is_respawn = True
                        log_info("DETECT STATES", f"Nodo {node_ip} tiene shards, es RE-SPAWN", 
                                colorize_full=True, color="purple")
            
            except Exception:
                pass
            
            # Actualizar estado (lock corto)
            new_state = "RE-SPAWN" if is_respawn else "NEW"
            with self.raft_server._lock:
                self.raft_server.node_states[node_ip] = new_state
            
            log_info("DETECT STATES", f"Nodo {node_ip} marcado como {new_state}", 
                    colorize_full=True, color="purple")
        
        # Detectar nodos que murieron
        dead_nodes = previous_active_copy - current_active
        
        dead_count = 0
        for node_ip in dead_nodes:
            # Leer y escribir estado (lock corto)
            with self.raft_server._lock:
                if node_ip in self.raft_server.node_states and self.raft_server.node_states[node_ip] == "ALIVE":
                    self.raft_server.node_states[node_ip] = "DEAD" 
                    should_log = True
                else:
                    should_log = False
            with self.raft._lock:
                if node_ip in self.raft.global_index["db_nodes"]:
                    self.raft.global_index["db_nodes"].discard(node_ip)
                    log_info("DETECT STATES", f"Nodo DB {node_ip} removido de db_nodes", 
                            colorize_full=True, color="purple")
            if should_log:
                dead_count += 1
                # Warning - amarillo para nodos muertos
                log_warning("DETECT STATES", f"Nodo {node_ip} detectado como DEAD", 
                        colorize_full=True, color="yellow")
        
        if dead_count > 0:
            log_info("DETECT STATES", f"Total de nodos muertos detectados: {dead_count}", 
                    colorize_full=True, color="purple")
        
        # Actualizar nodos que siguen activos
        alive_count = 0
        for node_ip in current_active:
            with self.raft_server._lock:
                if node_ip in self.raft_server.node_states and self.raft_server.node_states[node_ip] not in ["RE-SPAWN", "NEW"]:
                    self.raft_server.node_states[node_ip] = "ALIVE"
                    alive_count += 1
        
        # Actualizar previous_active_nodes (lock corto)
        with self.raft_server._lock:
            self.raft_server.previous_active_nodes = set(self.raft_server.node_states.keys())
        
        log_success("DETECT STATES", 
                    f"Detección completada: {alive_count} nodos ALIVE, {len(new_nodes)} nuevos/RE-SPAWN, {dead_count} DEAD", 
                    colorize_full=True, color="purple")
        
        # Código comentado original (con logs actualizados):
        # logger.info(Colors.info("Nodos actuales y sus estados:"))
        # for node, state in node_states_copy.items():
        #     if state != "DEAD":
        #         logger.info(Colors.info(f"\t{node}: {state}"))
        #     else:
        #         logger.info(Colors.warning(f"\t{node}: {state}"))

    # ============================================================================
    # PROCESAMIENTO DE NODOS NUEVOS Y REVIVIDOS
    # ============================================================================
    
    def _process_node_states(self):
        """Procesa nodos según su estado"""
        if self.raft.state != "leader":
            return
        
        with self.raft_server._lock:
            states_copy = self.raft_server.node_states.copy()
        
        for node_ip, state in states_copy.items():
            if state == "RE-SPAWN":
                threading.Thread(
                    target=self._process_respawn_node,
                    args=(node_ip,),
                    daemon=True
                ).start()
                # Info - morado oscuro para RE-SPAWN
                log_info("PROCESS NODE", f"Iniciando procesamiento de nodo RE-SPAWN: {node_ip}", 
                        colorize_full=True)
            
            elif state == "NEW":
                threading.Thread(
                    target=self._process_new_node,
                    args=(node_ip,),
                    daemon=True
                ).start()
                # Info - morado claro para NEW
                log_info("PROCESS NODE", f"Iniciando procesamiento de nodo NEW: {node_ip}", 
                        colorize_full=True, color="light_purple")

    def _process_new_node(self, node_ip: str):
        """Procesa un nodo completamente nuevo"""
        # Info - morado claro para NEW
        log_info("NEW NODE", f"Procesando nodo NEW: {node_ip}", 
                colorize_full=True, color="light_purple")
        
        try:
            # PRIMERO: Restaurar factor de replicación antes de balancear
            log_info("NEW NODE", f"Restaurando factor de replicación para nodo {node_ip}", 
                    colorize_full=True, color="light_purple")
            self._restore_replication_factor()
            
            # Obtener nodos vivos
            from .discovery import discover_active_clients
            nodes = discover_active_clients()
            alive_nodes = self._filter_active_nodes(nodes)
            
            log_info("NEW NODE", f"Nodos vivos detectados: {alive_nodes}", 
                    colorize_full=True, color="light_purple")
            
            # SEGUNDO: Hacer balanceo solo si hay más de k nodos
            if len(alive_nodes) > self.raft.db_replication_factor:
                log_info("NEW NODE", f"Iniciando balanceo de shards para nodo {node_ip}", 
                        colorize_full=True, color="light_purple")
                self._balance_shards(node_ip)
                log_success("NEW NODE", f"Balanceo completado para nodo {node_ip}", 
                            colorize_full=True, color="light_purple")
            else:
                log_info("NEW NODE", 
                        f"No se realiza balanceo: solo hay {len(alive_nodes)} nodos vivos (se requieren más de {self.raft.db_replication_factor})", 
                        colorize_full=True, color="light_purple")
            
            # Marcar como ALIVE
            with self.raft_server._lock:
                self.raft_server.node_states[node_ip] = "ALIVE"
            
            log_success("NEW NODE", f"Nodo NEW {node_ip} procesado y marcado como ALIVE", 
                        colorize_full=True, color="light_purple")
        
        except Exception as e:
            # Error - rojo
            log_error("NEW NODE", f"Error procesando NEW {node_ip}: {e}", 
                    colorize_full=True, color="red")

    def _process_respawn_node(self, node_ip: str):
        """Procesa un nodo que revivió - SIEMPRE se degrada pero primero se mezclan operaciones"""
        from raft.db_json_manager import DBJsonManager
        from raft.leader_manager import RemoteDBManager

        log_info("RESPAWN NODE", f"Procesando nodo RE-SPAWN: {node_ip}", 
                colorize_full=True)
        
        try:
            remote_db = RemoteDBManager()
            json_manager = DBJsonManager()
            
            # 1. Verificar si el nodo es DB (antes de obtener JSONs)
            is_db_node = False
            with self.raft._lock:
                is_db_node = (node_ip in self.raft.global_index.get("db_nodes", set()) or
                            self.raft.global_index.get("node_versions", {}).get(node_ip, {}).get("is_db_node", False))
            
            log_info("RESPAWN NODE", f"Nodo {node_ip} era DB: {is_db_node}", 
                    colorize_full=True)
            
            # 2. Obtener JSON del líder SIEMPRE
            try:
                leader_json = json_manager.read()
            except Exception as e:
                log_error("RESPAWN NODE", f"Error obteniendo JSON del líder: {e}", 
                        colorize_full=True, color="red")
                leader_json = None
            
            # 3. Obtener JSON del nodo SOLO si es DB (solo nodos DB tienen JSON)
            remote_json = None
            try:
                remote_json = remote_db.get_json_dump(node_ip)
            except Exception as e:
                log_warning("RESPAWN NODE", f"Error obteniendo JSON del nodo DB {node_ip}: {e}", 
                        colorize_full=True, color="yellow")
            
            if remote_json is not None:
                if not is_db_node:
                    is_db_node = True
                    log_info("RESPAWN NODE", f"Nodo {node_ip} detectado que era DB antes aunque no esta en node_versions", 
                    colorize_full=True)

        
            # 4. Si el nodo es DB y tiene JSON, mezclar operaciones
            if is_db_node and remote_json and leader_json:
                log_info("RESPAWN NODE", f"Mezclando operaciones de {node_ip} con líder", 
                        colorize_full=True)
                
                remote_ops = remote_json.get("log", [])
                leader_ops = leader_json.get("log", [])
                
                # Identificar operaciones que tiene el nodo pero no el líder
                leader_task_ids = {op["task_id"] for op in leader_ops}
                missing_ops_in_leader = [
                    op for op in remote_ops 
                    if op["task_id"] not in leader_task_ids
                ]
                
                if missing_ops_in_leader:
                    log_info("RESPAWN NODE", f"Ejecutando {len(missing_ops_in_leader)} operaciones faltantes del nodo revivido en líder", 
                            colorize_full=True)
                    
                    # Ejecutar en líder
                    for op in missing_ops_in_leader:
                        try:
                            result = self.raft_server.db_instance.execute_single_operation(op)
                            if result.get("success"):
                                log_info("RESPAWN NODE", f"Operación {op['task_id']} ejecutada en líder", 
                                        colorize_full=True)
                        except Exception as e:
                            log_error("RESPAWN NODE", f"Error ejecutando operación {op['task_id']} en líder: {e}", 
                                    colorize_full=True, color="red")
                    
                    # Actualizar leader_json después de ejecutar operaciones
                    leader_json = json_manager.read()

                    # Replicar en k-1 nodos DB
                    with self.raft._lock:
                        db_nodes = [n for n in self.raft.global_index.get("db_nodes", set()) 
                                if n != self.raft.node_id and n != node_ip]
                    
                    db_nodes = self._filter_active_nodes(db_nodes)
                    
                    if len(db_nodes) != self.raft.db_replication_factor and node_ip not in db_nodes:
                        log_info("RESPAWN NODE", f"No hay suficientes nodos DB, usando el nodo RESPAWN")
                        db_nodes.add(node_ip)
                        leader_db_version = leader_json.get("db_version", 0)
                        leader_term = leader_json.get("term", 0)
                        
                        remote_db.update_db_version(node_ip, leader_db_version)
                        remote_db.update_term(node_ip, leader_term)
                        
                        with self.raft._lock:
                            self.raft.global_index["db_nodes"].add(node_ip)
                            self.raft.global_index["node_versions"][node_ip]["is_db_node"] = True
                            
                            self.raft.global_index["node_versions"][node_ip]["db_version"] = leader_db_version
                            self.raft.global_index["node_versions"][node_ip]["db_version_prev"] = (leader_db_version - 1) if leader_db_version > 0 else 0


                        log_info("RESPAWN NODE", f"Nodo {node_ip} marcado como DB con éxito")

                    elif is_db_node:
                        log_info("RESPAWN NODE", f"Hay suficientes nodos DB y nodo RESPAWN {node_ip} era DB antes, degradando...")
                        self._demote_db_node(node_ip)

                    # Obtener operaciones del líder que no están en otros nodos
                    leader_ops = leader_json.get("log", [])
                    
                    # Replicando operaciones del lider a otros nodos DB
                    for db_node in db_nodes:
                        try:
                            other_node_json = remote_db.get_json_dump(db_node)
                            if other_node_json:
                                other_ops = other_node_json.get("log", [])
                                other_task_ids = {op["task_id"] for op in other_ops}
                                
                                missing_in_other = [
                                    op for op in leader_ops 
                                    if op["task_id"] not in other_task_ids
                                ]
                                
                                success_c = 0
                                for op in missing_in_other:
                                    try:
                                        result = remote_db.execute_single_operation(op, db_node)
                                        if result.get("success"):
                                            log_info("RESPAWN NODE", f"Operación {op['task_id']} replicada en {db_node}", 
                                                    colorize_full=True)   
                                            success_c += 1
                                            
                                    except Exception as e:
                                        log_error("RESPAWN NODE", f"Error replicando operación en {db_node}: {e}", 
                                                colorize_full=True, color="red")
                                    
                                if success_c == len(missing_in_other):
                                    # Actualizar db_version y term en el nodo remoto
                                    leader_db_version = leader_json.get("db_version", 0)
                                    leader_term = leader_json.get("term", 0)
                                    
                                    remote_db.update_db_version(db_node, leader_db_version)
                                    remote_db.update_term(db_node, leader_term)
                                    
                                    # Actualizar en global_index
                                    with self.raft._lock:
                                        if node_ip in self.raft.global_index["node_versions"]:
                                            self.raft.global_index["node_versions"][node_ip]["db_version"] = leader_db_version
                                            self.raft.global_index["node_versions"][node_ip]["db_version_prev"] = leader_json.get("db_version_prev", 0)
                                            
                        except Exception as e:
                            log_warning("RESPAWN NODE", f"No se pudo sincronizar con {db_node}: {e}", 
                                    colorize_full=True, color="yellow")
        
            # 5. Asegurar réplicas de shards del nodo revivido
            log_info("RESPAWN NODE", f"Asegurando réplicas de shards para {node_ip}", 
                    colorize_full=True)
            self._ensure_replicas_for_respawn_node(node_ip, leader_json)
            
            # 6. Balanceo - llamar a balance genérico que encuentra nodo con menos shards
            log_info("RESPAWN NODE", f"Iniciando balanceo general tras revivir {node_ip}", 
                    colorize_full=True)
            self._balance_shards_general()
            
            # 7. Marcar como ALIVE
            with self.raft_server._lock:
                self.raft_server.node_states[node_ip] = "ALIVE"
            
            log_success("RESPAWN NODE", f"Nodo {node_ip} procesado y marcado como ALIVE", 
                        colorize_full=True)
        
        except Exception as e:
            log_error("RESPAWN NODE", f"Error procesando RE-SPAWN {node_ip}: {e}", 
                    colorize_full=True, color="red")
            import traceback
            traceback.print_exc()


    # ============================================================================
    # HELPERS PARA PROCESAMIENTO DE NODOS REVIVIDOS
    # ============================================================================
    def _sync_db_node(self, node_ip: str, remote_json: dict, leader_json: dict, sync_leader: bool = False):
        """Sincroniza un nodo DB desactualizado enviando solo operaciones faltantes"""
        from raft.leader_manager import RemoteDBManager
        
        # Info - morado oscuro para sincronización DB
        log_info("SYNC DB NODE", f"Sincronizando nodo DB {node_ip} incrementalmente", 
                colorize_full=True)
        
        try:
            remote_db = RemoteDBManager()
            
            # 1. Obtener operaciones del nodo remoto
            remote_ops = remote_json.get("log", [])
            leader_ops = leader_json.get("log", [])
            
            # 2. Identificar operaciones pending en el nodo remoto y en el lider
            remote_pending = [op for op in remote_ops if op.get("status") == "pending"]
            leader_pending = [op for op in leader_ops if op.get("status") == "pending"]
            
            # 3.1 Ejecutar las pending del nodo remoto EN ORDEN
            if remote_pending:
                # Ordenar por el orden en que aparecen en el log
                remote_pending.sort(key=lambda x: remote_ops.index(x))
                
                log_info("SYNC DB NODE", f"Ejecutando {len(remote_pending)} operaciones pending en nodo: {node_ip}", 
                        colorize_full=True)
                
                for pending_op in remote_pending:
                    try:
                        # Mandar a ejecutar en el nodo remoto
                        result = remote_db.execute_single_operation(pending_op, node_ip)
                        
                        if result.get("success"):
                            log_info("SYNC DB NODE", f"Operación {pending_op['task_id']} ejecutada en nodo: {node_ip}", 
                                    colorize_full=True)
                        else:
                            log_warning("SYNC DB NODE", f"Advertencia ejecutando {pending_op['task_id']}: {result}", 
                                    colorize_full=True, color="yellow")
                    
                    except Exception as e:
                        log_error("SYNC DB NODE", f"Error ejecutando pending en nodo: {node_ip}: {e}", 
                                colorize_full=True, color="red")
            
            #3.2 Ejecutar las pending del nodo lider EN ORDEN
            if leader_pending:
                # Ordenar por el orden en que aparecen en el log
                leader_pending.sort(key=lambda x: leader_ops.index(x))
                
                log_info("SYNC DB NODE", f"Ejecutando {len(leader_pending)} operaciones pending en lider: {self.raft.node_id}", 
                        colorize_full=True)
                
                for pending_op in leader_pending:
                    try:
                        # Mandar a ejecutar en el nodo lider
                        result = self.raft_server.db_instance.execute_single_operation(pending_op)
                        
                        if result.get("success"):
                            log_info("SYNC DB NODE", f"Operación {pending_op['task_id']} ejecutada en lider: {self.raft.node_id}", 
                                    colorize_full=True)
                        else:
                            log_warning("SYNC DB NODE", f"Advertencia ejecutando {pending_op['task_id']}: {result}", 
                                    colorize_full=True, color="yellow")
                    
                    except Exception as e:
                        log_error("SYNC DB NODE", f"Error ejecutando pending en lider: {self.raft.node_id}: {e}", 
                                colorize_full=True, color="red")

            # 4.1 Identificar operaciones que tiene el líder pero no el nodo remoto
            remote_task_ids = {op["task_id"] for op in remote_ops}
            
            missing_ops_node = [
                op for op in leader_ops 
                if op["task_id"] not in remote_task_ids
            ]

            # 4.2 Identificar operaciones que tiene el nodo remoto pero no el líder 
            leader_task_ids = {op["task_id"] for op in leader_ops}
            
            missing_ops_leader = [
                op for op in remote_ops 
                if op["task_id"] not in leader_task_ids
            ]

            # 5.1 Enviar operaciones faltantes EN ORDEN al nodo
            success_count = 0
            if missing_ops_node:
                # Ordenar por el orden en el log del líder
                missing_ops_node.sort(key=lambda x: leader_ops.index(x))
                
                log_info("SYNC DB NODE", f"Enviando {len(missing_ops_node)} operaciones faltantes a nodo: {node_ip}", 
                        colorize_full=True)
                
                for missing_op in missing_ops_node:
                    try:
                        result = remote_db.execute_single_operation(missing_op, node_ip)
                        
                        if result.get("success"):
                            success_count += 1
                            log_info("SYNC DB NODE", f"Operación {missing_op['task_id']} enviada a nodo: {node_ip}", 
                                    colorize_full=True)
                        else:
                            log_warning("SYNC DB NODE", f"Advertencia enviando {missing_op['task_id']}: {result}", 
                                    colorize_full=True, color="yellow")
                    
                    except Exception as e:
                        log_error("SYNC DB NODE", f"Error enviando operación a nodo: {node_ip}: {e}", 
                                colorize_full=True, color="red")
                
            if not missing_ops_node or success_count == len(missing_ops_node):
                remote_db.update_term(node_ip, leader_json["term"])
                remote_db.update_db_version(node_ip, leader_json["db_version"])
                log_info("SYNC DB NODE", "Se completaron todas las operaciones de sync en el nodo remoto", 
                        colorize_full=True)

            if missing_ops_leader:
                # Ordenar por el orden en el log del nodo
                missing_ops_leader.sort(key=lambda x: remote_ops.index(x))
                
                log_info("SYNC DB NODE", f"Enviando {len(missing_ops_leader)} operaciones faltantes a lider: {self.raft.node_id}", 
                        colorize_full=True)
                
                for missing_op in missing_ops_leader:
                    try:
                        result = self.raft_server.db_instance.execute_single_operation(missing_op)
                        
                        if result.get("success"):
                            log_info("SYNC DB NODE", f"Operación {missing_op['task_id']} enviada a lider: {self.raft.node_id}", 
                                    colorize_full=True)
                        else:
                            log_warning("SYNC DB NODE", f"Advertencia enviando {missing_op['task_id']}: {result}", 
                                    colorize_full=True, color="yellow")
                    
                    except Exception as e:
                        log_error("SYNC DB NODE", f"Error enviando operación a lider: {self.raft.node_id}: {e}", 
                                colorize_full=True, color="red")
            
            with self.raft._lock:
                if node_ip not in self.raft.global_index["db_nodes"]:
                    self.raft.global_index["db_nodes"].add(node_ip)
            log_success("SYNC DB NODE", f"Nodo DB {node_ip} y lider {self.raft.node_id} sincronizados exitosamente", 
                        colorize_full=True)
        
        except Exception as e:
            log_error("SYNC DB NODE", f"Error sincronizando nodo DB {node_ip}: {e}", 
                    colorize_full=True, color="red")
            import traceback
            traceback.print_exc()

    def _demote_db_node(self, node_ip: str):
        """Degrada un nodo de DB a nodo normal"""
        remote_db = RemoteDBManager()
        try:
            with self.raft._lock:
                # Eliminar de db_nodes
                self.raft.global_index["db_nodes"].discard(node_ip)
                
                if node_ip in self.raft.global_index["node_versions"]:
                    self.raft.global_index["node_versions"][node_ip]["is_db_node"] = False
                    self.raft.global_index["node_versions"][node_ip]["db_version"] = 0
                    self.raft.global_index["node_versions"][node_ip]["db_version_prev"] = 0
            
            # Borrar JSON en el nodo remoto
            try:
                client = remote_db._get_client_server(node_ip)
                result = client.delete_json()
                if result.get("success"):
                    log_info("RESPAWN NODE", f"JSON eliminado en nodo {node_ip}",
                            colorize_full=True)
                else:
                    log_warning("RESPAWN NODE", f"No se pudo eliminar JSON en {node_ip}: {result}",
                            colorize_full=True, color="yellow")
            except Exception as e:
                log_error("RESPAWN NODE", f"Error eliminando JSON en {node_ip}: {e}",
                        colorize_full=True, color="red")
                
            log_info("DEMOTE DB NODE", f"Nodo {node_ip} degradado de nodo DB", 
                    colorize_full=True)
        
        except Exception as e:
            log_error("DEMOTE DB NODE", f"Error degradando nodo {node_ip}: {e}", 
                    colorize_full=True, color="red")

    def _update_respawn_node_ip_improved(self, node_ip: str, remote_json: dict, leader_json: dict):
        """Actualiza IP basándose en shards, excluyendo los eliminados por delete"""
        from raft.leader_manager import RemoteStorageManager
        
        try:
            log_info("UPDATE IP", f"Actualizando IP para nodo RE-SPAWN: {node_ip}", 
                    colorize_full=True)
            
            remote_storage = RemoteStorageManager()
            
            # Obtener shards del nodo revivido
            try:
                files = remote_storage.list_files(node_ip)
                if not files:
                    log_info("UPDATE IP", f"Nodo {node_ip} no tiene archivos", 
                            colorize_full=True)
                    return
                
                # Construir mapa de shards actuales
                current_shards = {}
                for filename in files:
                    ranges = remote_storage.get_file_ranges(filename, node_ip)
                    if ranges:
                        current_shards[filename] = set(ranges)
                
                if not current_shards:
                    log_info("UPDATE IP", f"Nodo {node_ip} no tiene shards", 
                            colorize_full=True)
                    return
                
                # Obtener IDs de tracks eliminados en el JSON del nodo revivido
                deleted_in_remote = set()
                if remote_json:
                    for op in remote_json.get("log", []):
                        import json
                        operation_info = json.loads(op.get("sql_operation", "{}"))
                        if (operation_info.get("operation") == "delete" and 
                            operation_info.get("model") == "Track"):
                            params = operation_info.get("params", {})
                            metadata = params.get("metadata_dict", {})
                            track_id = metadata.get("id")
                            if track_id:
                                deleted_in_remote.add(str(track_id))
                
                # Filtrar shards que fueron eliminados por el nodo revivido
                valid_shards = {}
                for filename, ranges in current_shards.items():
                    # Verificar si este filename corresponde a un track eliminado
                    is_deleted = any(track_id in filename for track_id in deleted_in_remote)
                    if not is_deleted:
                        valid_shards[filename] = ranges
                
                if not valid_shards:
                    log_info("UPDATE IP", f"No hay shards válidos para matching después de filtrar deletes", 
                            colorize_full=True)
                    return
                
                # Buscar nodo DEAD con shards coincidentes
                matched_old_node = None
                max_coincidences = 0
                
                for old_node_id, state in self.raft_server.node_states.items():
                    if state != "DEAD":
                        continue
                    
                    if old_node_id not in self.raft.global_index.get("node_shards", {}):
                        continue
                    
                    old_shards = self.raft.global_index["node_shards"][old_node_id].get("shards", {})
                    
                    # Contar coincidencias
                    coincidences = 0
                    for filename, ranges in valid_shards.items():
                        if filename in old_shards:
                            matching_ranges = ranges & set(old_shards[filename])
                            coincidences += len(matching_ranges)
                    
                    if coincidences > max_coincidences:
                        max_coincidences = coincidences
                        matched_old_node = old_node_id
                
                if matched_old_node and max_coincidences > 0:
                    log_info("UPDATE IP", 
                            f"Nodo {node_ip} identificado como {matched_old_node} (coincidencias: {max_coincidences} shards)", 
                            colorize_full=True)
                    
                    # Transferir información del nodo viejo al nuevo
                    with self.raft._lock:
                        if matched_old_node in self.raft.global_index.get("node_versions", {}):
                            old_versions = self.raft.global_index["node_versions"][matched_old_node]
                            
                            self.raft.global_index["node_versions"][node_ip] = {
                                "read_version": old_versions.get("read_version", 0),
                                "write_version": old_versions.get("write_version", 0),
                                "db_version": 0,  # Ya fue degradado
                                "db_version_prev": 0,
                                "is_db_node": False
                            }
                            
                            del self.raft.global_index["node_versions"][matched_old_node]
                        
                        # Actualizar files
                        if matched_old_node in self.raft.global_index.get("files", {}):
                            self.raft.global_index["files"][node_ip] = \
                                self.raft.global_index["files"][matched_old_node]
                            del self.raft.global_index["files"][matched_old_node]
                        
                        # Actualizar node_shards
                        if matched_old_node in self.raft.global_index.get("node_shards", {}):
                            self.raft.global_index["node_shards"][node_ip] = \
                                self.raft.global_index["node_shards"][matched_old_node]
                            del self.raft.global_index["node_shards"][matched_old_node]
                        
                        # Actualizar chunk_distribution
                        for filename, metadata in self.raft.global_index.get("files_metadata", {}).items():
                            distribution = metadata.get("chunk_distribution", {})
                            for range_key, nodes in distribution.items():
                                if matched_old_node in nodes:
                                    nodes.remove(matched_old_node)
                                    nodes.append(node_ip)
                    
                    # Actualizar estados
                    with self.raft_server._lock:
                        if matched_old_node in self.raft_server.node_states:
                            del self.raft_server.node_states[matched_old_node]
                        self.raft_server.node_states[node_ip] = "RE-SPAWN"
                    
                    log_success("UPDATE IP", f"IP actualizada: {matched_old_node} -> {node_ip}", 
                            colorize_full=True)
                else:
                    log_warning("UPDATE IP", f"No se encontró nodo DEAD correspondiente a {node_ip}", 
                            colorize_full=True, color="yellow")
            
            except Exception as e:
                log_error("UPDATE IP", f"Error obteniendo shards de {node_ip}: {e}", 
                        colorize_full=True, color="red")
        
        except Exception as e:
            log_error("UPDATE IP", f"Error en _update_respawn_node_ip_improved: {e}", 
                    colorize_full=True, color="red")
            import traceback
            traceback.print_exc()

    def _ensure_replicas_for_respawn_node(self, node_ip: str, leader_json: dict):
        """Asegura k réplicas para todos los shards del nodo revivido"""
        from raft.leader_manager import RemoteStorageManager
        import json
        try:
            remote_storage = RemoteStorageManager()
            
            # Obtener tracks eliminados en el JSON del líder
            deleted_in_leader = set()
            for op in leader_json.get("log", []):
                operation_info = json.loads(op.get("sql_operation", "{}"))
                if (operation_info.get("operation") == "delete" and 
                    operation_info.get("model") == "Track"):
                    params = operation_info.get("params", {})
                    metadata = params.get("metadata_dict", {})
                    track_id = metadata.get("id")
                    if track_id:
                        deleted_in_leader.add(str(track_id))
            
            # Obtener shards del nodo revivido
            files = remote_storage.list_files(node_ip)
            
            for filename in files:
                # Verificar si el archivo fue eliminado en el líder
                is_deleted = any(track_id in filename for track_id in deleted_in_leader)
                if is_deleted:
                    log_info("ENSURE REPLICAS", f"Archivo {filename} marcado como eliminado, saltando", 
                            colorize_full=True, color="orange")
                    continue
                
                ranges = remote_storage.get_file_ranges(filename, node_ip)
                
                for range_key in ranges:
                    # Verificar cuántas réplicas existen actualmente
                    with self.raft._lock:
                        files_metadata = self.raft.global_index.get("files_metadata", {})
                        if filename in files_metadata:
                            distribution = files_metadata[filename].get("chunk_distribution", {})
                            current_replicas = distribution.get(range_key, [])
                            current_replicas = self._filter_active_nodes(current_replicas)
                        else:
                            # Shard único que no está en el índice
                            current_replicas = [node_ip]
                    
                    needed_replicas = self.raft.db_replication_factor - len(current_replicas)
                    
                    if needed_replicas > 0:
                        log_info("ENSURE REPLICAS", 
                                f"Shard {filename}:{range_key} necesita {needed_replicas} réplicas adicionales", 
                                colorize_full=True, color="orange")
                        
                        # Encontrar nodos disponibles
                        from .discovery import discover_active_clients
                        nodes = discover_active_clients()
                        alive_nodes = self._filter_active_nodes(nodes)
                        
                        available_nodes = [n for n in alive_nodes if n not in current_replicas]
                        
                        if not available_nodes:
                            log_warning("ENSURE REPLICAS", "No hay nodos disponibles para replicar", 
                                    colorize_full=True, color="yellow")
                            continue
                        
                        # Ordenar por menor carga
                        node_shards_info = self.raft.global_index.get("node_shards", {})
                        available_nodes.sort(key=lambda n: node_shards_info.get(n, {}).get("total_chunks", 0))
                        
                        # Seleccionar nodos
                        nodes_to_add = available_nodes[:needed_replicas]
                        
                        # Replicar
                        range_data = remote_storage.get_chunk_range(filename, range_key, node_ip)
                        
                        for dest_node in nodes_to_add:
                            try:
                                remote_storage.create_file_range(filename, range_data, range_key, dest_node)
                                
                                # Actualizar índice
                                with self.raft._lock:
                                    if filename not in self.raft.global_index.get("files_metadata", {}):
                                        self.raft.global_index.setdefault("files_metadata", {})[filename] = {
                                            "chunk_distribution": {},
                                            "total_chunks": 0
                                        }
                                    
                                    distribution = self.raft.global_index["files_metadata"][filename].get("chunk_distribution", {})
                                    if range_key not in distribution:
                                        distribution[range_key] = []
                                    
                                    if dest_node not in distribution[range_key]:
                                        distribution[range_key].append(dest_node)
                                    
                                    # Actualizar node_shards
                                    if dest_node not in self.raft.global_index.get("node_shards", {}):
                                        self.raft.global_index.setdefault("node_shards", {})[dest_node] = {
                                            "total_chunks": 0,
                                            "shards": {}
                                        }
                                    
                                    if filename not in self.raft.global_index["node_shards"][dest_node]["shards"]:
                                        self.raft.global_index["node_shards"][dest_node]["shards"][filename] = []
                                    
                                    if range_key not in self.raft.global_index["node_shards"][dest_node]["shards"][filename]:
                                        self.raft.global_index["node_shards"][dest_node]["shards"][filename].append(range_key)
                                        range_start, range_end = map(int, range_key.split("-"))
                                        self.raft.global_index["node_shards"][dest_node]["total_chunks"] += (range_end - range_start)
                                
                                log_success("ENSURE REPLICAS", f"Shard {filename}:{range_key} replicado en {dest_node}", 
                                            colorize_full=True, color="orange")
                            
                            except Exception as e:
                                log_error("ENSURE REPLICAS", f"Error replicando en {dest_node}: {e}", 
                                        colorize_full=True, color="red")
        
        except Exception as e:
            log_error("ENSURE REPLICAS", f"Error asegurando réplicas: {e}", 
                    colorize_full=True, color="red")
            import traceback
            traceback.print_exc()

    def _remove_excess_replicas(self, node_ip: str):
        """Elimina réplicas sobrantes (mantener k=3)"""
        from raft.leader_manager import RemoteStorageManager
        
        try:
            remote_storage = RemoteStorageManager()
            files = remote_storage.list_files(node_ip)
            
            log_info("REMOVE EXCESS", f"Buscando réplicas excedentes para nodo {node_ip}", 
                    colorize_full=True)
            
            deleted_count = 0
            for filename in files:
                ranges = remote_storage.get_file_ranges(filename, node_ip)
                
                for range_key in ranges:
                    # Contar cuántos nodos tienen este shard
                    distribution = self.raft.global_index.get("files_metadata", {}).get(filename, {}).get("chunk_distribution", {})
                    nodes_with_shard = distribution.get(range_key, [])
                    
                    if len(nodes_with_shard) > self.raft.db_replication_factor:
                        # Eliminar del nodo con más chunks
                        node_chunks = {nid: len(self.raft.global_index.get("files", {}).get(nid, [])) 
                                    for nid in nodes_with_shard if nid != node_ip}
                        
                        if node_chunks:
                            max_node = max(node_chunks, key=node_chunks.get)
                            
                            # Intentar eliminar
                            deleted = remote_storage.delete_file_range(filename, range_key, max_node)
                            
                            if deleted:
                                # Actualizar índice
                                with self.raft._lock:
                                    distribution[range_key].remove(max_node)
                                log_info("REMOVE EXCESS", f"Eliminado shard {range_key} de {max_node}", 
                                        colorize_full=True)
                                deleted_count += 1
            
            if deleted_count > 0:
                log_success("REMOVE EXCESS", f"Eliminadas {deleted_count} réplicas excedentes para nodo {node_ip}", 
                            colorize_full=True)
            else:
                log_info("REMOVE EXCESS", f"No se encontraron réplicas excedentes para nodo {node_ip}", 
                        colorize_full=True)
        
        except Exception as e:
            log_error("REMOVE EXCESS", f"Error eliminando réplicas sobrantes: {e}", 
                    colorize_full=True, color="red")

    # ============================================================================
    # BALANCEO DE CARGA
    # ============================================================================
    def _balance_shards_general(self):
        """Balancea shards encontrando el nodo con menos chunks y balanceando hacia él"""
        try:
            from .discovery import discover_active_clients
            nodes = discover_active_clients()
            alive_nodes = self._filter_active_nodes(nodes)
            
            if len(alive_nodes) <= self.raft.db_replication_factor:
                log_warning("BALANCE GENERAL", f"Solo hay {len(alive_nodes)} nodos para balanceo", 
                        colorize_full=True)
            
            # Encontrar nodo con menos chunks
            with self.raft._lock:
                node_shards_info = self.raft.global_index.get("node_shards", {})
            
            node_loads = []
            for node in alive_nodes:
                total_chunks = node_shards_info.get(node, {}).get("total_chunks", 0)
                node_loads.append((node, total_chunks))
            
            # Ordenar por carga (menor primero)
            node_loads.sort(key=lambda x: (x[1], x[0]))
            
            if not node_loads:
                log_info("BALANCE GENERAL", "No hay nodos con información de carga", 
                        colorize_full=True, color="orange")
                return
            
            target_node = node_loads[0][0]
            target_load = node_loads[0][1]
            
            log_info("BALANCE GENERAL", f"Nodo con menor carga: {target_node} ({target_load} chunks)", 
                    colorize_full=True, color="orange")
            
            # Llamar al algoritmo de balanceo existente con el target correcto
            self._balance_shards(target_node)
            
        except Exception as e:
            log_error("BALANCE GENERAL", f"Error en balanceo general: {e}", 
                    colorize_full=True, color="red")
            import traceback
            traceback.print_exc()

    def _balance_shards(self, target_node: str):
        """
        Algoritmo mejorado de balanceo:
        1. Nodo actual a balancear es el target
        2. Lista negra vacía inicialmente
        3. Mientras el target sea el de menor cantidad de chunks:
        - Tomar nodo con más chunks
        - De ese nodo, tomar shard de mayor rango que no esté en lista negra y que target no tenga
        - Agregar a tareas: copiar a target y eliminar del origen
        - Agregar shard a lista negra
        - Recalcular en simulación o sea fingiendo que ya se ejecuto la decisión
        4. Ejecutar tareas solo si copias son exitosas
        """
        
        # Info - anaranjado
        log_info("BALANCE SHARDS", f"Iniciando algoritmo mejorado de balanceo para nodo: {target_node}", 
                colorize_full=True, color="orange")
        
        import json
        import copy
        def serialize_with_sets(obj):
            """Serialize object with sets to JSON."""
            if isinstance(obj, dict):
                return {k: serialize_with_sets(v) for k, v in obj.items()}
            elif isinstance(obj, (list, tuple)):
                return [serialize_with_sets(item) for item in obj]
            elif isinstance(obj, set):
                return list(obj)
            else:
                return obj
            
        global_index_copy = copy.deepcopy(self.raft.global_index)
        serializable_data = serialize_with_sets(global_index_copy)
        json_global_index = json.dumps(serializable_data, indent=7)

        log_info("BALANCE SHARDS",f"INDICE GLOBAL: {json_global_index}", 
                colorize_full=True, color="orange")
        try:
            # Obtener nodos vivos
            from .discovery import discover_active_clients
            nodes = discover_active_clients()
            alive_nodes = self._filter_active_nodes(nodes)
            
            # Info - anaranjado
            log_info("BALANCE SHARDS", f"Nodos vivos detectados: {alive_nodes}", 
                    colorize_full=True, color="orange")
            
            
            # Crear copia del estado actual para simulación
            import copy 
            with self.raft._lock:
                global_index_copy = copy.deepcopy(self.raft.global_index)

            files_metadata_copy = global_index_copy.get("files_metadata", {})         
            node_shards_copy = global_index_copy.get("node_shards", {})
            
            # Inicializar nodo si no existe en la copia
            if target_node not in node_shards_copy:
                node_shards_copy[target_node] = {
                    "total_chunks": 0,
                    "shards": {}
                }
                log_info("BALANCE SHARDS", f"Inicializado nodo {target_node} en simulación (0 chunks)", 
                        colorize_full=True, color="orange")
            
            # Lista negra: shards que ya hemos considerado
            blacklisted_shards = set()
            
            # Tareas de balanceo: lista de (shard_info, source_node)
            balance_tasks = []
            
            # Mientras target sea el nodo con menos chunks
            while True:
                # Calcular total de chunks por nodo 
                node_totals = {}
                for node in alive_nodes:
                    if node in node_shards_copy:
                        node_totals[node] = node_shards_copy[node]["total_chunks"]
                    else:
                        node_totals[node] = 0
                
                # Encontrar nodo con MÁS chunks (excluyendo target si es el máximo)
                source_candidates = [n for n in node_totals]
                if not source_candidates:
                    log_info("BALANCE SHARDS", "No hay candidatos para balanceo", 
                            colorize_full=True, color="orange")
                    break
                    
                max_node = max(source_candidates, key=lambda n: node_totals[n])
                source_candidates_copy = source_candidates.copy()
                source_candidates_copy.remove(max_node)
                second_max_node = max(source_candidates_copy, key=lambda n: node_totals[n])

                if max_node == target_node:
                    log_info("BALANCE SHARDS", f"El nodo {target_node} es el que más chunks tiene, terminando balanceo", 
                            colorize_full=True, color="orange")
                    break

                max_chunks = node_totals[max_node]
                second_max_chunks = node_totals[second_max_node]
                target_chunks = node_totals[target_node]
                
                # Si target ya no es el que menos tiene, terminar
                if target_chunks > min(node_totals.values()):
                    log_info("BALANCE SHARDS", 
                            f"Target {target_node} ya no es el de menor carga ({target_chunks} vs mínimo {min(node_totals.values())})", 
                            colorize_full=True, color="orange")
                    break
                
                log_info("BALANCE SHARDS", 
                        f"Estado actual - Target: {target_node} ({target_chunks}), Máximo: {max_node} ({max_chunks}), 2do Máximo: {second_max_node} ({second_max_chunks})", 
                        colorize_full=True, color="orange")
                
                # Buscar shard en max_node para mover a target
                shard_info = None
                
                # Obtener shards del nodo máximo
                max_node_shards = node_shards_copy.get(max_node, {}).get("shards", {})
                second_max_node_shards = node_shards_copy.get(second_max_node, {}).get("shards", {})
                
                # Buscar el shard de mayor tamaño que no esté en lista negra
                # y que target no tenga
                candidate_shards = self._get_candidates_for_max_node(target_node=target_node, max_node_shards=max_node_shards, node_shards_copy=node_shards_copy, files_metadata_copy=files_metadata_copy, blacklisted_shards=blacklisted_shards)
                
                # Si no hay candidatos, reintentar con el next max
                if not candidate_shards:
                    log_info("BALANCE SHARDS", f"No hay shards candidatos para mover desde {max_node}. Reintentando con {second_max_node}", 
                            colorize_full=True, color="orange")
                    
                    candidate_shards = self._get_candidates_for_max_node(target_node=target_node, max_node_shards=second_max_node_shards, node_shards_copy=node_shards_copy, files_metadata_copy=files_metadata_copy, blacklisted_shards=blacklisted_shards)

                    if not candidate_shards:
                        log_info("BALANCE SHARDS", f"No hay shards candidatos para mover desde {second_max_node}. Terminando.", 
                                colorize_full=True, color="orange")
                        break
                
                # Ordenar por tamaño (mayor primero)
                candidate_shards.sort(key=lambda x: x["size"], reverse=True)
                
                # Tomar el shard de mayor tamaño
                selected_shard = candidate_shards[0]
                shard_info = {
                    "filename": selected_shard["filename"],
                    "range_key": selected_shard["range_key"],
                    "size": selected_shard["size"],
                    "shard_id": selected_shard["shard_id"]
                }
                
                # Verificar que al mover no se pierda replicación
                file_meta = files_metadata_copy.get(shard_info["filename"], {})
                distribution = file_meta.get("chunk_distribution", {})
                current_replicas = self._filter_active_nodes(distribution.get(shard_info["range_key"], []))
                
                # AGREGAR TAREA DE BALANCEO
                balance_tasks.append({
                    "action": "copy",
                    "source_node": max_node,
                    "target_node": target_node,
                    "filename": shard_info["filename"],
                    "range_key": shard_info["range_key"],
                    "size": shard_info["size"],
                    "shard_id": shard_info["shard_id"]
                })
                
                # AGREGAR TAREA DE ELIMINACIÓN (condicional)
                balance_tasks.append({
                    "action": "delete",
                    "node": max_node,
                    "filename": shard_info["filename"],
                    "range_key": shard_info["range_key"],
                    "size": shard_info["size"],
                    "shard_id": shard_info["shard_id"],
                    "depends_on": len(balance_tasks) - 1  # Depende de la copia anterior
                })
                
                # Agregar a lista negra
                blacklisted_shards.add(shard_info["shard_id"])
                
                # ACTUALIZAR COPIAS
                # 1. Agregar shard a target en simulación
                if shard_info["filename"] not in node_shards_copy[target_node]["shards"]:
                    node_shards_copy[target_node]["shards"][shard_info["filename"]] = []
                
                if shard_info["range_key"] not in node_shards_copy[target_node]["shards"][shard_info["filename"]]:
                    node_shards_copy[target_node]["shards"][shard_info["filename"]].append(shard_info["range_key"])
                    node_shards_copy[target_node]["total_chunks"] += shard_info["size"]
                
                # 2. Actualizar files_metadata 
                if shard_info["filename"] not in files_metadata_copy:
                    files_metadata_copy[shard_info["filename"]] = {"chunk_distribution": {}}
                
                if shard_info["range_key"] not in files_metadata_copy[shard_info["filename"]]["chunk_distribution"]:
                    files_metadata_copy[shard_info["filename"]]["chunk_distribution"][shard_info["range_key"]] = []
                
                if target_node not in files_metadata_copy[shard_info["filename"]]["chunk_distribution"][shard_info["range_key"]]:
                    files_metadata_copy[shard_info["filename"]]["chunk_distribution"][shard_info["range_key"]].append(target_node)
                
                # 3. Eliminar shard de source en copia (solo de node_shards, no de metadata)
                if shard_info["range_key"] in node_shards_copy[max_node]["shards"].get(shard_info["filename"], []):
                    node_shards_copy[max_node]["shards"][shard_info["filename"]].remove(shard_info["range_key"])
                    node_shards_copy[max_node]["total_chunks"] -= shard_info["size"]
                    
                    # Limpiar estructura si queda vacía
                    if not node_shards_copy[max_node]["shards"][shard_info["filename"]]:
                        del node_shards_copy[max_node]["shards"][shard_info["filename"]]
                
                log_info("BALANCE SHARDS", 
                        f"Shard {shard_info['shard_id']} ({shard_info['size']} chunks) programado para mover de {max_node} a {target_node}", 
                        colorize_full=True, color="orange")
            
            # EJECUTAR TAREAS DE BALANCEO
            if balance_tasks:
                copy_count = len([t for t in balance_tasks if t['action'] == 'copy'])
                log_info("BALANCE SHARDS", f"Ejecutando {copy_count} tareas de balanceo", 
                        colorize_full=True, color="orange")
                self._execute_balance_tasks(balance_tasks, target_node)
                log_success("BALANCE SHARDS", f"Balanceo completado exitosamente para nodo {target_node}", 
                            colorize_full=True, color="orange")
            else:
                log_info("BALANCE SHARDS", "No se generaron tareas de balanceo", 
                        colorize_full=True, color="orange")
            
        except Exception as e:
            log_error("BALANCE SHARDS", f"Error en algoritmo mejorado de balanceo: {e}", 
                    colorize_full=True, color="red")
            import traceback
            traceback.print_exc()

    def _get_candidates_for_max_node(self, target_node: str, max_node_shards: dict, node_shards_copy: dict, files_metadata_copy: dict, blacklisted_shards: list):
        """Obtiene candidatos para mover desde el nodo con más chunks"""
        candidate_shards = []
        
        for filename, range_keys in max_node_shards.items():
            for range_key in range_keys:
                # Verificar si está en lista negra
                shard_id = f"{filename}:{range_key}"
                if shard_id in blacklisted_shards:
                    continue
                
                # Verificar si target ya tiene este shard
                target_has_shard = False
                if target_node in node_shards_copy:
                    target_shards = node_shards_copy[target_node].get("shards", {})
                    if filename in target_shards and range_key in target_shards[filename]:
                        target_has_shard = True
                
                # Verificar en files_metadata también
                file_meta = files_metadata_copy.get(filename, {})
                distribution = file_meta.get("chunk_distribution", {})
                if range_key in distribution and target_node in distribution[range_key]:
                    target_has_shard = True
                
                if not target_has_shard:
                    # Calcular tamaño del shard
                    range_start, range_end = map(int, range_key.split("-"))
                    shard_size = range_end - range_start
                    
                    candidate_shards.append({
                        "filename": filename,
                        "range_key": range_key,
                        "size": shard_size,
                        "shard_id": shard_id
                    })
        
        return candidate_shards

    def _execute_balance_tasks(self, balance_tasks: list, target_node: str):
        """
        Ejecuta las tareas de balanceo en orden.
        Solo elimina shards del origen si la copia fue exitosa.
        """
        from raft.leader_manager import RemoteStorageManager
        
        remote_storage = RemoteStorageManager()
        executed_copies = {}  # shard_id -> éxito
        
        # Info - anaranjado
        log_info("EXECUTE BALANCE", f"Iniciando ejecución de {len(balance_tasks)} tareas de balanceo", 
                colorize_full=True, color="orange")
        
        # Primera fase: Ejecutar todas las copias
        for task in balance_tasks:
            if task["action"] == "copy":
                
                try:
                    log_info("EXECUTE BALANCE", 
                            f"Copiando shard {task['shard_id']} ({task['size']} chunks) desde {task['source_node']} a {task['target_node']}", 
                            colorize_full=True, color="orange")
                        
                    # Actualizar índices globales
                    import copy
                    with self.raft._lock:
                        global_index_copy = copy.deepcopy(self.raft.global_index)
                        log_info("EXECUTE BALANCE", "Se copió el índice global para tareas de balance (COPIA)", 
                                colorize_full=True, color="orange")

                    # Leer del nodo origen
                    range_data = remote_storage.get_chunk_range(
                        task["filename"], task["range_key"], task["source_node"]
                    )
                    
                    # Escribir en nodo destino
                    remote_storage.create_file_range(
                        task["filename"], range_data, task["range_key"], task["target_node"]
                    )

                    # Actualizar files_metadata
                    files_metadata = global_index_copy.get("files_metadata", {})
                    if task["filename"] in files_metadata:
                        distribution = files_metadata[task["filename"]].get("chunk_distribution", {})
                        if task["range_key"] in distribution:
                            if task["target_node"] not in distribution[task["range_key"]]:
                                distribution[task["range_key"]].append(task["target_node"])
                                files_metadata[task["filename"]]["chunk_distribution"] = distribution
                    global_index_copy["files_metadata"] = files_metadata
                    
                    # Actualizar node_shards para target
                    node_shards_info = global_index_copy.get("node_shards", {})
                    if task["target_node"] not in node_shards_info:
                        node_shards_info[task["target_node"]] = {
                            "total_chunks": 0,
                            "shards": {}
                        }
                    
                    if task["filename"] not in node_shards_info[task["target_node"]]["shards"]:
                        node_shards_info[task["target_node"]]["shards"][task["filename"]] = []
                    
                    if task["range_key"] not in node_shards_info[task["target_node"]]["shards"][task["filename"]]:
                        node_shards_info[task["target_node"]]["shards"][task["filename"]].append(task["range_key"])
                        node_shards_info[task["target_node"]]["total_chunks"] += task["size"]
                    
                    global_index_copy["node_shards"] = node_shards_info
                    
                    # Actualizar files
                    if task["target_node"] not in global_index_copy["files"]:
                        global_index_copy["files"][task["target_node"]] = []
                    
                    if task["filename"] not in global_index_copy["files"][task["target_node"]]:
                        global_index_copy["files"][task["target_node"]].append(task["filename"])
                
                    executed_copies[task["shard_id"]] = True

                    log_success("EXECUTE BALANCE", f"Copia exitosa de shard {task['shard_id']}", 
                            colorize_full=True, color="orange")

                    with self.raft._lock:
                        self.raft.global_index["node_shards"] = global_index_copy["node_shards"]
                        self.raft.global_index["files_metadata"] = global_index_copy["files_metadata"]
                        self.raft.global_index["files"] = global_index_copy["files"]
                                                                                  
                        log_info("EXECUTE BALANCE", "Se actualizó el índice global con las tareas de balanceo (COPIA)", 
                                colorize_full=True, color="orange")
                    
                except Exception as e:
                    log_error("EXECUTE BALANCE", f"Error copiando shard {task['shard_id']}: {e}", 
                            colorize_full=True, color="red")
                    executed_copies[task["shard_id"]] = False
        
        # Segunda fase: Eliminar shards del origen solo si la copia fue exitosa
        for task in balance_tasks:
            if task["action"] == "delete":
                shard_id = task["shard_id"]
                
                # Verificar si la copia correspondiente fue exitosa
                copy_task_id = task.get("depends_on")
                if copy_task_id is not None and 0 <= copy_task_id < len(balance_tasks):
                    copy_task = balance_tasks[copy_task_id]
                    if executed_copies.get(copy_task["shard_id"], False):
                        try:
                            # Verificar que todavía hay suficientes réplicas
                            with self.raft._lock:
                                global_index_copy = copy.deepcopy(self.raft.global_index)
                                log_info("EXECUTE BALANCE", "Se copió el índice global para tareas de balance (ELIMINAR)", 
                                        colorize_full=True, color="orange")
                                
                            files_metadata = global_index_copy.get("files_metadata", {})
                            distribution = files_metadata.get(task["filename"], {}).get("chunk_distribution", {})
                            current_replicas = distribution.get(task["range_key"], [])
                            current_replicas = self._filter_active_nodes(current_replicas)

                            if len(current_replicas) > self.raft.db_replication_factor:
                                log_info("EXECUTE BALANCE", 
                                        f"Eliminando shard {shard_id} de {task['node']} (réplicas vivas: {len(current_replicas)})", 
                                        colorize_full=True, color="orange")
                                
                                # Eliminar físicamente
                                deleted = remote_storage.delete_file_range(
                                    task["filename"], task["range_key"], task["node"]
                                )

                                if deleted:
                                    # Actualizar índices
                                    files_metadata[task["filename"]]["chunk_distribution"][task["range_key"]].remove(task["node"])
                                    node_shards_info = global_index_copy.get("node_shards", {})
                                     
                                    if task["node"] in node_shards_info:
                                        if task["filename"] in node_shards_info[task["node"]]["shards"]:
                                            if task["range_key"] in node_shards_info[task["node"]]["shards"][task["filename"]]:
                                                node_shards_info[task["node"]]["shards"][task["filename"]].remove(task["range_key"])
                                                node_shards_info[task["node"]]["total_chunks"] -= task["size"]
                                                
                                                # if not node_shards_info[task["node"]]["shards"][task["filename"]]:
                                                #     del node_shards_info[task["node"]]["shards"][task["filename"]]
                                    
                                    for range_key in global_index_copy["files_metadata"][task["filename"]]["chunk_distribution"].keys():
                                        if task["node"] in global_index_copy["files_metadata"][task["filename"]]["chunk_distribution"][range_key] and task["filename"] not in global_index_copy["files"][task["node"]]:
                                            global_index_copy["files"][task["node"]].append(task["filename"])
                                            
                                        
                                    log_success("EXECUTE BALANCE", f"Eliminación exitosa de shard {shard_id}", 
                                            colorize_full=True, color="orange")

                                    with self.raft._lock:
                                        self.raft.global_index["node_shards"] = node_shards_info
                                        self.raft.global_index["files_metadata"] = files_metadata
                                        self.raft.global_index["files"][task["node"]] = global_index_copy["files"][task["node"]]

                                        log_info("EXECUTE BALANCE", "Se actualizó el índice global con las tareas de balanceo (ELIMINAR)", 
                                                colorize_full=True, color="orange")
                                else:
                                    log_warning("EXECUTE BALANCE", f"No se pudo eliminar shard {shard_id}", 
                                            colorize_full=True, color="yellow")
                            else:
                                log_warning("EXECUTE BALANCE", 
                                        f"No se elimina shard {shard_id}: solo hay {len(current_replicas)} réplicas", 
                                        colorize_full=True, color="yellow")
                            
                        except Exception as e:
                            log_error("EXECUTE BALANCE", f"Error eliminando shard {shard_id}: {e}", 
                                    colorize_full=True, color="red")
                    else:
                        log_warning("EXECUTE BALANCE", 
                                f"No se elimina shard {shard_id}: la copia no fue exitosa", 
                                colorize_full=True, color="yellow")
        
        import json
        import copy
        def serialize_with_sets(obj):
            """Serialize object with sets to JSON."""
            if isinstance(obj, dict):
                return {k: serialize_with_sets(v) for k, v in obj.items()}
            elif isinstance(obj, (list, tuple)):
                return [serialize_with_sets(item) for item in obj]
            elif isinstance(obj, set):
                return list(obj)
            else:
                return obj
            
        global_index_copy = copy.deepcopy(self.raft.global_index)
        serializable_data = serialize_with_sets(global_index_copy)
        json_global_index = json.dumps(serializable_data, indent=7)

        log_info("BALANCE SHARDS",f"INDICE GLOBAL: {json_global_index}", 
                colorize_full=True, color="orange")
        log_success("EXECUTE BALANCE", f"Ejecución de balanceo completada para nodo {target_node}", 
                    colorize_full=True, color="orange")

    def _restore_replication_factor(self):
        """
        Restaura el factor de replicación k para todos los archivos.
        Si hay menos nodos activos que k, reduce k temporalmente.
        """
        from .discovery import discover_active_clients
        
        # ✅ OBTENER NODOS ACTIVOS AL INICIO
        active_nodes = set(discover_active_clients())
        active_nodes = set(self._filter_active_nodes(list(active_nodes)))
        
        log_info("RESTORE REPLICATION", f"Iniciando restauración de factor de replicación (k={self.raft.db_replication_factor})",
                colorize_full=True, color="cyan")
        
        with self.raft._lock:
            files_metadata = self.raft.global_index.get("files_metadata", {})
            if not files_metadata:
                log_info("RESTORE REPLICATION", "No hay archivos que restaurar",
                        colorize_full=True, color="cyan")
                return
        
        # REDUCIR k SI HAY MENOS NODOS ACTIVOS
        k = self.raft.db_replication_factor
        num_active_nodes = len(active_nodes)
        
        if num_active_nodes < k:
            k = num_active_nodes
            log_warning("RESTORE REPLICATION", 
                    f"Reduciendo k de {self.raft.db_replication_factor} a {k} (solo {num_active_nodes} nodos activos)",
                    colorize_full=True, color="yellow")
        
        # CONSTRUIR LISTA DE CAMBIOS DE REPLICACIÓN
        replication_changes = []
        
        for filename, file_info in files_metadata.items():
            distribution = file_info.get("chunk_distribution", {})
            
            for range_key, nodes_with_shard in distribution.items():
                # FILTRAR SOLO NODOS ACTIVOS
                active_nodes_with_shard = [n for n in nodes_with_shard if n in active_nodes]
                current_replicas = len(active_nodes_with_shard)
                
                if current_replicas < k:
                    missing_count = k - current_replicas
                    log_info("RESTORE REPLICATION", 
                            f"Archivo {filename} rango {range_key}: {current_replicas}/{k} réplicas activas (faltan {missing_count})",
                            colorize_full=True, color="yellow")
                    
                    # Seleccionar nodos destino que no tengan el shard
                    available_destinations = [n for n in active_nodes if n not in active_nodes_with_shard]
                    
                    # Limitar a los nodos necesarios
                    selected_destinations = available_destinations[:missing_count]
                    
                    # Usar el primer nodo activo como origen
                    source_node = active_nodes_with_shard[0] if active_nodes_with_shard else None
                    
                    if source_node and selected_destinations:
                        for dest_node in selected_destinations:
                            replication_changes.append({
                                "filename": filename,
                                "range_key": range_key,
                                "source_node": source_node,
                                "dest_node": dest_node
                            })
                    else:
                        log_warning("RESTORE REPLICATION", 
                                f"No se puede replicar {filename} rango {range_key}: sin nodos origen o destino disponibles",
                                colorize_full=True, color="yellow")
        
        if not replication_changes:
            log_success("RESTORE REPLICATION", "Factor de replicación ya está satisfecho",
                    colorize_full=True, color="cyan")
            return
        
        # EJECUTAR REPLICACIÓN USANDO _execute_replication_changes
        log_info("RESTORE REPLICATION", f"Se necesita replicar {len(replication_changes)} shards",
                colorize_full=True, color="cyan")
        
        self._execute_replication_changes(replication_changes)

    def _execute_replication_changes(self, replication_changes):
        """Ejecuta la replicación de shards entre nodos"""
        from raft.leader_manager import RemoteStorageManager
        
        remote_storage = RemoteStorageManager()
        
        log_info("EXECUTE REPLICATION", f"Iniciando ejecución de {len(replication_changes)} cambios de replicación", 
                colorize_full=True, color="orange")
        
        for change in replication_changes:
            try:
                log_info("EXECUTE REPLICATION", 
                        f"Replicando shard {change['range_key']} de {change['filename']} desde {change['source_node']} a {change['dest_node']}", 
                        colorize_full=True, color="orange")
                
                # Leer el shard del nodo origen
                range_start, range_end = map(int, change['range_key'].split("-"))
                shard_size = range_end - range_start
                
                range_data = remote_storage.get_chunk_range(
                    change['filename'], change['range_key'], change['source_node']
                )
                
                # ✅ FIX: Validar que obtuvimos datos
                if not range_data or len(range_data) == 0:
                    log_error("EXECUTE REPLICATION", 
                            f"No se obtuvieron datos del shard {change['range_key']} desde {change['source_node']}", 
                            colorize_full=True, color="red")
                    continue
                
                # Escribir en el nodo destino
                remote_storage.create_file_range(
                    change['filename'], range_data, change['range_key'], change['dest_node']
                )
                
                # ✅ FIX: VERIFICAR que la replicación fue exitosa leyendo del nodo destino
                try:
                    log_info("EXECUTE REPLICATION", f"Verificando replicación en {change['dest_node']}...", 
                            colorize_full=True, color="orange")
                    
                    # Intentar leer el rango que acabamos de escribir
                    verification_data = remote_storage.get_chunk_range(
                        change['filename'], change['range_key'], change['dest_node']
                    )
                    
                    if not verification_data or len(verification_data) != len(range_data):
                        log_error("EXECUTE REPLICATION", 
                                f"Verificación falló: datos escritos ({len(range_data)} bytes) != datos leídos ({len(verification_data) if verification_data else 0} bytes)", 
                                colorize_full=True, color="red")
                        # No actualizar índice si la verificación falló
                        continue
                    
                    log_info("EXECUTE REPLICATION", f"Verificación exitosa: {len(verification_data)} bytes", 
                            colorize_full=True, color="orange")
                    
                except Exception as e:
                    log_error("EXECUTE REPLICATION", 
                            f"Error verificando replicación en {change['dest_node']}: {e}", 
                            colorize_full=True, color="red")
                    # No actualizar índice si la verificación falló
                    continue
                
                # Solo actualizar índices si la verificación fue exitosa
                import copy
                with self.raft._lock:
                    global_index_copy = copy.deepcopy(self.raft.global_index)
                    log_info("EXECUTE REPLICATION", "Se copió el índice global para tareas de replicación", 
                            colorize_full=True, color="orange")

                # Actualizar files_metadata
                files_metadata = global_index_copy.get("files_metadata", {})
                if change['filename'] in files_metadata:
                    distribution = files_metadata[change['filename']].get("chunk_distribution", {})
                    if change['range_key'] in distribution:
                        if change['dest_node'] not in distribution[change['range_key']]:
                            distribution[change['range_key']].append(change['dest_node'])
                
                # Actualizar node_shards para destino
                node_shards_info = global_index_copy.get("node_shards", {})
                if change['dest_node'] not in node_shards_info:
                    node_shards_info[change['dest_node']] = {
                        "total_chunks": 0,
                        "shards": {}
                    }
                
                if change['filename'] not in node_shards_info[change['dest_node']]["shards"]:
                    node_shards_info[change['dest_node']]["shards"][change['filename']] = []
                
                if change['range_key'] not in node_shards_info[change['dest_node']]["shards"][change["filename"]]:
                    node_shards_info[change['dest_node']]["shards"][change['filename']].append(change['range_key'])
                    node_shards_info[change['dest_node']]["total_chunks"] += shard_size
                
                # Actualizar files
                if change['dest_node'] not in global_index_copy["files"]:
                    global_index_copy["files"][change['dest_node']] = []
                
                if change['filename'] not in global_index_copy["files"][change['dest_node']]:
                    global_index_copy["files"][change['dest_node']].append(change['filename'])

                with self.raft._lock:
                    self.raft.global_index["node_shards"] = global_index_copy["node_shards"]
                    self.raft.global_index["files"] = global_index_copy["files"]
                    self.raft.global_index["files_metadata"] = global_index_copy["files_metadata"]

                    log_info("EXECUTE REPLICATION", "Se actualizó el índice global con las tareas de replicación exitosa", 
                            colorize_full=True, color="orange")
                
                log_success("EXECUTE REPLICATION", f"Shard replicado exitosamente a {change['dest_node']}", 
                            colorize_full=True, color="orange")
                
            except Exception as e:
                log_error("EXECUTE REPLICATION", f"Error replicando shard: {e}", 
                        colorize_full=True, color="red")
                import traceback
                log_error("EXECUTE REPLICATION", traceback.format_exc(), 
                        colorize_full=True, color="red")
        
        log_success("EXECUTE REPLICATION", f"Ejecución de replicación completada", 
                    colorize_full=True, color="orange")

    def _cleanup_excess_replicas(self):
        """Elimina réplicas excedentes después del balanceo"""
        from raft.leader_manager import RemoteStorageManager
        
        try:
            remote_storage = RemoteStorageManager()
            
            log_info("CLEANUP REPLICAS", "Iniciando limpieza de réplicas excedentes", 
                    colorize_full=True, color="orange")
            
            with self.raft._lock:
                files_metadata = self.raft.global_index.get("files_metadata", {})
                node_shards_info = self.raft.global_index.get("node_shards", {})
                
                deleted_count = 0
                
                for filename, file_info in files_metadata.items():
                    distribution = file_info.get("chunk_distribution", {})
                    
                    for range_key, nodes in distribution.items():
                        nodes_str = ", ".join(nodes)
                        alives = self._filter_active_nodes(nodes)
                        alives_str = ", ".join(alives)
                        log_info("CLEANUP REPLICAS", f"Distribucion de chunks {range_key} en {nodes_str}, de ellos vivos {alives_str}")
                        nodes = alives
                        
                        if len(nodes) > self.raft.db_replication_factor:
                            # Ordenar nodos por cantidad de chunks (los que más tienen primero)
                            nodes_by_load = sorted(
                                nodes,
                                key=lambda n: node_shards_info.get(n, {}).get("total_chunks", 0),
                                reverse=True
                            )
                            
                            # Eliminar de los nodos más cargados
                            excess_count = len(nodes) - self.raft.db_replication_factor
                            nodes_to_remove = nodes_by_load[:excess_count]
                            
                            for node in nodes_to_remove:
                                log_info("CLEANUP REPLICAS", f"Eliminando réplica excedente de {range_key} de {node}", 
                                        colorize_full=True, color="orange")
                                
                                # Eliminar físicamente
                                remote_storage.delete_file_range(filename, range_key, node)
                                
                                # Actualizar índices
                                if node in node_shards_info:
                                    if filename in node_shards_info[node]["shards"]:
                                        if range_key in node_shards_info[node]["shards"][filename]:
                                            node_shards_info[node]["shards"][filename].remove(range_key)
                                            node_shards_info[node]["total_chunks"] -= (
                                                int(range_key.split("-")[1]) - int(range_key.split("-")[0])
                                            )
                                            
                                            if not node_shards_info[node]["shards"][filename]:
                                                del node_shards_info[node]["shards"][filename]
                                
                                # Eliminar de distribution
                                if node in distribution[range_key]:
                                    distribution[range_key].remove(node)
                                
                                deleted_count += 1
            
            if deleted_count > 0:
                log_success("CLEANUP REPLICAS", f"Limpieza completada: {deleted_count} réplicas excedentes eliminadas", 
                            colorize_full=True, color="orange")
            else:
                log_info("CLEANUP REPLICAS", "No se encontraron réplicas excedentes para eliminar", 
                        colorize_full=True, color="orange")
        
        except Exception as e:
            log_error("CLEANUP REPLICAS", f"Error limpiando réplicas excedentes: {e}", 
                    colorize_full=True, color="red")

   
    # ============================================================================
    # MANEJO DE BASE DE DATOS DE METADATOS
    # ============================================================================
    def _check_leader_change_notification(self):
        """Verifica si hay aviso de sincronización al convertirse en líder"""
        try:
            db_json = self.raft_server.db_instance.json_manager.read()
            
            if db_json.get("become_new_leader", False):
                log_info("LEADER SYNC", "Detectada señal de nuevo líder, iniciando sincronización", 
                        colorize_full=True, color="cyan")
                
                # Encontrar nodo DB con mayor db_version
                with self.raft._lock:
                    db_nodes = list(self.raft.global_index.get("db_nodes", set()))
                
                if not db_nodes:
                    log_warning("LEADER SYNC", "No hay nodos DB para sincronizar", 
                            colorize_full=True, color="yellow")
                    # Limpiar señal
                    db_json["become_new_leader"] = False
                    self.raft_server.db_instance.json_manager.write(db_json)
                    return
                
                db_nodes = self._filter_active_nodes(db_nodes)
                
                # Obtener db_version de cada nodo DB
                remote_db = RemoteDBManager()
                node_versions = []
                
                for node_id in db_nodes:
                    if node_id == self.raft.node_id:
                        continue
                    try:
                        node_json = remote_db.get_json_dump(node_id)
                        if node_json:
                            node_versions.append({
                                "node_id": node_id,
                                "db_version": node_json.get("db_version", 0),
                                "json": node_json
                            })
                    except Exception as e:
                        log_warning("LEADER SYNC", f"No se pudo obtener JSON de {node_id}: {e}", 
                                colorize_full=True, color="yellow")
                
                if not node_versions:
                    log_warning("LEADER SYNC", "No se pudo obtener JSON de ningún nodo DB", 
                            colorize_full=True, color="yellow")
                    db_json["become_new_leader"] = False
                    self.raft_server.db_instance.json_manager.write(db_json)
                    return
                
                # Ordenar por db_version (mayor primero) y luego alfabéticamente por IP
                node_versions.sort(key=lambda x: (-x["db_version"], x["node_id"]))
                best_source = node_versions[0]
                
                log_info("LEADER SYNC", f"Sincronizando desde nodo {best_source['node_id']} (db_version: {best_source['db_version']})", 
                        colorize_full=True, color="cyan")
                
                # Restaurar JSON
                self.raft_server.db_instance.restore_json_from_dump(best_source["json"])
                
                # Ejecutar operaciones pending
                self.raft_server.db_instance.execute_pending_operations_from_json()
                
                # Actualizar versión
                self.raft_server.db_instance.update_db_version(best_source["db_version"])
                
                # Marcar como nodo DB
                with self.raft._lock:
                    self.raft.global_index["db_nodes"].add(self.raft.node_id)
                    if self.raft.node_id in self.raft.global_index["node_versions"]:
                        self.raft.global_index["node_versions"][self.raft.node_id]["is_db_node"] = True
                        self.raft.global_index["node_versions"][self.raft.node_id]["db_version"] = best_source["db_version"]
                
                log_success("LEADER SYNC", "Sincronización de nuevo líder completada", 
                            colorize_full=True, color="cyan")
                
        except Exception as e:
            log_error("LEADER SYNC", f"Error en sincronización de nuevo líder: {e}", 
                    colorize_full=True, color="red")
            import traceback
            traceback.print_exc()

    def _manage_db_nodes(self):
        """
        Gestiona los nodos de base de datos para mantener exactamente k nodos
        (líder + k-1 adicionales).
        """
        #TODO: Manejar caso de particion de red, sincronizar DBs
        if self.raft.state != "leader":
            return
        
        k = self.raft.db_replication_factor
        
        # Info - cyan para gestión de DB
        log_info("MANAGE DB NODES", f"Iniciando gestión de nodos DB (k={k})", 
                colorize_full=True, color="cyan")
        
        with self.raft._lock:
            current_db_nodes = self.raft.global_index["db_nodes"].copy()

        from .discovery import discover_active_clients
        nodes = discover_active_clients()
        time.sleep(0.1)
        active_nodes = self._filter_active_nodes(nodes)
        
        log_info("MANAGE DB NODES", f"Nodos DB actuales: {current_db_nodes}", 
                colorize_full=True, color="cyan")
        log_info("MANAGE DB NODES", f"Nodos activos totales: {active_nodes}", 
                colorize_full=True, color="cyan")
        
        # Asegurar que el líder esté en db_nodes
        if self.raft.node_id not in current_db_nodes:
            with self.raft._lock:
                current_db_nodes.add(self.raft.node_id)
                log_info("MANAGE DB NODES", f"Líder {self.raft.node_id} agregado a db_nodes", 
                        colorize_full=True, color="cyan")
        
        # Contar los nodos DB vivos reales
        real_db_nodes = [n for n in current_db_nodes if n in active_nodes]

        # Contar nodos DB activos (excluyendo el líder)
        active_db_nodes = [n for n in current_db_nodes 
                        if n != self.raft.node_id and n in active_nodes]
        
        log_info("MANAGE DB NODES", f"Nodos DB activos (sin líder): {active_db_nodes} (total: {len(active_db_nodes)})", 
                colorize_full=True, color="cyan")
        
        # Si faltan nodos DB
        if len(active_db_nodes) < k - 1 and len(active_nodes) >= k - 1:
            needed = (k - 1) - len(active_db_nodes)
            candidates = [n for n in active_nodes 
                        if n not in current_db_nodes]
            
            if len(current_db_nodes) != len(real_db_nodes):
                dead_db_nodes = [n for n in current_db_nodes if n not in real_db_nodes]
                if dead_db_nodes:
                    for node_id in dead_db_nodes:
                        if node_id in self.raft.global_index["node_versions"]:
                            with self.raft._lock:
                                if node_id in self.raft.global_index["db_nodes"]:
                                    self.raft.global_index["db_nodes"].discard(node_id)
                                self.raft.global_index["node_versions"][node_id]["is_db_node"] = False
                                self.raft.global_index["node_versions"][node_id]["db_version"] = 0
                                self.raft.global_index["node_versions"][node_id]["db_version_prev"] = 0
                            
                            with self.raft_server._lock:
                                self.raft_server.node_states["node_id"] = "DEAD"

                            log_info("MANAGE DB NODES", f"Nodo {node_id} eliminado de DB nodes por estar DEAD", 
                    colorize_full=True, color="cyan")

            
            log_info("MANAGE DB NODES", f"Faltan {needed} nodos DB. Candidatos: {candidates}", 
                    colorize_full=True, color="cyan")
            
            # Seleccionar candidatos de forma consistente (ordenar por ID)
            candidates.sort()
            new_db_nodes = candidates[:needed]
            
            if new_db_nodes:
                log_info("MANAGE DB NODES", f"Nodos seleccionados para promoción: {new_db_nodes}", 
                        colorize_full=True, color="cyan")
            else:
                log_warning("MANAGE DB NODES", "No hay candidatos disponibles para promoción", 
                        colorize_full=True, color="yellow")
            
            for node_id in new_db_nodes:
                log_info("MANAGE DB NODES", f"Promoviendo nodo {node_id} a nodo DB...", 
                        colorize_full=True, color="cyan")
                
                current_db_nodes.add(node_id)
                with self.raft._lock:
                    if node_id not in self.raft.global_index["node_versions"]:
                        self.raft.global_index["node_versions"][node_id] = {
                            "read_version": 0,
                            "write_version": 0,
                            "db_version": 0,
                            "db_version_prev": 0,
                            "is_db_node": True
                        }
                    else:
                        self.raft.global_index["node_versions"][node_id]["is_db_node"] = True
                
                # Sincronizar con el nodo DB que tiene mayor db_version
                try:
                    # Encontrar nodo DB con mayor db_version
                    best_source_node = None
                    max_db_version = -1
                    
                    for db_node in current_db_nodes:
                        if db_node == node_id:  # No comparar consigo mismo
                            continue
                        
                        node_version_info = self.raft.global_index["node_versions"].get(db_node, {})
                        db_version = node_version_info.get("db_version", 0)
                        
                        if db_version > max_db_version:
                            max_db_version = db_version
                            best_source_node = db_node
                    
                    if best_source_node:
                        log_info("MANAGE DB NODES", f"Sincronizando {node_id} desde {best_source_node} (db_version: {max_db_version})", 
                                colorize_full=True, color="cyan")
                        
                        # Obtener JSON del nodo fuente
                        remote_db = RemoteDBManager()
                        
                        try:
                            source_json = remote_db.get_json_dump(best_source_node)
                            term = source_json.get('term', 0) if (getattr(source_json, 'term', False)) else 0
                            
                            if source_json:
                                log_info("MANAGE DB NODES", f"JSON obtenido de {best_source_node}, tamaño del log: {len(source_json.get('log', []))}", 
                                        colorize_full=True, color="cyan")
                                
                                # Enviar JSON al nuevo nodo
                                if node_id == self.raft_server.host:
                                    # Nodo local
                                    log_info("MANAGE DB NODES", f"Restaurando JSON localmente en {node_id}", 
                                            colorize_full=True, color="cyan")
                                    self.raft_server.db_instance.restore_json_from_dump(source_json)
                                    self.raft_server.db_instance.execute_pending_operations_from_json()
                                    
                                    max_db_version = max_db_version if max_db_version >= 0 else 0
                                    self.raft_server.db_instance.update_db_version(max_db_version)
                                    self.raft_server.db_instance.update_term(term)

                                else:
                                    # Nodo remoto - usar RemoteDBManager
                                    log_info("MANAGE DB NODES", f"Restaurando JSON remotamente en {node_id}", 
                                            colorize_full=True, color="cyan")
                                    
                                    # restore_json_from_dump necesita recibir json_data como parámetro
                                    client = remote_db._get_client_server(node_id)
                                    result = client.restore_json_from_dump(source_json)
                                    
                                    if result.get("success"):
                                        log_info("MANAGE DB NODES", f"JSON restaurado en {node_id}, ejecutando operaciones pending...", 
                                                colorize_full=True, color="cyan")
                                        # Ejecutar operaciones pending del JSON
                                        client.execute_pending_operations_from_json()

                                        max_db_version = max_db_version if max_db_version > -1 else 0
                                        client.update_db_version(max_db_version)
                                        client.update_term(term)

                                        log_info("MANAGE DB NODES", f"Operaciones pending ejecutadas en {node_id}", 
                                                colorize_full=True, color="cyan")
                                    else:
                                        # Error - rojo
                                        log_error("MANAGE DB NODES", f"Error restaurando JSON en {node_id}: {result.get('error', 'Unknown')}", 
                                                colorize_full=True, color="red")
                                        raise Exception(f"Failed to restore JSON: {result.get('error')}")
                                
                                # Actualizar versiones del nuevo nodo y marcarlo como DB
                                with self.raft._lock:
                                    self.raft.global_index["db_nodes"].add(node_id)
                                    self.raft.global_index["node_versions"][node_id]["is_db_node"] = True
                                    
                                    max_db_version = max_db_version if max_db_version > -1 else 0
                                    self.raft.global_index["node_versions"][node_id]["db_version"] = max_db_version
                                    self.raft.global_index["node_versions"][node_id]["db_version_prev"] = source_json.get("db_version_prev", 0)
                                

                                
                                log_success("MANAGE DB NODES", f"Nodo {node_id} sincronizado exitosamente con db_version={max_db_version}", 
                                            colorize_full=True, color="cyan")
                            else:
                                # Warning - amarillo
                                log_warning("MANAGE DB NODES", f"JSON vacío o None recibido de {best_source_node}", 
                                        colorize_full=True, color="yellow")
                        
                        except Exception as e:
                            # Error - rojo
                            log_error("MANAGE DB NODES", f"Error obteniendo o restaurando JSON de {best_source_node}: {e}", 
                                    colorize_full=True, color="red")
                            import traceback
                            log_error("MANAGE DB NODES", traceback.format_exc(), 
                                    colorize_full=True, color="red")
                
                except Exception as e:
                    # Error - rojo
                    log_error("MANAGE DB NODES", f"Error sincronizando nuevo nodo DB {node_id}: {e}", 
                            colorize_full=True, color="red")
                    import traceback
                    log_error("MANAGE DB NODES", traceback.format_exc(), 
                            colorize_full=True, color="red")
                
                log_success("MANAGE DB NODES", f"Nodo {node_id} promovido a nodo de base de datos", 
                            colorize_full=True, color="cyan")
        
        # Si sobran nodos DB
        elif len(active_db_nodes) > k - 1:
            excess = len(active_db_nodes) - (k - 1)
            
            log_info("MANAGE DB NODES", f"Sobran {excess} nodos DB", 
                    colorize_full=True, color="cyan")
            
            # Ordenar por db_version (menor primero) y eliminar el exceso
            db_nodes_with_versions = []
            for node_id in active_db_nodes:
                version_info = self.raft.global_index["node_versions"].get(node_id, {})
                db_version = version_info.get("db_version", 0)
                if node_id != self.raft_server.host: #NO INCLUIR AL LIDER NUNCA
                    db_nodes_with_versions.append((node_id, db_version)) 
            
            db_nodes_with_versions.sort(key=lambda x: x[1])  # Ordenar por db_version
            nodes_to_remove = [node_id for node_id, _ in db_nodes_with_versions[:excess]]
            
            log_info("MANAGE DB NODES", f"Nodos a degradar: {nodes_to_remove}", 
                    colorize_full=True, color="cyan")
            
            for node_id in nodes_to_remove:
                current_db_nodes.discard(node_id)
                try:
                    self._demote_db_node(node_id)

                    log_info("MANAGE DB NODES", f"Nodo {node_id} degradado de nodo de base de datos", 
                        colorize_full=True, color="cyan")
                except Exception as e:
                    log_error("MANAGE DB NODES", f"Ha ocurrido un error al degradar el nodo {node_id}: {e}")
        
        else:
            log_success("MANAGE DB NODES", f"Nodos DB correctos: {len(active_db_nodes) + 1} de {k}", 
                        colorize_full=True, color="cyan")
            
            # --- Comprobación de sincronización de nodos DB ---
            try:
                # Info - cyan para verificación de sincronización
                log_info("MANAGE DB NODES", "Verificando sincronización de nodos DB", 
                        colorize_full=True, color="cyan")
                
                # Obtener información de sincronización de todos los nodos DB
                node_sync_info = self._get_db_node_sync_info()
                
                # Verificar si todos tienen la misma db_version y última operación
                if node_sync_info:
                    # Obtener valores del líder
                    leader_id = self.raft.node_id
                    if leader_id in node_sync_info:
                        leader_info = node_sync_info[leader_id]
                        expected_db_version = leader_info["db_version"] if leader_info["db_version"] else 0
                        expected_last_op = leader_info["last_operation"] if leader_info["last_operation"] else {"term": 0, "task_id": 0}
                        
                        term1 = expected_last_op["term"]
                        task1 = expected_last_op["task_id"]
                        log_info("MANAGE DB NODES", f"Líder {leader_id}: db_version={expected_db_version}, last_op_term={term1}, task_id={task1}", 
                                colorize_full=True, color="cyan")
                        
                        # Comparar con cada nodo DB
                        for node_id, node_info in node_sync_info.items():
                            if node_id == leader_id:
                                continue
                            
                            if not "db_version" in node_info.keys():
                                node_info["db_version"] = 0

                            node_db_version = node_info["db_version"]
                            node_last_op = node_info["last_operation"] if node_info["last_operation"] else {"term": 0, "task_id": 0}
                            
                            term2 = node_last_op["term"]
                            task2 = node_last_op["task_id"]
                            log_info("MANAGE DB NODES", f"Nodo {node_id}: db_version={node_db_version}, last_op_term={term2}, task_id={task2}", 
                                    colorize_full=True, color="cyan")
                            
                            # Comparar db_version
                            if (node_db_version != expected_db_version) or (expected_last_op.get("task_id") != node_last_op.get("task_id") or (node_last_op["term"] != expected_last_op["term"])) :
                                
                                # Warning - amarillo
                                log_warning("MANAGE DB NODES", 
                                        f"Nodo {node_id} y líder {leader_id} están desincronizados. \n" +
                                        f"db_version del nodo: {node_db_version} vs db_version del líder: {expected_db_version} \n" +
                                        f"task_id: {task1} vs {task2}", 
                                        colorize_full=True, color="yellow")
                                
                                log_info("MANAGE DB NODES", f"Sincronizando nodo {node_id} con {leader_id}...", 
                                        colorize_full=True, color="cyan")
                                self._sync_db_node(
                                    node_id,
                                    node_info, 
                                    leader_info
                                )
            
            except Exception as e:
                # Error - rojo
                log_error("MANAGE DB NODES", f"Error en comprobación de sincronización: {e}", 
                        colorize_full=True, color="red")
                import traceback
                log_error("MANAGE DB NODES", traceback.format_exc(), 
                        colorize_full=True, color="red")
        
        # Actualizar el índice global
        with self.raft._lock:
            self.raft.global_index["version"] += 1
        
        log_success("MANAGE DB NODES", "Gestión de nodos DB completada exitosamente", 
                    colorize_full=True, color="cyan")
        # logger.info(f"=== FIN GESTION DE NODOS DB ===\n")

    def _get_db_node_sync_info(self) -> dict:
        with self.raft._lock:
            current_db_nodes = self.raft.global_index["db_nodes"].copy()

        from .discovery import discover_active_clients
        nodes = discover_active_clients()
        time.sleep(0.1)
        active_nodes = self._filter_active_nodes(nodes)

        node_sync_info = {}

        for node_id in current_db_nodes:
            try:
                if node_id in self.raft_server.node_states and self.raft_server.node_states[node_id] == "DEAD":
                    continue
                if node_id == self.raft.node_id:
                    # Nodo líder local
                    node_data = self.raft_server.db_instance.json_manager.read()
                    db_version = node_data.get("db_version", 0)
                    term = node_data.get("term", 0)
                    log = node_data.get("log", [])
                    last_op = self.raft_server.db_instance.json_manager.get_last_operation()
                else:
                    # Nodo DB remoto
                    remote_db = RemoteDBManager()
                    node_data = remote_db.get_json_dump(node_id)
                    if node_data:
                        db_version = node_data.get("db_version", 0)
                        log = node_data.get("log", [])
                        term = node_data.get("term", 0)
                        last_op = log[-1] if log else None
                    else:
                        # Warning - amarillo
                        log_warning("MANAGE DB NODES", f"No se pudo obtener JSON del nodo {node_id}", 
                                colorize_full=True, color="yellow")
                        continue
                
                node_sync_info[node_id] = {
                    "db_version": db_version,
                    "last_operation": last_op,
                    "log": log,
                    "term": term,
                    "json_data": node_data
                }
                
            except Exception as e:
                # Error - rojo
                log_error("MANAGE DB NODES", f"Error obteniendo info de sincronización de {node_id}: {e}", 
                        colorize_full=True, color="red")
                raise e
        
        return node_sync_info
    
    # ============================================================================
    # COORDINACIÓN DE LECTURA DE METADATA
    # ============================================================================

    def read_metadata(self, query_data: dict):
        """
        Lee metadatos del nodo DB con menor read_version.
        Solo puede llamarse desde el líder.
        """
        raft = get_raft_instance()

        if raft.state != "leader":
            raise Exception("Solo el líder puede coordinar operaciones")
        
        db_nodes = self._filter_active_nodes(self.raft.global_index.get("db_nodes", {}))
        db_nodes, read_versions = self._get_sorted_db_nodes_by_read(db_nodes)
        
        if not db_nodes:
            raise Exception("No hay nodos de base de datos disponibles")

        # Info - gray_light para lectura de metadata
        log_info("READ METADATA", f"Iniciando lectura de metadata con query: {query_data}", 
                colorize_full=True, color="gray_light")
        
        # Intentar con nodos en orden de menor read_version
        # Preparamos un string con la lista de nodos y sus reads en orden
        node_list_str = ", ".join([f"{node} (read: {read_versions[node]})" for node in db_nodes])
        log_info("READ METADATA", f"Escogiendo nodo DB para lectura de metadata, opciones (ordenadas por menor read): {node_list_str}", 
                colorize_full=True, color="gray_light")
        
        for node_id in db_nodes:
            try:
                log_info("READ METADATA", f"Intentando leer metadata desde nodo DB {node_id}", 
                        colorize_full=True, color="gray_light")
                
                if node_id == self.raft_server.host:
                    log_info("READ METADATA", f"Leyendo metadata localmente desde nodo DB {node_id}", 
                            colorize_full=True, color="gray_light")
                    result = self.raft_server.db_instance.get_data(query_data)
                else:
                    log_info("READ METADATA", f"Leyendo metadata remotamente desde nodo DB {node_id}", 
                            colorize_full=True, color="gray_light")
                    remote_db = RemoteDBManager()
                    result = remote_db.get_data(query_data, node_id)
                
                log_success("READ METADATA", f"Lectura de metadata exitosa desde nodo {node_id}", 
                            colorize_full=True, color="gray_light")
                self.raft.update_node_version("read", node_id)
                return result
            
            except Exception as e:
                # Warning - amarillo
                log_warning("READ METADATA", f"Error leyendo de nodo DB {node_id}: {e}", 
                        colorize_full=True, color="yellow")
                continue
        
        # Error - rojo
        log_error("READ METADATA", "No se pudo leer de ningún nodo DB", 
                colorize_full=True, color="red")
        raise Exception("No se pudo leer de ningún nodo DB")

    # ============================================================================
    # COORDINACIÓN DE ESCRITURA DE METADATA (K-1 PC)
    # ============================================================================
    def manage_metadata(self, metadata_obj, operation: str):
        """
        Gestiona metadatos en k nodos DB con commit en dos fases.
        operation: 'create', 'update', 'delete'
        """        
        if self.raft.state != "leader":
            raise Exception("Solo el líder puede coordinar operaciones")

        task_id = str(uuid.uuid4())
        
        try:
            db_nodes = list(self.raft.global_index["db_nodes"])
            db_nodes = self._filter_active_nodes(db_nodes)
            prepare_results = {}

            # Info - azul claro
            log_info("MANAGE METADATA", f"Nodos DB vivos {db_nodes}", 
                    colorize_full=True, color="light_blue")
            
            # FASE 1: PREPARE
            for node_id in db_nodes:
                try:
                    if node_id == self.raft_server.host:
                        if operation == "create":
                            result = self.raft_server.db_instance.prepare_create(metadata_obj=metadata_obj, task_id=task_id, term=self.raft.current_term)
                        elif operation == "update":
                            result = self.raft_server.db_instance.prepare_update(metadata_obj=metadata_obj, task_id=task_id, term=self.raft.current_term)
                        else:
                            result = self.raft_server.db_instance.prepare_delete(metadata_obj=metadata_obj, task_id=task_id, term=self.raft.current_term)
                    else:
                        remote_db = RemoteDBManager()
                        safe_metadata_dict = self.raft_server.db_instance.serialize_for_transfer(metadata_obj)
                        result = remote_db.prepare_operation(node_id, safe_metadata_dict, operation, task_id, term=self.raft.current_term)
                    
                    prepare_results[node_id] = result
                
                except Exception as e:
                    # Error - rojo
                    log_error("MANAGE METADATA", f"Error en prepare en {node_id}: {e}", 
                            colorize_full=True, color="red")
                    prepare_results[node_id] = {"success": False, "error": str(e)}
            
            # Verificar que todos los nodos DB respondieron OK
            success_nodes = [
                node_id for node_id, result in prepare_results.items() 
                if result.get("success")
            ]

            # Success - azul oscuro
            log_success("MANAGE METADATA", f"Nodos con prepare exitoso: {', '.join(success_nodes)}, Total: {len(success_nodes)}", 
                    colorize_full=True, color="light_blue")
            
            # FASE 2: COMMIT
            commit_success_count, commit_success_nodes = self._commit_metadata_write(success_nodes, task_id)

            if commit_success_count < len(success_nodes):
                # Warning - amarillo
                log_warning("MANAGE METADATA", f"Nodos con commit exitoso: {', '.join(commit_success_nodes)}, Total: {commit_success_count}", 
                        colorize_full=True, color="yellow")
            elif commit_success_count == len(success_nodes):
                # Success - azul oscuro
                log_success("MANAGE METADATA", f"Nodos con commit exitoso: {', '.join(commit_success_nodes)}, Total: {len(commit_success_nodes)}", 
                        colorize_full=True, color="light_blue")

            if not result:
                result = len(success_nodes) == len(prepare_results.items())
                
            return result
        
        except Exception as e:
            # Error - rojo
            log_error("MANAGE METADATA", f"Error: {e}", 
                    colorize_full=True, color="red")
            raise

    def _commit_metadata_write(self, nodes: List[str], task_id: str, max_retries: int = 3) -> tuple:
        """Hace commit en todos los nodos con reintentos"""
        success_count = 0
        success_commit = []
        
        for node_id in nodes:
            committed = False
            
            for attempt in range(max_retries):
                try:
                    if node_id == self.raft_server.host:
                        result = self.raft_server.db_instance.commit_operation(task_id, node_id)
                    else:
                        remote_db = RemoteDBManager()
                        result = remote_db.commit_operation(node_id, task_id)
                    
                    if result.get("success"):
                        success_count += 1
                        success_commit.append(node_id)
                        committed = True
                        with self.raft._lock:
                            db_version = self.raft.global_index["node_versions"][node_id]["db_version"]
                            self.raft.global_index["node_versions"][node_id]["db_version_prev"] = db_version
                            self.raft.global_index["node_versions"][node_id]["db_version"] = db_version + 1
                        break
                
                except Exception as e:
                    # Info - azul claro
                    log_info("MANAGE METADATA", f"Error en commit en {node_id} (intento {attempt + 1}/{max_retries}): {e}", 
                            colorize_full=True, color="light_blue")
                    if attempt < max_retries - 1:
                        time.sleep(1)
            
            if not committed:
                # Info - azul claro
                log_warning("MANAGE METADATA", f"Commit falló definitivamente en {node_id} después de {max_retries} intentos", 
                        colorize_full=True)
        
        return success_count, success_commit

    def _rollback_metadata_write(self, nodes: List[str], task_id: str):
        """Hace rollback en todos los nodos con reintentos"""
        for node_id in nodes:
            rolled_back = False
            
            for attempt in range(3):
                try:
                    if node_id == self.raft_server.host:
                        self.raft_server.db_instance.rollback_operation(task_id)
                    else:
                        remote_db = RemoteDBManager()
                        remote_db.rollback_operation(node_id, task_id)
                    
                    rolled_back = True
                    break
                
                except Exception as e:
                    # Error - rojo
                    log_error("MANAGE METADATA", f"Error en rollback en {node_id} (intento {attempt + 1}/3): {e}", 
                            colorize_full=True, color="red")
                    if attempt < 2:
                        time.sleep(1)
            
            if not rolled_back:
                # Error - rojo
                log_error("MANAGE METADATA", f"Rollback falló definitivamente en {node_id}", 
                        colorize_full=True, color="red")
    
    # ============================================================================
    # COORDINACIÓN DE LECTURA DE ARCHIVOS (DISTRIBUCIÓN DE CARGA)
    # ============================================================================

    def read_file_chunks(self, filename: str, start_chunk: int, chunk_count: int):
        """
        Coordina la lectura de chunks de un archivo.
        Selecciona UN nodo (menor read_version) y le delega toda la operación.
        """
        if self.raft.state != "leader":
            raise Exception("Solo el líder puede coordinar operaciones")
        
        # Info - light_green para lectura de chunks
        log_info("READ CHUNKS", f"Iniciando lectura de {chunk_count} chunks del archivo {filename} desde chunk {start_chunk}", 
                colorize_full=True, color="light_green")
        
        # Verificar que el archivo existe en el índice
        if filename not in self.raft.global_index.get("files_metadata", {}):
            log_info("READ CHUNKS", f"Índice global: {self.raft.global_index}", 
                    colorize_full=True, color="light_green")
            # Error - rojo
            log_error("READ CHUNKS", f"Archivo {filename} no encontrado en el índice global", 
                    colorize_full=True, color="red")
            raise FileNotFoundError(f"Archivo {filename} no encontrado en el índice global")
        
        # Obtener TODOS los nodos disponibles
        all_nodes = list(self.raft.global_index["node_versions"].keys())
        all_nodes = self._filter_active_nodes(all_nodes)
        
        if not all_nodes:
            # Error - rojo
            log_error("READ CHUNKS", "No hay nodos disponibles", 
                    colorize_full=True, color="red")
            raise Exception("No hay nodos disponibles")
        
        # Seleccionar el nodo con menor read_version
        selected_node = self._select_node_by_version(all_nodes, "read")
        
        # Actualizar read_version ANTES de delegar
        self.raft.update_node_version("read", selected_node)
        
        log_info("READ CHUNKS", 
                f"Delegando lectura de {filename} chunks [{start_chunk}:{start_chunk + chunk_count}] al nodo {selected_node}", 
                colorize_full=True, color="light_green")
        
        # Delegar la lectura completa
        if selected_node == self.raft_server.host:
            log_info("READ CHUNKS", f"Ejecutando lectura delegada localmente en nodo {selected_node}", 
                    colorize_full=True, color="light_green")
            chunks = self._read_chunks_delegated_local(filename, start_chunk, chunk_count)
        else:
            log_info("READ CHUNKS", f"Ejecutando lectura delegada remotamente en nodo {selected_node}", 
                    colorize_full=True, color="light_green")
            chunks = self._read_chunks_delegated_remote(selected_node, filename, start_chunk, chunk_count)
        
        log_success("READ CHUNKS", 
                    f"Lectura completada: {len(chunks)} chunks obtenidos del archivo {filename} desde nodo {selected_node}", 
                    colorize_full=True, color="light_green")
        return chunks

    def _read_chunks_delegated_local(self, filename: str, start_chunk: int, chunk_count: int) -> list:
        """
        Lectura delegada cuando el nodo seleccionado es el líder.
        """
        end_chunk = (start_chunk + chunk_count - 1) if (start_chunk != 0 and chunk_count != 1) else 1
        log_info("READ DELEGATED", 
                f"Ejecutando lectura delegada local para archivo {filename}, chunks {start_chunk}-{end_chunk}", 
                colorize_full=True, color="light_green")
        
        try:
            result = self.raft_server.storage_instance.read_chunks_delegated(
                filename,
                start_chunk,
                chunk_count,
                self.raft.global_index
            )
            
            log_success("READ DELEGATED", 
                        f"Lectura delegada local completada: {len(result)} chunks obtenidos", 
                        colorize_full=True, color="light_green")
            return result
            
        except Exception as e:
            log_error("READ DELEGATED", 
                    f"Error en lectura delegada local: {type(e).__name__}: {e}", 
                    colorize_full=True, color="red")
            raise

    def _read_chunks_delegated_remote(
        self, node_id: str, filename: str, start_chunk: int, chunk_count: int
    ) -> list:
        """
        Delega la lectura completa a un nodo remoto con reintentos.
        """
        # Info - light_green para lectura delegada remota
        log_info("READ DELEGATED", 
                f"Iniciando lectura delegada remota en nodo {node_id} para archivo {filename}, chunks {start_chunk}-{start_chunk + chunk_count - 1}", 
                colorize_full=True, color="light_green")
        
        for attempt in range(3):
            try:
                log_info("READ DELEGATED", 
                        f"Intento {attempt + 1}/3: delegando lectura a nodo {node_id}", 
                        colorize_full=True, color="light_green")
                
                remote_storage = RemoteStorageManager()
                chunks = remote_storage.read_chunks_delegated(
                    filename,
                    start_chunk,
                    chunk_count,
                    node_id,
                    self.raft.global_index
                )
                
                log_success("READ DELEGATED", 
                            f"Intento {attempt + 1}/3 exitoso: {len(chunks)} chunks obtenidos de nodo {node_id}", 
                            colorize_full=True, color="light_green")
                return chunks
            
            except Exception as e:
                # Warning - amarillo para intentos fallidos
                log_warning("READ DELEGATED", 
                        f"Error delegando lectura a {node_id} (intento {attempt + 1}/3): {e}", 
                        colorize_full=True, color="yellow")
                if attempt < 2:
                    log_info("READ DELEGATED", f"Reintentando en 1 segundo...", 
                            colorize_full=True, color="light_green")
                    time.sleep(1)
        
        # Error - rojo para fallo completo
        log_error("READ DELEGATED", 
                f"Error delegando lectura a nodo {node_id} después de 3 intentos", 
                colorize_full=True, color="red")
        raise Exception(f"Error delegando lectura a nodo {node_id} después de 3 intentos")

    # ============================================================================
    # COORDINACIÓN DE ESCRITURA DE ARCHIVOS
    # ============================================================================

    def manage_file(self, filename: str, file_data: bytes = None, operation: str = "create", real_name: str = None):
        """
        Gestiona archivos: crea o elimina según la operación.
        operation: 'create' o 'delete'
        """
        if self.raft.state != "leader":
            raise Exception("Solo el líder puede coordinar operaciones")
        
        # Info - verde para manage_file
        log_info("MANAGE FILE", f"Iniciando operación '{operation}' para archivo de track '{real_name}' con nombre combinación de ID + Username: {filename}", 
                colorize_full=True, color="green")
        
        if operation == "delete":
            return self._delete_file(filename)
        elif operation == "create":
            return self._create_file(filename, file_data)
        else:
            # Error - rojo
            log_error("MANAGE FILE", f"Operación desconocida: {operation}", 
                    colorize_full=True, color="red")
            raise ValueError(f"Operación desconocida: {operation}")

    def _create_file(self, filename: str, file_data: bytes):
        """
        Escribe un archivo dividiéndolo en p rangos de chunks,
        replicado en k nodos por cada rango.
        """
        from backend.settings import CHUNK_SIZE, CHUNK_RANGES

        # Dividir archivo en chunks
        total_size = len(file_data)
        total_chunks = ceil(total_size / CHUNK_SIZE)
        
        # Dividir en p rangos
        chunk_ranges = self._divide_into_ranges(0, total_chunks, CHUNK_RANGES)
        
        # Para cada rango, seleccionar k nodos con menor write_version
        distribution = {}
        write_tasks = []
        already_selected = {}

        # Info - verde claro para _create_file
        log_info("MANAGE FILE", f"=== INICIO _create_file {filename} ===", 
                colorize_full=True, color="light_green")
        log_info("MANAGE FILE", f"Nodos en global_index: {list(self.raft.global_index['node_versions'].keys())}", 
                colorize_full=True, color="light_green")

        versiones_iniciales = {node: info.get('write_version', 0) for node, info in self.raft.global_index['node_versions'].items()}
        log_info("MANAGE FILE", f"Versiones iniciales write: {str(versiones_iniciales)}", 
                colorize_full=True, color="light_green")

        try:
            for i, chunk_range in enumerate(chunk_ranges):
                all_nodes = list(self.raft.global_index["node_versions"].keys())
                all_nodes = self._filter_active_nodes(all_nodes)

                available_nodes = all_nodes
                
                log_info("MANAGE FILE", f"--- Rango {i} ({chunk_range[0]}-{chunk_range[1]}) ---", 
                        colorize_full=True, color="light_green")
                log_info("MANAGE FILE", f"Nodos disponibles: {available_nodes}", 
                        colorize_full=True, color="light_green")
                log_info("MANAGE FILE", f"Ya seleccionados en esta operación: {str(already_selected)}", 
                        colorize_full=True, color="light_green")
                
                selected_nodes = self._select_k_nodes_by_version(
                    available_nodes,
                    "write",
                    self.raft.db_replication_factor,
                    already_selected=already_selected
                )
                
                log_info("MANAGE FILE", f"Nodos seleccionados para este rango: {selected_nodes}", 
                        colorize_full=True, color="light_green")
                
                for node_id in selected_nodes:
                    already_selected[node_id] = already_selected.get(node_id, 0) + 1
                
                range_key = f"{chunk_range[0]}-{chunk_range[1]}"
                distribution[range_key] = selected_nodes
                
                start_byte = chunk_range[0] * CHUNK_SIZE
                end_byte = min(chunk_range[1] * CHUNK_SIZE, total_size)
                range_data = file_data[start_byte:end_byte]
                
                for node_id in selected_nodes:
                    write_tasks.append({
                        "node_id": node_id,
                        "filename": filename,
                        "range_key": range_key,
                        "data": range_data
                    })

            log_info("MANAGE FILE", f"Distribución final: {str(distribution)}", 
                    colorize_full=True, color="light_green")
            log_info("MANAGE FILE", f"Contador de selecciones: {str(already_selected)}", 
                    colorize_full=True, color="light_green")
            
            self._execute_write_tasks_parallel(write_tasks)
            self._update_file_index(filename, total_chunks, distribution)
            
            return {
                "success": True,
                "distribution": distribution,
                "total_chunks": total_chunks
            }
        except Exception as e:
            log_warning("MANAGE FILE", f"Ha ocurrido un error al intentar crear el archivo: {e}")
            return {
                "success": False,
                "distribution": {},
                "total_chunks": 0
            }

    def _delete_file(self, filename: str):
        """
        Elimina un archivo distribuido de todos los nodos que lo contienen.
        """
        # Info - verde oscuro para _delete_file
        log_info("MANAGE FILE", f"=== INICIO _delete_file: {filename} ===", 
                colorize_full=True, color="dark_green")
        
        # Obtener información del archivo del índice global
        if "files_metadata" not in self.raft.global_index:
            # Error - rojo
            log_error("MANAGE FILE", f"Archivo {filename} no encontrado en índice", 
                    colorize_full=True, color="red")
            raise FileNotFoundError(f"Archivo {filename} no encontrado en índice")
        
        file_metadata = self.raft.global_index["files_metadata"].get(filename)
        if not file_metadata:
            # Error - rojo
            log_error("MANAGE FILE", f"Archivo {filename} no encontrado en índice", 
                    colorize_full=True, color="red")
            raise FileNotFoundError(f"Archivo {filename} no encontrado en índice")
        
        chunk_distribution = file_metadata.get("chunk_distribution", {})
        
        # Crear tareas de eliminación
        delete_tasks = []
        nodes_with_file = set()
        
        for range_key, nodes in chunk_distribution.items():
            for node_id in nodes:
                nodes_with_file.add(node_id)
                delete_tasks.append({
                    "node_id": node_id,
                    "filename": filename,
                    "range_key": range_key
                })
        nodes_with_file = self._filter_active_nodes(nodes_with_file)
        log_info("MANAGE FILE", f"Nodos vivos con archivo: {nodes_with_file}", 
                colorize_full=True, color="dark_green")
        log_info("MANAGE FILE", f"Rangos a eliminar: {list(chunk_distribution.keys())}", 
                colorize_full=True, color="dark_green")
        
        # Ejecutar eliminaciones en paralelo
        self._execute_delete_tasks_parallel(delete_tasks)
        
        # Actualizar índice global
        self._remove_file_from_index(filename, chunk_distribution)
        
        log_info("MANAGE FILE", f"=== FIN _delete_file: {filename} ===\n", 
                colorize_full=True, color="dark_green")
        
        return {
            "success": True,
            "deleted_from_nodes": list(nodes_with_file),
            "ranges_deleted": list(chunk_distribution.keys())
        }

    def _execute_delete_tasks_parallel(self, tasks: List[dict]):
        """Ejecuta tareas de eliminación en paralelo"""
        threads = []
        errors = []
        
        def delete_task(task):
            try:
                node_id = task["node_id"]
                filename = task["filename"]
                range_key = task["range_key"]
                
                if node_id == self.raft_server.host:
                    self.raft_server.storage_instance.delete_file_range(filename, range_key)
                else:
                    remote_storage = RemoteStorageManager()
                    remote_storage.delete_file_range(filename, range_key, node_id)
                
                # Info - verde oscuro
                log_info("MANAGE FILE", f"Rango {range_key} eliminado de nodo {node_id}", 
                        colorize_full=True, color="dark_green")
            
            except Exception as e:
                errors.append({"task": task, "error": str(e)})
                # Error - rojo
                log_error("MANAGE FILE", f"Error eliminando de {node_id}: {e}", 
                        colorize_full=True, color="red")
        
        for task in tasks:
            t = threading.Thread(target=delete_task, args=(task,))
            t.start()
            threads.append(t)
        
        from backend.settings import RPC_TIMEOUT
        for t in threads:
            t.join(timeout=RPC_TIMEOUT * 2)
        
        if errors:
            # Warning - amarillo
            log_warning("MANAGE FILE", f"Errores en {len(errors)} tareas de eliminación", 
                    colorize_full=True, color="yellow")

    def _remove_file_from_index(self, filename: str, chunk_distribution: dict):
        """Elimina un archivo del índice global"""
        with self.raft._lock:
            # Eliminar de files_metadata
            if filename in self.raft.global_index.get("files_metadata", {}):
                del self.raft.global_index["files_metadata"][filename]
            
            # Eliminar de files por nodo
            for range_key, nodes in chunk_distribution.items():
                range_start, range_end = map(int, range_key.split("-"))
                chunks_in_range = range_end - range_start
                
                for node_id in nodes:
                    # Eliminar de files
                    if node_id in self.raft.global_index.get("files", {}):
                        if filename in self.raft.global_index["files"][node_id]:
                            self.raft.global_index["files"][node_id].remove(filename)
                    
                    # Actualizar node_shards
                    if node_id in self.raft.global_index.get("node_shards", {}):
                        if filename in self.raft.global_index["node_shards"][node_id].get("shards", {}):
                            # Eliminar el range_key específico
                            if range_key in self.raft.global_index["node_shards"][node_id]["shards"][filename]:
                                self.raft.global_index["node_shards"][node_id]["shards"][filename].remove(range_key)
                            
                            # Si no quedan rangos, eliminar el archivo completo
                            if not self.raft.global_index["node_shards"][node_id]["shards"][filename]:
                                del self.raft.global_index["node_shards"][node_id]["shards"][filename]
                            
                            # Actualizar contador de chunks
                            self.raft.global_index["node_shards"][node_id]["total_chunks"] -= chunks_in_range
            
            self.raft.global_index["version"] += 1
        
        # Info - lilac para actualizar índice
        log_info("MANAGE FILE", f"Índice actualizado - archivo {filename} eliminado (v{self.raft.global_index['version']})", 
                colorize_full=True, color="lilac")

    def _execute_write_tasks_parallel(self, tasks: List[dict]):
        """Ejecuta tareas de escritura en paralelo"""
        threads = []
        errors = []
        
        def write_task(task):
            try:
                node_id = task["node_id"]
                filename = task["filename"]
                range_key = task["range_key"]
                data = task["data"]

                if isinstance(data, dict):
                    # Warning - amarillo
                    log_warning("MANAGE FILE", f"Error: data es un dict, no bytes.", 
                            colorize_full=True, color="yellow")
                
                if node_id == self.raft_server.host:
                    self.raft_server.storage_instance.create_file_range(filename, data, range_key)
                else:
                    remote_storage = RemoteStorageManager()
                    remote_storage.create_file_range(filename, data, range_key, node_id)
                
                self.raft.update_node_version("write", node_id)
                # Info - verde claro
                log_info("MANAGE FILE", f"Rango {range_key} escrito en nodo {node_id}", 
                        colorize_full=True, color="light_green")
            
            except Exception as e:
                errors.append({"task": task, "error": str(e)})
                # Error - rojo
                log_error("MANAGE FILE", f"Error escribiendo en {node_id}: {e}", 
                        colorize_full=True, color="red")
                raise e
        
        for task in tasks:
            t = threading.Thread(target=write_task, args=(task,))
            t.start()
            threads.append(t)
        
        from backend.settings import RPC_TIMEOUT
        for t in threads:
            t.join(timeout=RPC_TIMEOUT * 2)
        
        if errors:
            # Warning - amarillo
            log_warning("MANAGE FILE", f"Errores en escritura en {len(tasks)} tareas: Intentando continuar...", 
                    colorize_full=True, color="yellow")

    def _update_file_index(self, filename: str, total_chunks: int, distribution: dict):
        """Actualiza el índice global con información del archivo"""
        with self.raft._lock:
            if "files_metadata" not in self.raft.global_index:
                self.raft.global_index["files_metadata"] = {}
            
            self.raft.global_index["files_metadata"][filename] = {
                "total_chunks": total_chunks,
                "chunk_distribution": distribution,
                "created_at": time.time()
            }
            
            # Actualizar node_shards
            if "node_shards" not in self.raft.global_index:
                self.raft.global_index["node_shards"] = {}
            
            # Actualizar lista de archivos por nodo y conteo de shards
            for range_key, nodes in distribution.items():
                range_start, range_end = map(int, range_key.split("-"))
                chunks_in_range = range_end - range_start
                
                for node_id in nodes:
                    # Actualizar files
                    if node_id not in self.raft.global_index["files"]:
                        self.raft.global_index["files"][node_id] = []
                    
                    if filename not in self.raft.global_index["files"][node_id]:
                        self.raft.global_index["files"][node_id].append(filename)
                    
                    # Actualizar node_shards
                    if node_id not in self.raft.global_index["node_shards"]:
                        self.raft.global_index["node_shards"][node_id] = {
                            "total_chunks": 0,
                            "shards": {}
                        }
                    
                    if filename not in self.raft.global_index["node_shards"][node_id]["shards"]:
                        self.raft.global_index["node_shards"][node_id]["shards"][filename] = []
                    
                    if range_key not in self.raft.global_index["node_shards"][node_id]["shards"][filename]:
                        self.raft.global_index["node_shards"][node_id]["shards"][filename].append(range_key)
                        self.raft.global_index["node_shards"][node_id]["total_chunks"] += chunks_in_range
            
            self.raft.global_index["version"] += 1
            
        # Info - lilac para actualizar índice
        log_info("MANAGE FILE", f"Índice actualizado para {filename} (v{self.raft.global_index['version']})", 
                colorize_full=True, color="lilac")


    # ============================================================================
    # HELPERS VARIOS
    # ============================================================================

    def _get_sorted_db_nodes_by_read(self, db_nodes: dict) -> (List[str], Dict[str, int]): # type: ignore
        """Retorna nodos DB ordenados por read_version (menor primero) y un diccionario con los read versions"""
        from raft.log_utils import log_error
        with self.raft._lock:
            node_versions = self.raft.global_index["node_versions"].copy()
        
        try:
            # Crear diccionario de read versions para cada nodo
            read_versions = {n: node_versions.get(n, {}).get("read_version", 0) for n in db_nodes}
            
            # Ordenar por read_version y luego por nombre del nodo
            sorted_nodes = sorted(
                db_nodes,
                key=lambda n: (
                    read_versions[n],
                    n
                )
            )
            
            return sorted_nodes, read_versions
        except Exception as e:
            log_error("READ METADATA", f"Ocurrio un error al ordenar por menor read version: {e}")

    def _select_node_by_version(self, nodes: List[str], version_type: str) -> str:
        """Selecciona el nodo con menor versión (read o write)"""
        with self.raft._lock:
            node_versions = self.raft.global_index["node_versions"].copy()
        
        return min(
            nodes,
            key=lambda n: (
                node_versions.get(n, {}).get(f"{version_type}_version", 0),
                n
            )
        )

    def _select_k_nodes_by_version(self, nodes: List[str], version_type: str, k: int, already_selected: dict = None) -> List[str]:
        """Selecciona los k nodos con menor versión ajustada"""
        import logging
        logger = logging.getLogger("LeaderManager")
        
        with self.raft._lock:
            node_versions = self.raft.global_index["node_versions"]
        
        if already_selected is None:
            already_selected = {}
        
        # Crear una copia de los contadores de versión ajustados
        adjusted_versions = {}
        version_details = []
        
        for node in nodes:
            base_version = node_versions.get(node, {}).get(f"{version_type}_version", 0)
            times_selected = already_selected.get(node, 0)
            adjusted_version = base_version + times_selected
            
            adjusted_versions[node] = adjusted_version
            version_details.append({
                "node": node,
                "base_version": base_version,
                "times_selected": times_selected,
                "adjusted_version": adjusted_version
            })
        
        # Log detallado
        logger.info(f"  _select_k_nodes_by_version - Detalles:")
        for detail in sorted(version_details, key=lambda x: x["adjusted_version"]):
            logger.info(f"    Node: {detail['node']:15s} | Base: {detail['base_version']:3d} | "
                    f"Times selected: {detail['times_selected']:2d} | "
                    f"Adjusted: {detail['adjusted_version']:3d}")
        
        # Ordenar por versión ajustada y luego por nombre del nodo
        sorted_nodes = sorted(
            nodes,
            key=lambda n: (
                adjusted_versions.get(n, 0),
                n  # Para desempatar consistentemente
            )
        )
        
        logger.info(f"  Nodos ordenados por versión ajustada: {sorted_nodes}")
        
        # Seleccionar los primeros k nodos
        selected = sorted_nodes[:k]
        logger.info(f"  Seleccionados (k={k}): {selected}")
        
        # Log adicional: mostrar el diccionario already_selected como string
        logger.info(f"  Already_selected actual: {str(already_selected)}")
        
        return selected

    def _divide_into_ranges(self, start: int, end: int, num_ranges: int) -> List[tuple]:
        """Divide un rango [start, end) en num_ranges sub-rangos"""
        total = end - start
        base_size = total // num_ranges
        remainder = total % num_ranges
        
        ranges = []
        current = start
        
        for i in range(num_ranges):
            range_size = base_size + (1 if i < remainder else 0)
            ranges.append((current, current + range_size))
            current += range_size
        
        return ranges
    
    # def _filter_alive_nodes(self, nodes: List[str]) -> List[str]:
    #     """Filtra solo los nodos que están actualmente vivos"""  
    #     from .discovery import discover_active_clients
    #     return [node for node in nodes if node in set(discover_active_clients())]

    def _serialize_metadata(self, metadata_obj):
        """Serializa un objeto de metadatos"""
        return {
            "model": metadata_obj.__class__.__name__,
            "id": getattr(metadata_obj, 'id', None),
            "fields": {
                field.name: getattr(metadata_obj, field.name)
                for field in metadata_obj._meta.fields
            }
        }

    def _get_client_server(self, node_id: str):
        from .discovery import get_service_tasks
        from raft.log_utils import log_warning

        nodes = get_service_tasks()

        try:
            for node in nodes:
                if node_id == node.ip:
                    return self.raft_server._get_client_server(
                        node_id, node.ip, self.raft_server.port, "node", requires_validation=False
                    )
        except Exception as e:
            log_warning("GET CLIENT SERVER", f"Error al buscar DB remota para {node_id}: {e}")

    def _get_file_info_from_index(self, audio_id: str) -> int:
        with self.raft._lock:
            try:
                return self.raft.global_index["files_metadata"][audio_id]["total_chunks"]
            except Exception as e:
                logger.error(f'Error obteniendo total chunks: {e}')


class RemoteStorageManager:
    def __init__(self):
        self.raft_server: RaftServer = get_raft_server()
        self.storage_manager: StorageManager = self.raft_server.storage_instance

    def list_files(self, node_id: str):
        if node_id == self.raft_server.node_id:
            return self.storage_manager.list_files()

        client = self._get_client_server(node_id)
        if client is None:
            logger.error(f"No se pudo obtener cliente para nodo {node_id}. ")
        return client.list_files()

    def exists(self, filename: str, node_id: str) -> bool:
        if node_id == self.raft_server.node_id:
            return self.storage_manager.exists(filename)

        client = self._get_client_server(node_id)
        return client.exists(filename)

    def create_file(self, filename: str, data: bytes, node_id: str):
        if node_id == self.raft_server.node_id:
            return self.storage_manager.create_file(filename)

        client = self._get_client_server(node_id)
        return client.create_file(filename)

    def delete_file(self, filename: str, node_id: str) -> bool:
        if node_id == self.raft_server.node_id:
            return self.storage_manager.delete_file(filename)

        client = self._get_client_server(node_id)
        return client.delete_file(filename)

    def get_file_info(self, filename: str, node_id: str) -> dict:
        if node_id == self.raft_server.node_id:
            return self.storage_manager.get_file_info(filename)

        client = self._get_client_server(node_id)
        return client.get_file_info(filename)

    def get_chunk(self, filename: str, chunk_index: int, node_id: str) -> bytes:
        if node_id == self.raft_server.node_id:
            return self.storage_manager.get_chunk(filename, chunk_index)

        client = self._get_client_server(node_id)
        return client.get_chunk(filename, chunk_index)
    
    def get_chunk_range(self, filename: str, range_id: str, node_id: str) -> bytes:
        if node_id == self.raft_server.node_id:
            return self.storage_manager.get_chunk_range(filename, range_id)

        client = self._get_client_server(node_id)
        return client.get_chunk_range(filename, range_id)
    
    def create_file_range(self, filename: str, data: bytes, range_id: str, node_id: str):
        if node_id == self.raft_server.node_id:
            return self.storage_manager.create_file_range(filename, data, range_id)

        client = self._get_client_server(node_id)
        return client.create_file_range(filename, data, range_id)
    
    def read_chunks_delegated(self, filename: str, start_chunk: int, chunk_count: int, 
                          node_id: str, global_index: dict) -> list:
        if node_id == self.raft_server.node_id:
            return self.storage_manager.read_chunks_delegated(
                filename, start_chunk, chunk_count, global_index
            )

        client = self._get_client_server(node_id)
        return client.read_chunks_delegated(filename, start_chunk, chunk_count, global_index)
    
    def get_file_ranges(self, filename: str, node_id: str) -> list:
        if node_id == self.raft_server.node_id:
            return self.storage_manager.get_file_ranges(filename)

        client = self._get_client_server(node_id)
        return client.get_file_ranges(filename)

    def delete_file_range(self, filename: str, range_id: str, node_id: str) -> bool:
        if node_id == self.raft_server.node_id:
            return self.storage_manager.delete_file_range(filename, range_id)

        client = self._get_client_server(node_id)
        return client.delete_file_range(filename, range_id)
    
    def _get_client_server(self, node_id: str):
        from .discovery import get_service_tasks

        nodes = get_service_tasks()

        for node in nodes:
            # logger.info(f"Buscando almacenamiento remoto en nodo {node_id}, comparando con {node.ip}")
            if node_id == node.ip:
                start = time.time()
                retries = 0
                while(True):
                    # logger.info(f"Nodo de almacenamiento remoto encontrado: {node_id} ({node.ip}) Retrie: {retries}")

                    client = self.raft_server._get_client_server(
                        node_id, node.ip, self.raft_server.port, "storage"
                    )

                    if client is not None:
                        return client
                    retries += 1
                    now = time.time()
                    if abs(start - now) > 0.8:
                        break 
                    if retries > 100:
                        time.sleep(0.2)

        raise Exception("Error al buscar almacenamiento remoto")

class RemoteDBManager:
    """
    Manager remoto para metadatos usando RAFT.
    Similar a RemoteStorageManager pero para DBManager (SQLite).
    """

    def __init__(self):
        self.raft_server = get_raft_server()
        self.db_manager = self.raft_server.db_instance  # recordar registrarlo como mismo Store Manager

    # GET
    def get_data(self, query_data: dict, node_id: str):
        if node_id == self.raft_server.node_id:
            return self.db_manager.get_data(query_data)

        client = self._get_client_server(node_id)
        return client.get_data(query_data)

    # EXISTS
    def exists(self, metadata, node_id: str) -> bool:
        if node_id == self.raft_server.node_id:
            return self.db_manager.exists(metadata)

        client = self._get_client_server(node_id)
        return client.exists(metadata)

    # CREATE / UPDATE
    def create_data(self, metadata, node_id: str):
        if node_id == self.raft_server.node_id:
            return self.db_manager.create_data(metadata)

        client = self._get_client_server(node_id)
        return client.create_data(metadata)

    # DELETE
    def delete_data(self, metadata, node_id: str) -> bool:
        if node_id == self.raft_server.node_id:
            return self.db_manager.delete_data(metadata)

        client = self._get_client_server(node_id)
        return client.delete_data(metadata)
    
    def get_full_dump(self, node_id: str):
        if node_id == self.raft_server.node_id:
            return self.db_manager.get_full_dump()

        client = self._get_client_server(node_id)
        return client.get_full_dump()

    def restore_from_dump(self, db_dump: dict, node_id: str):
        if node_id == self.raft_server.node_id:
            return self.db_manager.restore_from_dump(db_dump)

        client = self._get_client_server(node_id)
        return client.restore_from_dump(db_dump)
    

    def commit_operation(self, node_id: str, task_id: str):
        # LOCAL
        if node_id == self.raft_server.node_id:
            result = self.db_manager.commit_operation(task_id, node_id)
            return result

        # REMOTO
        client = self._get_client_server(node_id)

        if client is None:
            return {"success": False, "error": "Proxy None"}

        try:
            result = client.commit_operation(task_id)
            return result

        except Exception as e:
            return {"success": False, "error": str(e)}


    def rollback_operation(self, node_id: str, task_id: str):
        if node_id == self.raft_server.node_id:
            return self.db_manager.rollback_operation(task_id)

        client = self._get_client_server(node_id)
        return client.rollback_operation(task_id)

    def get_json_dump(self, node_id: str):
        if node_id == self.raft_server.node_id:
            return self.db_manager.get_json_dump()

        client = self._get_client_server(node_id)
        return client.get_json_dump()
    
    def restore_json_from_dump(self, json_data: dict, node_id: str):
        """Restaura el JSON en un nodo (local o remoto)"""
        if node_id == self.raft_server.node_id:
            return self.db_manager.restore_json_from_dump(json_data)

        client = self._get_client_server(node_id)
        return client.restore_json_from_dump(json_data)
    
    def execute_single_operation(self, operation_dict: dict, node_id: str):
        if node_id == self.raft_server.node_id:
            return self.db_manager.execute_single_operation(operation_dict)

        client = self._get_client_server(node_id)
        return client.execute_single_operation(operation_dict)
    
    def prepare_operation(self, node_id, metadata_dict, operation, task_id, term):
        model_name = metadata_dict["model"]
        data = metadata_dict["data"]

        if node_id == self.raft_server.node_id:
            if operation == "create":
                return self.db_manager.prepare_create(data=data, task_id=task_id, model_name=model_name, term=term)
            elif operation == "update":
                return self.db_manager.prepare_update(data=data, task_id=task_id, model_name=model_name, term=term)
            else:
                return self.db_manager.prepare_delete(data=data, task_id=task_id, model_name=model_name, term=term)

        client = self._get_client_server(node_id)
        if operation == "create":
            return client.prepare_create(data=data, task_id=task_id, model_name=model_name, term=term)
        elif operation == "update":
            return client.prepare_update(data=data, task_id=task_id, model_name=model_name, term=term)
        else:
            return client.prepare_delete(data=data, task_id=task_id, model_name=model_name, term=term)

    def update_db_version(self, node_id: str, db_version: int):
        if node_id == self.raft_server.node_id:
            return self.db_manager.update_db_version(db_version)

        client = self._get_client_server(node_id)
        return client.update_db_version(db_version)
    
    def update_term(self, node_id: str, term: int):
        if node_id == self.raft_server.node_id:
            return self.db_manager.update_term(term)

        client = self._get_client_server(node_id)
        return client.update_term(term)
    
    # def execute_pending_operation_from_this_json(self, node_id: str, node_json: dict):
    #     if node_id == self.raft_server.node_id:
    #         return self.db_manager.execute_pending_operation_from_this_json(node_json)

    #     client = self._get_client_server(node_id)
    #     return client.db_manager.execute_pending_operation_from_this_json(node_json)

    # INTERNAL
    def _get_client_server(self, node_id: str):
        from .discovery import get_service_tasks

        nodes = get_service_tasks()

        for node in nodes:
            if node_id == node.ip:
                return self.raft_server._get_client_server(
                    node_id, node.ip, self.raft_server.port, "db"
                )

        raise Exception("Error al buscar DB remota")
    