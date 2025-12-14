#leader_manager.py
import threading
import logging
import traceback
import time
from typing import List, Dict, Set
from math import ceil
from Pyro5 import api as rpc
from raft.raft import RaftServer
from raft.storage_manager import StorageManager

from .utils import get_raft_server, get_raft_instance, next_term


class Colors:
    GREEN = "\033[92m"
    BLUE = "\033[94m"
    ORANGE = "\033[93m"
    RESET = "\033[0m"
    BOLD = "\033[1m"

# Logging básico
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("LeaderManager")

NODE_CHECK_INTERVAL = 2  # segundos

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
                
                logger.info(f"{Colors.BOLD}{Colors.ORANGE} Sending heartbeat on term {current_term} {Colors.RESET}")
                
                # Incrementar contador de ciclos
                with self._monitoring_count_lock:
                    self._monitoring_cycle_count += 1
                
                # Solo hacer log detallado cada 5 ciclos
                if self._monitoring_cycle_count % 1 == 0:
                    # Formatear los hashes para mostrar solo primeros 3 dígitos
                    formatted_nodes = []
                    for node_hash, ip, port in current_nodes:
                        hash_str = str(node_hash)
                        if len(hash_str) > 3:
                            formatted_hash = hash_str[:3] + ".."
                        else:
                            formatted_hash = hash_str
                        formatted_nodes.append((formatted_hash, ip, port))

                    # Calcular hash del nodo actual (igual que con los otros)
                    current_hash = hash(self.host)
                    current_hash_str = str(current_hash)
                    current_formatted_hash = (
                        current_hash_str[:3] + ".." if len(current_hash_str) > 3 else current_hash_str
                    )

                    current_node_info = (current_formatted_hash, self.host, self.port)

                    # Log final
                    logger.info(
                        f"Term: {current_term}. "
                        f"Nodo actual: {current_node_info}. "
                        f"Nodos activos: {formatted_nodes}"
                    )
                
                self._update_index()

                self._detect_node_state_changes()

                self._process_node_states()

                self._manage_db_nodes()

                # TODO: FUTURA IMPLEMENTACION DE SSE
                self._process_pending_tasks()

            except Exception as e:
                logger.error(f"Error en _monitoring_loop: {e}")
                logger.error(traceback.format_exc())


            # Esperar antes de la siguiente iteración
            self._stop_event.wait(self.poll_interval)

        logger.info("Monitoring loop finalizado.")

    def _get_client_nodes(self):
        """Obtiene la lista de nodos clientes activos (IPs)"""
        from .discovery import discover_active_clients
        client_ips = [ip for ip in discover_active_clients() if ip != self.host]
        clients = [(hash(ip), ip, self.port) for ip in client_ips]

        return clients

    def _manage_db_nodes(self):
        """
        Gestiona los nodos de base de datos para mantener exactamente k nodos
        (líder + k-1 adicionales).
        """
        if self.raft.state != "leader":
            return
        
        k = self.raft.db_replication_factor
        with self.raft._lock:
            current_db_nodes = self.raft.global_index["db_nodes"].copy()
        active_nodes = [node_id for node_id, _, _ in self.raft_server._get_client_nodes() 
                        if self.raft_server._is_node_active(node_id)]
        
        # logger.info(f"\n=== GESTION DE NODOS DB ===")
        # logger.info(f"k requerido: {k}")
        # logger.info(f"Nodos DB actuales: {current_db_nodes}")
        # logger.info(f"Nodos activos totales: {active_nodes}")
        
        # Asegurar que el líder esté en db_nodes
        if self.raft.node_id not in current_db_nodes:
            with self.raft._lock:
                current_db_nodes.add(self.raft.node_id)
                logger.info(f"Líder {self.raft.node_id} agregado a db_nodes")
        
        # Contar nodos DB activos (excluyendo el líder)
        active_db_nodes = [n for n in current_db_nodes 
                        if n != self.raft.node_id and n in active_nodes]
        
        # logger.info(f"Nodos DB activos (sin líder): {active_db_nodes} (total: {len(active_db_nodes)})")
        
        # Si faltan nodos DB
        if len(active_db_nodes) < k - 1 and len(active_nodes) >= k - 1:
            needed = (k - 1) - len(active_db_nodes)
            candidates = [n for n in active_nodes 
                        if n not in current_db_nodes]
            
            logger.info(f"Faltan {needed} nodos DB. Candidatos: {candidates}")
            
            # Seleccionar candidatos de forma consistente (ordenar por ID)
            candidates.sort()
            new_db_nodes = candidates[:needed]
            
            logger.info(f"Nodos seleccionados para promoción: {new_db_nodes}")
            
            for node_id in new_db_nodes:
                logger.info(f"Promoviendo nodo {node_id} a nodo DB...")
                
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
                        logger.info(f"Sincronizando {node_id} desde {best_source_node} (db_version: {max_db_version})")
                        
                        # Obtener JSON del nodo fuente
                        remote_db = RemoteDBManager()
                        
                        try:
                            source_json = remote_db.get_json_dump(best_source_node)
                            
                            if source_json:
                                logger.info(f"JSON obtenido de {best_source_node}, tamaño del log: {len(source_json.get('log', []))}")
                                
                                # Enviar JSON al nuevo nodo
                                if node_id == self.raft_server.host:
                                    # Nodo local
                                    logger.info(f"Restaurando JSON localmente en {node_id}")
                                    self.raft_server.db_instance.restore_json_from_dump(source_json)
                                    self.raft_server.db_instance.execute_pending_operations_from_json()
                                else:
                                    # Nodo remoto - usar RemoteDBManager
                                    logger.info(f"Restaurando JSON remotamente en {node_id}")
                                    
                                    # restore_json_from_dump necesita recibir json_data como parámetro
                                    client = remote_db._get_client_server(node_id)
                                    result = client.restore_json_from_dump(source_json)
                                    
                                    if result.get("success"):
                                        logger.info(f"JSON restaurado en {node_id}, ejecutando operaciones pending...")
                                        # Ejecutar operaciones pending del JSON
                                        client.execute_pending_operations_from_json()
                                        logger.info(f"Operaciones pending ejecutadas en {node_id}")
                                    else:
                                        logger.error(f"Error restaurando JSON en {node_id}: {result.get('error', 'Unknown')}")
                                        raise Exception(f"Failed to restore JSON: {result.get('error')}")
                                
                                # Actualizar versiones del nuevo nodo
                                with self.raft._lock:
                                    self.raft.global_index["node_versions"][node_id]["db_version"] = max_db_version
                                    self.raft.global_index["node_versions"][node_id]["db_version_prev"] = source_json.get("db_version_prev", 0)
                                
                                logger.info(f"Nodo {node_id} sincronizado exitosamente con db_version={max_db_version}")
                            else:
                                logger.warning(f"JSON vacío o None recibido de {best_source_node}")
                        
                        except Exception as e:
                            logger.error(f"Error obteniendo o restaurando JSON de {best_source_node}: {e}")
                            import traceback
                            logger.error(traceback.format_exc())
                
                except Exception as e:
                    logger.error(f"Error sincronizando nuevo nodo DB {node_id}: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                
                logger.info(f"Nodo {node_id} promovido a nodo de base de datos")
        
        # Si sobran nodos DB
        elif len(active_db_nodes) > k - 1:
            excess = len(active_db_nodes) - (k - 1)
            
            logger.info(f"Sobran {excess} nodos DB")
            
            # Ordenar por db_version (menor primero) y eliminar el exceso
            db_nodes_with_versions = []
            for node_id in active_db_nodes:
                version_info = self.raft.global_index["node_versions"].get(node_id, {})
                db_version = version_info.get("db_version", 0)
                db_nodes_with_versions.append((node_id, db_version))
            
            db_nodes_with_versions.sort(key=lambda x: x[1])  # Ordenar por db_version
            nodes_to_remove = [node_id for node_id, _ in db_nodes_with_versions[:excess]]
            
            logger.info(f"Nodos a degradar: {nodes_to_remove}")
            
            for node_id in nodes_to_remove:
                current_db_nodes.discard(node_id)
                with self.raft._lock:
                    if node_id in self.raft.global_index["node_versions"]:
                        self.raft.global_index["node_versions"][node_id]["is_db_node"] = False
                        self.raft.global_index["node_versions"][node_id]["db_version"] = 0
                        self.raft.global_index["node_versions"][node_id]["db_version_prev"] = 0
                
                logger.info(f"Nodo {node_id} degradado de nodo de base de datos")
        
        else:
            logger.info(f"Nodos DB correctos: {len(active_db_nodes) + 1} de {k}")

            # --- Comprobación de sincronización de nodos DB ---
            try:
                # Obtener información de sincronización de todos los nodos DB
                node_sync_info = {}
                
                for node_id in current_db_nodes:
                    try:
                        if node_id == self.raft.node_id:
                            # Nodo líder local
                            node_data = self.raft_server.db_instance.json_manager.read()
                            db_version = node_data.get("db_version", 0)
                            last_op = self.raft_server.db_instance.json_manager.get_last_operation()
                        else:
                            # Nodo DB remoto
                            remote_db = RemoteDBManager()
                            node_data = remote_db.get_json_dump(node_id)
                            if node_data:
                                db_version = node_data.get("db_version", 0)
                                log = node_data.get("log", [])
                                last_op = log[-1] if log else None
                            else:
                                logger.warning(f"No se pudo obtener JSON del nodo {node_id}")
                                continue
                        
                        node_sync_info[node_id] = {
                            "db_version": db_version,
                            "last_operation": last_op,
                            "json_data": node_data
                        }
                        
                    except Exception as e:
                        logger.error(f"Error obteniendo info de sincronización de {node_id}: {e}")
                
                # Verificar si todos tienen la misma db_version y última operación
                if node_sync_info:
                    # Obtener valores del líder
                    leader_id = self.raft.node_id
                    if leader_id in node_sync_info:
                        leader_info = node_sync_info[leader_id]
                        expected_db_version = leader_info["db_version"] if leader_info["db_version"] else -1
                        expected_last_op = leader_info["last_operation"] if leader_info["last_operation"] else {"term": -1, "task_id": -1}
                        
                        term1 = expected_last_op["term"]
                        task1 = expected_last_op["task_id"]
                        logger.info(f"Líder {leader_id}: db_version={expected_db_version}, last_op_term={term1}, task_id={task1}")
                        
                        # Comparar con cada nodo DB
                        for node_id, node_info in node_sync_info.items():
                            if node_id == leader_id:
                                continue
                            
                            node_db_version = node_info["db_version"] if node_info["db_version"] else -1
                            node_last_op = node_info["last_operation"] if node_info["last_operation"] else {"term": -1, "task_id": -1}
                            
                            term2 = node_last_op["term"]
                            task2 = node_last_op["task_id"]
                            logger.info(f"Nodo {node_id}: db_version={node_db_version}, last_op_term={term2}, task_id={task2}")
                            
                            # Comparar db_version
                            if (node_db_version != expected_db_version) or (expected_last_op.get("task_id") != node_last_op.get("task_id") or (node_last_op["term"] != expected_last_op["term"])) :
                                
                                logger.warning(f"Nodo {node_id} y lider: {leader_id} estan desincronizados. \ndb_version: {node_db_version} vs {expected_db_version} \ntask_id: {task1} vs {task2}")
                                
                                logger.info(f"Sincronizando nodo {node_id} con {leader_id}...")
                                # self._sync_db_nodes(
                                #         node_id,
                                #         leader_id, 
                                #         node_info["json_data"], 
                                #         leader_info["json_data"]
                                #     )
            
            except Exception as e:
                logger.error(f"Error en comprobación de sincronización: {e}")
                import traceback
                traceback.print_exc()
                
        # Actualizar el índice global
        with self.raft._lock:
            self.raft.global_index["db_nodes"] = current_db_nodes
            self.raft.global_index["version"] += 1
        # logger.info(f"=== FIN GESTION DE NODOS DB ===\n")

    # def _sync_db_nodes(node_id: str, node_id_2: str, node_info: dict, node_info_2: dict):
       

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


    def _detect_node_state_changes(self):
        """Detecta cambios en el estado de los nodos"""
        from raft.discovery import discover_active_clients
        
        if self.raft.state != "leader":
            return
        
        current_active = set(discover_active_clients())
        current_active = self._filter_active_nodes(current_active)
        logger.info(f"Nodos activos: {', '.join(current_active)}")
        
        # Leer si es primera vez (lock corto)
        with self.raft_server._lock:
            is_first_time = not hasattr(self.raft_server, 'previous_active_nodes')
        
        if is_first_time:
            # Primera vez - todos los nodos activos son ALIVE
            logger.info("[First] Primera vez poniendo todos los nodos activos como vivos")
            
            with self.raft_server._lock:
                self.raft_server.node_states[self.host] = "ALIVE"

                for node_ip in current_active:
                    self.raft_server.node_states[node_ip] = "ALIVE"
                self.raft_server.previous_active_nodes = set(self.raft_server.node_states.keys())
            return
        
        # Leer estados actuales y previous_active_nodes (lock corto)
        with self.raft_server._lock:
            node_states_copy = self.raft_server.node_states.copy()
            previous_active_copy = self.raft_server.previous_active_nodes.copy()
        
        logger.info("Nodos y sus estados: ")
        for node, state in node_states_copy.items():
            logger.info(f"  {node}: {state}")
        
        # Detectar nodos que revivieron o son nuevos
        new_nodes = current_active - previous_active_copy
        logging.info(new_nodes)
        
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
                except:
                    pass
                
                # Check 2: Tiene shards?
                if not is_respawn:
                    from raft.leader_manager import RemoteStorageManager
                    remote_storage = RemoteStorageManager()
                    files = remote_storage.list_files(node_ip)
                    if files:
                        is_respawn = True
            except:
                pass
            
            # Actualizar estado (lock corto)
            new_state = "RE-SPAWN" if is_respawn else "NEW"
            with self.raft_server._lock:
                self.raft_server.node_states[node_ip] = new_state
            
            logger.info(f"Nodo {node_ip} detectado como {new_state}")
        
        # Detectar nodos que murieron
        dead_nodes = previous_active_copy - current_active
        logger.info(
            f"[NODE_STATE] Nodos activos anteriores: {' ,'.join(previous_active_copy)}, "
            f"Nodos activos actuales: {' ,'.join(current_active)}, "
            f"Muertos: {' ,'.join(dead_nodes)}"
        )
        
        for node_ip in dead_nodes:
            # Leer y escribir estado (lock corto)
            with self.raft_server._lock:
                if node_ip in self.raft_server.node_states and self.raft_server.node_states[node_ip] == "ALIVE":
                    self.raft_server.node_states[node_ip] = "DEAD"
                    should_log = True
                else:
                    should_log = False
            
            if should_log:
                logger.info(f"Nodo {node_ip} detectado como DEAD")
        
        # Actualizar nodos que siguen activos
        for node_ip in current_active:
            with self.raft_server._lock:
                if node_ip in self.raft_server.node_states and self.raft_server.node_states[node_ip] not in ["RE-SPAWN", "NEW"]:
                    self.raft_server.node_states[node_ip] = "ALIVE"
        
        # Actualizar previous_active_nodes (lock corto)
        with self.raft_server._lock:
            self.raft_server.previous_active_nodes = set(self.raft_server.node_states.keys())

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
            
            elif state == "NEW":
                threading.Thread(
                    target=self._process_new_node,
                    args=(node_ip,),
                    daemon=True
                ).start()

    def _process_respawn_node(self, node_ip: str):
        """Procesa un nodo que revivió"""
        from raft.db_json_manager import DBJsonManager
        from raft.leader_manager import RemoteDBManager, RemoteStorageManager

        logger.info(f"Procesando nodo RE-SPAWN: {node_ip}")
        
        try:
            # 1. Verificar si era nodo DB
            remote_db = RemoteDBManager()
            json_manager = DBJsonManager()
            leader_json = json_manager.read()
            
            try:
                remote_json = remote_db.get_json_dump(node_ip)
                if remote_json:
                    
                    # Comparar db_version
                    remote_version = remote_json.get("db_version", 0)
                    leader_version = leader_json.get("db_version", 0)
                    
                    # Comparar últimas 5 operaciones
                    remote_ops = remote_json.get("log", [])
                    leader_ops = leader_json.get("log", [])
                    
                    is_updated = (remote_version == leader_version and remote_ops == leader_ops)
                    
                    if not is_updated:
                        # Contar nodos DB actualizados
                        updated_db_count = sum(
                            1 for nid in self.raft.global_index.get("db_nodes", set())
                            if nid != node_ip and self.raft_server.node_states.get(nid) == "ALIVE"
                        )
                        
                        if updated_db_count < self.raft.db_replication_factor:
                            # Actualizar el nodo
                            self._sync_db_node(node_ip, remote_json, leader_json)
                        else:
                            # Degradar a no-DB
                            self._demote_db_node(node_ip)
                            is_db_node = False
            except Exception as e:
                logger.info(f"Error verificando BD en {node_ip}: {e}")
            
            # 2. Actualizar IP en índice global (matching shards)
            self._update_respawn_node_ip(node_ip)
            
            # 3. Eliminar réplicas sobrantes
            self._remove_excess_replicas(node_ip)
            
            # 4. Balanceo de shards
            self._balance_shards(node_ip)
            
            # 5. Marcar como ALIVE
            with self.raft_server._lock:
                self.raft_server.node_states[node_ip] = "ALIVE"
            
            logger.info(f"Nodo {node_ip} procesado y marcado como ALIVE")
        
        except Exception as e:
            logger.info(f"Error procesando RE-SPAWN {node_ip}: {e}")

    def _process_new_node(self, node_ip: str):
        """Procesa un nodo completamente nuevo"""
        logger.info(f"Procesando nodo NEW: {node_ip}")
        
        try:
            # Solo hacer balanceo
            self._balance_shards(node_ip)
            
            # Marcar como ALIVE
            with self.raft_server._lock:
                self.raft_server.node_states[node_ip] = "ALIVE"
            
            logger.info(f"Nodo NEW {node_ip} procesado y marcado como ALIVE")
        
        except Exception as e:
            logger.error(f"Error procesando NEW {node_ip}: {e}")

    def _sync_db_node(self, node_ip: str, remote_json: dict, leader_json: dict):
        """Sincroniza un nodo DB desactualizado enviando solo operaciones faltantes"""
        from raft.leader_manager import RemoteDBManager
        
        logger.info(f"Sincronizando nodo DB {node_ip} incrementalmente")
        
        try:
            remote_db = RemoteDBManager()
            
            # 1. Obtener operaciones del nodo remoto
            remote_ops = remote_json.get("log", [])
            leader_ops = leader_json.get("log", [])
            
            # 2. Identificar operaciones pending en el nodo remoto
            remote_pending = [op for op in remote_ops if op.get("status") == "pending"]
            
            # 3. Ejecutar las pending del nodo remoto EN ORDEN
            if remote_pending:
                # Ordenar por el orden en que aparecen en el log
                remote_pending.sort(key=lambda x: remote_ops.index(x))
                
                logger.info(f"Ejecutando {len(remote_pending)} operaciones pending en {node_ip}")
                
                for pending_op in remote_pending:
                    try:
                        # Mandar a ejecutar en el nodo remoto
                        result = remote_db.execute_single_operation(pending_op, node_ip)
                        
                        if result.get("success"):
                            logger.info(f"Operación {pending_op['task_id']} ejecutada en {node_ip}")
                        else:
                            logger.info(f"Advertencia ejecutando {pending_op['task_id']}: {result}")
                    
                    except Exception as e:
                        logger.info(f"Error ejecutando pending en {node_ip}: {e}")
            
            # 4. Identificar operaciones que tiene el líder pero no el nodo remoto
            remote_task_ids = {op["task_id"] for op in remote_ops}
            
            missing_ops = [
                op for op in leader_ops 
                if op["task_id"] not in remote_task_ids and op.get("status") == "completed"
            ]
            
            # 5. Enviar operaciones faltantes EN ORDEN
            if missing_ops:
                # Ordenar por el orden en el log del líder
                missing_ops.sort(key=lambda x: leader_ops.index(x))
                
                logger.info(f"Enviando {len(missing_ops)} operaciones faltantes a {node_ip}")
                
                for missing_op in missing_ops:
                    try:
                        result = remote_db.execute_single_operation(missing_op, node_ip)
                        
                        if result.get("success"):
                            logger.info(f"Operación {missing_op['task_id']} enviada a {node_ip}")
                        else:
                            logger.warning(f"Advertencia enviando {missing_op['task_id']}: {result}")
                    
                    except Exception as e:
                        logger.error(f"Error enviando operación a {node_ip}: {e}")
            
            logger.info(f"Nodo DB {node_ip} sincronizado exitosamente")
        
        except Exception as e:
            logger.info(f"Error sincronizando nodo DB {node_ip}: {e}")
            import traceback
            traceback.print_exc()

    def _demote_db_node(self, node_ip: str):
        """Degrada un nodo de DB a nodo normal"""
        try:
            with self.raft._lock:
                # Eliminar de db_nodes
                self.raft.global_index["db_nodes"].discard(node_ip)
                
                if node_ip in self.raft.global_index["node_versions"]:
                    self.raft.global_index["node_versions"][node_ip]["is_db_node"] = False
                    self.raft.global_index["node_versions"][node_ip]["db_version"] = 0
                    self.raft.global_index["node_versions"][node_ip]["db_version_prev"] = 0
            
            logger.info(f"Nodo {node_ip} degradado de nodo DB")
        
        except Exception as e:
            logger.info(f"Error degradando nodo {node_ip}: {e}")

    def _update_respawn_node_ip(self, node_ip: str):
        """Actualiza la IP de un nodo que revivió en el índice"""
        from raft.leader_manager import RemoteStorageManager
        
        try:
            logger.info(f"Actualizando IP para nodo RE-SPAWN: {node_ip}")
            
            remote_storage = RemoteStorageManager()
            
            # Obtener shards que tiene el nodo que revivió
            try:
                # Obtener archivos del nodo
                files = remote_storage.list_files(node_ip)
                
                if not files:
                    logger.info(f"Nodo {node_ip} no tiene archivos")
                    return
                
                # Construir mapa de shards del nodo actual
                current_shards = {}  # {filename: [range_keys]}
                
                for filename in files:
                    ranges = remote_storage.get_file_ranges(filename, node_ip)
                    if ranges:
                        current_shards[filename] = ranges
                
                if not current_shards:
                    logger.info(f"Nodo {node_ip} no tiene shards")
                    return
                
                # Buscar en nodos DEAD cuál tiene los mismos shards
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
                    
                    for filename, ranges in current_shards.items():
                        if filename in old_shards:
                            matching_ranges = set(ranges) & set(old_shards[filename])
                            coincidences += len(matching_ranges)
                    
                    if coincidences > max_coincidences:
                        max_coincidences = coincidences
                        matched_old_node = old_node_id
                
                if matched_old_node and max_coincidences > 0:
                    logger.info(
                        f"Nodo {node_ip} identificado como {matched_old_node} "
                        f"(coincidencias: {max_coincidences} shards)"
                    )
                    
                    with self.raft._lock:
                        # Actualizar el índice: transferir info del nodo viejo al nuevo
                        if matched_old_node in self.raft.global_index.get("node_versions", {}):
                            old_versions = self.raft.global_index["node_versions"][matched_old_node]
                            
                            self.raft.global_index["node_versions"][node_ip] = {
                                "read_version": old_versions.get("read_version", 0),
                                "write_version": old_versions.get("write_version", 0),
                                "db_version": old_versions.get("db_version", 0),
                                "db_version_prev": old_versions.get("db_version_prev", 0),
                                "is_db_node": old_versions.get("is_db_node", False)
                            }
                            
                            # Eliminar entrada del nodo viejo
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
                        
                        # Actualizar chunk_distribution en files_metadata
                        for filename, metadata in self.raft.global_index.get("files_metadata", {}).items():
                            distribution = metadata.get("chunk_distribution", {})
                            
                            for range_key, nodes in distribution.items():
                                if matched_old_node in nodes:
                                    nodes.remove(matched_old_node)
                                    nodes.append(node_ip)
                    
                    # Actualizar estado del nodo
                    with self.raft_server._lock:
                        del self.raft_server.node_states[matched_old_node]
                        self.raft_server.node_states[node_ip] = "RE-SPAWN"
                    
                    logger.info(f"IP actualizada: {matched_old_node} -> {node_ip}")
                else:
                    logger.info(f"No se encontró nodo DEAD correspondiente a {node_ip}")
            
            except Exception as e:
                logger.info(f"Error obteniendo shards de {node_ip}: {e}")
        
        except Exception as e:
            logger.info(f"Error en _update_respawn_node_ip: {e}")
            import traceback
            traceback.print_exc()

    def _remove_excess_replicas(self, node_ip: str):
        """Elimina réplicas sobrantes (mantener k=3)"""
        from raft.leader_manager import RemoteStorageManager
        
        try:
            remote_storage = RemoteStorageManager()
            files = remote_storage.list_files(node_ip)
            
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
                                logger.info(f"Eliminado shard {range_key} de {max_node}")
        
        except Exception as e:
            logger.error(f"Error eliminando réplicas sobrantes: {e}")

    def _balance_shards(self, target_node_ip: str):
        """Balancea shards entre nodos copiando del más cargado al target"""
        from raft.leader_manager import RemoteStorageManager
        
        try:
            logger.info(f"Iniciando balanceo de shards hacia {target_node_ip}")
            
            remote_storage = RemoteStorageManager()
            
            # Calcular total de chunks por nodo usando node_shards
            node_shards_info = self.raft.global_index.get("node_shards", {})
            
            if not node_shards_info:
                logger.info("No hay información de shards para balancear")
                return
            
            node_chunks = {
                node_id: info.get("total_chunks", 0)
                for node_id, info in node_shards_info.items()
            }
            
            # Agregar target si no existe
            if target_node_ip not in node_chunks:
                node_chunks[target_node_ip] = 0
            
            if len(node_chunks) < 2:
                logger.info("No hay suficientes nodos para balancear")
                return
            
            # Nodo con más chunks (excluyendo target)
            max_node = max(
                (nid for nid in node_chunks if nid != target_node_ip),
                key=lambda n: node_chunks[n],
                default=None
            )
            
            if not max_node:
                return
            
            max_chunks = node_chunks[max_node]
            target_chunks = node_chunks[target_node_ip]
            
            # Si la diferencia no es significativa, no hacer nada
            if max_chunks - target_chunks < 5:
                logger.info(
                    f"Diferencia de chunks no significativa: {max_chunks} vs {target_chunks}"
                )
                return
            
            logger.info(
                f"Balanceando desde {max_node} ({max_chunks} chunks) "
                f"hacia {target_node_ip} ({target_chunks} chunks)"
            )
            
            # Obtener shards del nodo con más chunks
            max_node_shards = node_shards_info.get(max_node, {}).get("shards", {})
            
            if not max_node_shards:
                logger.info(f"Nodo {max_node} no tiene shards")
                return
            
            # Encontrar el shard más grande
            largest_shard = None
            largest_size = 0
            largest_filename = None
            
            for filename, range_keys in max_node_shards.items():
                for range_key in range_keys:
                    range_start, range_end = map(int, range_key.split("-"))
                    shard_size = range_end - range_start
                    
                    if shard_size > largest_size:
                        largest_size = shard_size
                        largest_shard = range_key
                        largest_filename = filename
            
            if not largest_shard:
                logger.info("No se encontró shard para copiar")
                return
            
            logger.info(
                f"Copiando shard {largest_shard} de {largest_filename} "
                f"({largest_size} chunks) desde {max_node} a {target_node_ip}"
            )
            
            # Copiar el shard
            try:
                # 1. Leer el shard del nodo origen
                range_data = remote_storage.get_chunk_range(largest_filename, largest_shard, max_node)
                
                # 2. Escribir en el nodo destino
                remote_storage.create_file_range(largest_filename, range_data, largest_shard, target_node_ip)
                
                # 3. Actualizar el índice global
                with self.raft._lock:
                    files_metadata = self.raft.global_index.get("files_metadata", {})
                    
                    if largest_filename in files_metadata:
                        distribution = files_metadata[largest_filename].get("chunk_distribution", {})
                        
                        if largest_shard in distribution:
                            if target_node_ip not in distribution[largest_shard]:
                                distribution[largest_shard].append(target_node_ip)
                    
                    # 4. Actualizar node_shards para target
                    if target_node_ip not in node_shards_info:
                        node_shards_info[target_node_ip] = {
                            "total_chunks": 0,
                            "shards": {}
                        }
                    
                    if largest_filename not in node_shards_info[target_node_ip]["shards"]:
                        node_shards_info[target_node_ip]["shards"][largest_filename] = []
                    
                    if largest_shard not in node_shards_info[target_node_ip]["shards"][largest_filename]:
                        node_shards_info[target_node_ip]["shards"][largest_filename].append(largest_shard)
                        node_shards_info[target_node_ip]["total_chunks"] += largest_size
                    
                    # 5. Actualizar files
                    if target_node_ip not in self.raft.global_index["files"]:
                        self.raft.global_index["files"][target_node_ip] = []
                    
                    if largest_filename not in self.raft.global_index["files"][target_node_ip]:
                        self.raft.global_index["files"][target_node_ip].append(largest_filename)
                
                logger.info(
                    f"Shard {largest_shard} copiado exitosamente a {target_node_ip}"
                )
                
                # 6. Si ahora hay más de k=3 réplicas, eliminar del nodo origen
                distribution = files_metadata.get(largest_filename, {}).get("chunk_distribution", {})
                nodes_with_shard = distribution.get(largest_shard, [])
                
                if len(nodes_with_shard) > self.raft.db_replication_factor:
                    logger.info(f"Eliminando shard {largest_shard} de {max_node} (réplicas excedidas)")
                    
                    deleted = remote_storage.delete_file_range(largest_filename, largest_shard, max_node)
                    
                    if deleted:
                        # Actualizar índice
                        with self.raft._lock:
                            nodes_with_shard.remove(max_node)
                            
                            # Actualizar node_shards
                            if max_node in node_shards_info:
                                if largest_filename in node_shards_info[max_node]["shards"]:
                                    if largest_shard in node_shards_info[max_node]["shards"][largest_filename]:
                                        node_shards_info[max_node]["shards"][largest_filename].remove(largest_shard)
                                        node_shards_info[max_node]["total_chunks"] -= largest_size
                                        
                                        # Si no quedan más shards de este archivo, eliminarlo
                                        if not node_shards_info[max_node]["shards"][largest_filename]:
                                            del node_shards_info[max_node]["shards"][largest_filename]
                        
                        logger.info(f"Shard eliminado de {max_node}")
            
            except Exception as e:
                logger.error(f"Error copiando shard: {e}")
                import traceback
                traceback.print_exc()
            
            # Marcar como ALIVE una vez completado
            with self.raft_server._lock:
                if self.raft_server.node_states.get(target_node_ip) in ["RE-SPAWN", "NEW"]:
                    self.raft_server.node_states[target_node_ip] = "ALIVE"
                    logger.info(f"Nodo {target_node_ip} marcado como ALIVE tras balanceo")
        
        except Exception as e:
            logger.info(f"Error balanceando shards: {e}")
            import traceback
            traceback.print_exc()

    def _update_index(self):
        """
        Reconstruye el índice global de archivos y metadatos.
        Se ejecuta solo en el líder.
        """
        if self.raft.state != "leader":
            return False
        
        from .discovery import discover_active_clients
        
        # Actualizar lista de archivos de cada nodo
        nodes = discover_active_clients()
        nodes_str = " ,".join(nodes)
        logger.info(f"[Index] Nodos activos descubiertos {nodes_str}")
        if self.raft_server is None:
            logging.warning("[Index] Es None el raf server en update index")
        for node in nodes:
            try:
                if node == self.raft_server.node_id:
                    # Archivos del nodo local
                    files = self.raft_server.storage_instance.list_files()
                else:
                    # Archivos de nodo remoto
                    remote = RemoteStorageManager()
                    files = remote.list_files(node)
                
                with self.raft._lock:
                    self.raft.global_index["files"][node] = files
                    
                    # Asegurar que el nodo tenga entrada en node_versions
                    if node not in self.raft.global_index["node_versions"]:
                        self.raft.global_index["node_versions"][node] = {
                            "read_version": 0,
                            "write_version": 0,
                            "db_version": 0,
                            "db_version_prev": 0,
                            "is_db_node": node in self.raft.global_index["db_nodes"]
                        }
            except Exception as e:
                logger.error(f"Error al obtener archivos del nodo {node}: {e}")
                self.raft.global_index["files"][node] = []
        
        # Incrementar versión del índice
        with self.raft._lock:
            self.raft.global_index["version"] += 1
        
        return 
    
    def _process_pending_tasks(self):
        """Procesa tareas pendientes recuperadas del log"""
        # with self.raft._lock:
        #     pending = [t for t in self.raft.pending_tasks if t["status"] == "pending"]
            
        #     for task in pending[:5]:  # Procesar máximo 5 por ciclo
        #         try:
        #             task_type = task["request_data"].get("type")
                    
        #             if task_type == "metadata_write":
        #                 self._retry_metadata_write(task)
        #             elif task_type == "file_write":
        #                 self._retry_file_write(task)
        #             #TODO: Implementar correctamente esta funcion cuando se vaya a usar SSE
                    
        #         except Exception as e:
        #             logger.error(f"Error procesando tarea {task['task_id']}: {e}")
        pass

    def _retry_metadata_write(self, task: dict):
        """Reintenta una escritura de metadata pendiente"""
        # Implementación de retry para metadata
        pass  # TODO: implementar cuando sea necesario

    def _retry_file_write(self, task: dict):
        """Reintenta una escritura de archivo pendiente"""
        # Implementación de retry para files
        pass  # TODO: implementar cuando sea necesario

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
        
        db_nodes, read_versions = self._get_sorted_db_nodes_by_read()
        db_nodes = self._filter_alive_nodes(db_nodes)
        
        if not db_nodes:
            raise Exception("No hay nodos de base de datos disponibles")

        logging.info("\n\n[LEADER] En read_metadata:")
        logging.info("Nodos DB vivos %s", db_nodes)

        
        # Intentar con nodos en orden de menor read_version
        # Preparamos un string con la lista de nodos y sus reads en orden
        node_list_str = ", ".join([f"{node} (read: {read_versions[node]})" for node in db_nodes])
        logging.info("\n\n[LEADER] Escogiendo nodo DB para lectura de metadata, opciones (ordenadas por menor read): %s", node_list_str)
        for node_id in db_nodes:
            try:
                logging.info("[LEADER] Intentando leer metadata desde nodo DB %s", node_id)
                self.raft.update_node_version("read", node_id)
                
                if node_id == self.raft_server.host:
                    logging.info("[LEADER] Leyendo metadata localmente desde nodo DB %s", node_id)
                    result = self.raft_server.db_instance.get_data(query_data)
                else:
                    logging.info("[LEADER] Leyendo metadata remotamente desde nodo DB %s", node_id)
                    remote_db = RemoteDBManager()
                    result = remote_db.get_data(query_data, node_id)
                
                return result
            
            except Exception as e:
                logger.warning(f"Error leyendo de nodo DB {node_id}: {e}")
                continue
        
        raise Exception("No se pudo leer de ningún nodo DB")

    # ============================================================================
    # COORDINACIÓN DE ESCRITURA DE METADATA (2PC)
    # ============================================================================

    def write_metadata(self, metadata_obj, operation: str):
        """
        Escribe metadatos en k nodos DB con commit en dos fases.
        operation: 'create', 'update', 'delete'
        """        
        if self.raft.state != "leader":
            raise Exception("Solo el líder puede coordinar operaciones")
        
        # Crear tarea pendiente
        task_data = {
            "type": "metadata_write",
            "operation": operation,
            "metadata": self._serialize_metadata(metadata_obj),
            "timestamp": time.time(),
            "term": self.raft.current_term,
        }

        task_id = self.raft.add_pending_task(task_data)
        
        try:
            db_nodes = list(self.raft.global_index["db_nodes"])
            db_nodes = self._filter_alive_nodes(db_nodes)
            prepare_results = {}

            logging.info("Nodos DB vivos %s", db_nodes)
            # FASE 1: PREPARE
            for node_id in db_nodes:
                try:
                    if node_id == self.raft_server.host:
                        if operation == "create" or operation == "update":
                            result = self.raft_server.db_instance.prepare_create(metadata_obj=metadata_obj, task_id=task_id, term=self.raft.current_term)
                        else:
                            result = self.raft_server.db_instance.prepare_delete(metadata_obj=metadata_obj, task_id=task_id, term=self.raft.current_term)
                    else:
                        remote_db = RemoteDBManager()
                        safe_metadata_dict = self.raft_server.db_instance.serialize_for_transfer(metadata_obj)
                        result = remote_db.prepare_operation(node_id, safe_metadata_dict, operation, task_id, term=self.raft.current_term)
                    
                    prepare_results[node_id] = result
                    
                    # NO ACTUALIZAR VERSION DE WRITE EN OPERACIONES DE METADATOS
                    # if result.get("success"):
                    #     self.raft.update_node_version("write", node_id) 
                
                except Exception as e:
                    logger.error(f"Error en prepare en {node_id}: {e}")
                    prepare_results[node_id] = {"success": False, "error": str(e)}
            
            # Verificar que todos los nodos DB respondieron OK
            success_nodes = [
                node_id for node_id, result in prepare_results.items() 
                if result.get("success")
            ]

            logging.info(f"Nodos con prepare exitoso: {', '.join(success_nodes)}, Total: {len(success_nodes)}")
            
            # FASE 2: COMMIT
            commit_success_count, commit_success_nodes = self._commit_metadata_write(success_nodes, task_id)

            if commit_success_count < len(success_nodes):
                logger.warning(f"Nodos con commit exitoso: {', '.join(commit_success_nodes)}, Total: {commit_success_count}")
            elif commit_success_count == len(success_nodes):
                logger.info(f"Nodos con commit exitoso: {', '.join(commit_success_nodes)}, Total: {len(commit_success_nodes)}")

            # Marcar tarea como completada
            # result = prepare_results[self.raft_server.host]
            # self.raft.update_task_status(task_id, "completed", result)
            
            return result
        
        except Exception as e:
            # self.raft.update_task_status(task_id, "failed", str(e))
            logger.error(f"[Write Metadata] Error: {e}")

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
                        break
                
                except Exception as e:
                    logger.info(f"Error en commit en {node_id} (intento {attempt + 1}/{max_retries}): {e}")
                    if attempt < max_retries - 1:
                        time.sleep(1)
            
            if not committed:
                logger.info(f"Commit falló definitivamente en {node_id} después de {max_retries} intentos")
        
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
                    logger.error(f"Error en rollback en {node_id} (intento {attempt + 1}/3): {e}")
                    if attempt < 2:
                        time.sleep(1)
            
            if not rolled_back:
                logger.error(f"Rollback falló definitivamente en {node_id}")
    
    # ============================================================================
    # COORDINACIÓN DE LECTURA DE ARCHIVOS (DISTRIBUCIÓN DE CARGA)
    # ============================================================================

    def read_file_chunks(self, filename: str, start_chunk: int, chunk_count: int):
        """
        Coordina la lectura de chunks de un archivo.
        Selecciona UN nodo (menor read_version) y le delega toda la operación.
        """
        from .raft import RaftServer
        if isinstance(self.raft, RaftServer):
            print("En read_file_chunks")

        if self.raft.state != "leader":
            raise Exception("Solo el líder puede coordinar operaciones")
        
        # Verificar que el archivo existe en el índice
        if filename not in self.raft.global_index.get("files_metadata", {}):
            raise FileNotFoundError(f"Archivo {filename} no encontrado en el índice global")
        
        # Obtener TODOS los nodos disponibles
        all_nodes = list(self.raft.global_index["node_versions"].keys())
        
        if not all_nodes:
            raise Exception("No hay nodos disponibles")
        
        # Seleccionar el nodo con menor read_version
        selected_node = self._select_node_by_version(all_nodes, "read")
        
        # Actualizar read_version ANTES de delegar
        self.raft.update_node_version("read", selected_node)
        
        logger.info(
            f"Delegando lectura de {filename} chunks [{start_chunk}:{start_chunk + chunk_count}] "
            f"al nodo {selected_node}"
        )
        
        # Delegar la lectura completa
        if selected_node == self.raft_server.host:
            chunks = self._read_chunks_delegated_local(filename, start_chunk, chunk_count)
        else:
            chunks = self._read_chunks_delegated_remote(selected_node, filename, start_chunk, chunk_count)
        
        return chunks

    def _read_chunks_delegated_local(self, filename: str, start_chunk: int, chunk_count: int) -> list:
        """
        Lectura delegada cuando el nodo seleccionado es el líder.
        El líder busca chunks locales y pide faltantes a otros nodos.
        """
        # Llamar al método del storage_instance que hace toda la lógica
        return self.raft_server.storage_instance.read_chunks_delegated(
            filename,
            start_chunk,
            chunk_count,
            self.raft.global_index
        )

    def _read_chunks_delegated_remote(
        self, node_id: str, filename: str, start_chunk: int, chunk_count: int
    ) -> list:
        """
        Delega la lectura completa a un nodo remoto con reintentos.
        """
        for attempt in range(3):
            try:
                remote_storage = RemoteStorageManager()
                chunks = remote_storage.read_chunks_delegated(
                    filename,
                    start_chunk,
                    chunk_count,
                    node_id,
                    self.raft.global_index
                )
                
                return chunks
            
            except Exception as e:
                logger.error(f"Error delegando lectura a {node_id} (intento {attempt + 1}/3): {e}")
                if attempt < 2:
                    time.sleep(1)
        
        raise Exception(f"Error delegando lectura a nodo {node_id} después de 3 intentos")

    # ============================================================================
    # COORDINACIÓN DE ESCRITURA DE ARCHIVOS
    # ============================================================================

    def write_file(self, filename: str, file_data: bytes):
        """
        Escribe un archivo dividiéndolo en p rangos de chunks,
        replicado en k nodos por cada rango.
        """

        from backend.settings import CHUNK_SIZE, CHUNK_RANGES

        if self.raft.state != "leader":
            raise Exception("Solo el líder puede coordinar operaciones")
        
        # Dividir archivo en chunks
        total_size = len(file_data)
        total_chunks = ceil(total_size / CHUNK_SIZE)
        
        # Dividir en p rangos
        chunk_ranges = self._divide_into_ranges(0, total_chunks, CHUNK_RANGES)
        
        # Para cada rango, seleccionar k nodos con menor write_version
        distribution = {}
        write_tasks = []
        already_selected = {}  # Diccionario para contar selecciones por nodo

        import logging
        logger = logging.getLogger("LeaderManager")
        logger.info(f"\n=== INICIO write_file ===")
        logger.info(f"Nodos en global_index: {list(self.raft.global_index['node_versions'].keys())}")

        # Convertir el diccionario a string
        versiones_iniciales = {node: info.get('write_version', 0) for node, info in self.raft.global_index['node_versions'].items()}
        logger.info(f"Versiones iniciales write: {str(versiones_iniciales)}")

        for i, chunk_range in enumerate(chunk_ranges):
            # Obtener nodos disponibles
            all_nodes = list(self.raft.global_index["node_versions"].keys())
            all_nodes = self._filter_alive_nodes(all_nodes)

            available_nodes = all_nodes
            
            logger.info(f"\n--- Rango {i} ({chunk_range[0]}-{chunk_range[1]}) ---")
            logger.info(f"Nodos disponibles: {available_nodes}")
            # Convertir el diccionario a string
            logger.info(f"Ya seleccionados en esta operación: {str(already_selected)}")
            
            # Seleccionar k nodos con menor write_version, considerando ya seleccionados
            selected_nodes = self._select_k_nodes_by_version(
                available_nodes,
                "write",
                self.raft.db_replication_factor,
                already_selected=already_selected
            )
            
            logger.info(f"Nodos seleccionados para este rango: {selected_nodes}")
            
            # Actualizar contador de selecciones
            for node_id in selected_nodes:
                already_selected[node_id] = already_selected.get(node_id, 0) + 1
            
            range_key = f"{chunk_range[0]}-{chunk_range[1]}"
            distribution[range_key] = selected_nodes
            
            # Extraer datos del rango
            start_byte = chunk_range[0] * CHUNK_SIZE
            end_byte = min(chunk_range[1] * CHUNK_SIZE, total_size)
            range_data = file_data[start_byte:end_byte]
            
            # Crear tareas de escritura
            for node_id in selected_nodes:
                write_tasks.append({
                    "node_id": node_id,
                    "filename": filename,
                    "range_key": range_key,
                    "data": range_data
                })

        logger.info(f"\n=== FIN write_file ===")
        # Convertir diccionarios a string
        logger.info(f"Distribución final: {str(distribution)}")
        logger.info(f"Contador de selecciones: {str(already_selected)}")
        
        # Ejecutar escrituras en paralelo
        self._execute_write_tasks_parallel(write_tasks)
        
        # Actualizar índice global
        self._update_file_index(filename, total_chunks, distribution)
        
        return {
            "success": True,
            "distribution": distribution,
            "total_chunks": total_chunks
        }

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
                    logger.info(f"\n\n[FILE] Error: data es un dict, no bytes. Task:", task)
                    logger.info(f"Data: {data}")
                    
                if node_id == self.raft_server.host:
                    self.raft_server.storage_instance.create_file_range(filename, data, range_key)
                else:
                    remote_storage = RemoteStorageManager()
                    remote_storage.create_file_range(filename, data, range_key, node_id)
                
                self.raft.update_node_version("write", node_id)
                logger.info(f"Rango {range_key} escrito en nodo {node_id}")
            
            except Exception as e:
                errors.append({"task": task, "error": str(e)})
                logger.error(f"Error escribiendo en {node_id}: {e}")
        
        for task in tasks:
            t = threading.Thread(target=write_task, args=(task,))
            t.start()
            threads.append(t)
        
        from backend.settings import RPC_TIMEOUT
        for t in threads:
            t.join(timeout=RPC_TIMEOUT * 2)
        
        if errors:
            logger.warning(f"Errores en escritura en {len(tasks)} tareas: {errors}. Intentando continuar...")

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
            
        logger.info(f"Índice actualizado para {filename} (v{self.raft.global_index['version']})")

    def _get_sorted_db_nodes_by_read(self) -> (List[str], Dict[str, int]): # type: ignore
        """Retorna nodos DB ordenados por read_version (menor primero) y un diccionario con los read versions"""
        with self.raft._lock:
            db_nodes = list(self.raft.global_index["db_nodes"])
            node_versions = self.raft.global_index["node_versions"].copy()
        
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
    
    def _filter_alive_nodes(self, nodes: List[str]) -> List[str]:
        """Filtra solo los nodos que están actualmente vivos"""  
        from .discovery import discover_active_clients
        return [node for node in nodes if node in set(discover_active_clients())]

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

        nodes = get_service_tasks()

        for node in nodes:
            if node_id == node.ip:
                return self.raft_server._get_client_server(
                    node_id, node.ip, self.raft_server.port, "node", requires_validation=False
                )

        raise Exception("Error al buscar DB remota")
    

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
                    if abs(start - now) > 0.1 or retries > 100:
                        break 

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

        import logging
        logging.info(f"\n\n[LeaderManager] En prepare_operation para nodo {node_id}")
        if node_id == self.raft_server.node_id:
            if operation in ["create", "update"]:
                return self.db_manager.prepare_create(data=data, task_id=task_id, model_name=model_name, term=term)
            else:
                return self.db_manager.prepare_delete(data=data, task_id=task_id, model_name=model_name, term=term)

        client = self._get_client_server(node_id)
        if operation in ["create", "update"]:
            return client.prepare_create(data=data, task_id=task_id, model_name=model_name, term=term)
        else:
            return client.prepare_delete(data=data, task_id=task_id, model_name=model_name, term=term)

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
    