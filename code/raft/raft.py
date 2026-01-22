#raft.py
import random
import threading
import time
import uuid
import Pyro5.api as rpc

from dataclasses import dataclass
from typing import List, Optional, Callable, Any, Dict, Set

from .db_manager import DBManager
from .storage_manager import StorageManager
from .discovery import get_service_tasks

LOGGINGS_ENABLED = False
HEARTBEAT_INTERVAL = [1, 2.9] #0.5 antes del DNS
ELECTION_TIMEOUT_RANGE = (3, 7) #(1, 2) antes del DNS
DB_REPLICATION_FACTOR = 3  # k nodos de base de datos


class Colors:
    GREEN = "\033[92m"
    BLUE = "\033[94m"
    ORANGE = "\033[93m"
    RESET = "\033[0m"
    BOLD = "\033[1m"


@dataclass
class LogEntry:
    term: int
    command: Any


@dataclass
class RaftResponse:
    term: int
    success: bool


@rpc.expose
class RaftConsensusFunctions:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        """
        Se ejecuta antes que __init__. Aquí garantizamos el singleton.
        """
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:  # bloqueo de doble verificación
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
        node_id: int,
        on_timer_end_callback: Optional[Callable] = None,
        on_become_leader: Optional[Callable] = None,
        on_non_being_leader: Optional[Callable] = None,
    ):
        if getattr(self, "_initialized", False):
            return
        # inicializar aca el indice, vacio. Recordar poner la version de indice como un numero
        self.global_index = {
            "version": 0,
            "files": {},  # {node_id: [list of files]}
            "files_metadata": {},  # {filename: {"total_chunks": int, "chunk_distribution": dict, "created_at": float}}
            "db_nodes": set(),  # IDs de nodos que son base de datos
            "node_versions": {},  # {node_id: {"read_version": int, "write_version": int, "is_db_node": bool}}
            "node_shards": {}  # {node_id: {"total_chunks": int, "shards": {filename: [range_keys]}}}
        }
        self.db_replication_factor = DB_REPLICATION_FACTOR  # k nodos de base de datos (líder + k-1)
        self.pending_tasks = []  # Cola de tareas pendientes
        self.operation_cache = []  # Cache de últimas operaciones exitosas (máximo 5)
        self._initialized = True

        self.node_id: int = node_id
        self.current_term: int = 0
        self.voted_for: Optional[int] = None
        self.commit_index: int = 0
        self.last_applied: int = 0
        self.log: List[LogEntry] = []
        self.current_leader_id: Optional[int] = None
        self.state: str = "follower"

        self.on_timer_end_callback: Callable = on_timer_end_callback or (lambda: None)
        self.on_become_leader: Callable = on_become_leader or (lambda: None)
        self.on_non_being_leader: Callable = on_non_being_leader or (lambda: None)

        self._timer: Optional[threading.Timer] = None
        self.election_timeout_range = ELECTION_TIMEOUT_RANGE
        self.next_index: Dict[int, int] = {}
        self.match_index: Dict[int, int] = {}

        self._log(f"Inicializando nodo (term={self.current_term}, state={self.state})")
        self._start_timer()

    # UTILITY METHODS

    def _get_color(self) -> str:
        if self.state == "follower":
            return Colors.GREEN
        elif self.state == "candidate":
            return Colors.BLUE
        elif self.state == "leader":
            return Colors.ORANGE
        return Colors.RESET

    def _log(self, message: str):
        import logging
        if not LOGGINGS_ENABLED:
            return
        color = self._get_color()
        logging.info(f"{color}[Node {self.node_id}] {message}{Colors.RESET}")

    # STATE MANAGEMENT

    def become_follower(self, new_term: Optional[int] = None):
        if new_term is not None and new_term > self.current_term:
            self._log(f"Actualizando término: {self.current_term} -> {new_term}")
            self.current_term = new_term
            self.voted_for = None

        old_state = self.state
        self.state = "follower"
        self._log(
            f"Transición de estado: {old_state} -> FOLLOWER (term={self.current_term})"
        )
        self.on_non_being_leader()
        self._start_timer()

    def become_candidate(self):
        old_state = self.state
        self.state = "candidate"
        self.next_term()
        self.voted_for = self.node_id
        self.current_leader_id = None

        self._log(
            f"Transición de estado: {old_state} -> CANDIDATE (term={self.current_term})"
        )
        self._log(f"Votando por sí mismo en término {self.current_term}")

        self.on_non_being_leader()
        self.on_timer_end_callback()
        self._start_timer()

    def become_leader(self):
        old_state = self.state
        self.state = "leader"
        self.current_leader_id = self.node_id
        self.next_term()

        self._log(
            f"Transición de estado: {old_state} -> LEADER (term={self.current_term})"
        )
        self._cancel_timer()

        last_log_index = len(self.log)
        for node_id in self.next_index.keys():
            self.next_index[node_id] = last_log_index + 1
            self.match_index[node_id] = 0

        # Sincronizar base de datos ANTES de notificar que somos líder
        self._sync_database_on_become_leader()

        # DESPUÉS recuperar tareas pendientes del log
        # self.recover_pending_tasks_on_become_leader()

        # FINALMENTE notificar al callback externo
        self.on_become_leader()
        
        self._log(
            f"Inicializando índices de replicación: next_index={last_log_index + 1}, match_index=0"
        )

    # TIMER CONTROL

    def _start_timer(self):
        with self._lock:
            self._cancel_timer()
            timeout = random.uniform(*self.election_timeout_range)
            self._timer = threading.Timer(timeout, self._on_timeout)
            self._timer.daemon = True
            self._timer.start()
            self._log(f"Timer de elección iniciado: {timeout:.3f}s")

    def _cancel_timer(self):
        if self._timer:
            self._timer.cancel()
            self._timer = None
            self._log("Timer de elección cancelado")

    def _on_timeout(self):
        if self.state != "leader":
            self._log(
                f"Timeout de elección expirado (state={self.state}, term={self.current_term})"
            )
            self.become_candidate()

    # RAFT RPCs

    def next_term(self):
        self.current_term += 1

    def get_state(self) -> dict:
        return {
            "node_id": self.node_id,
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "log_length": len(self.log),
            "state": self.state,
            "current_leader_id": self.current_leader_id,
        }
    
    def append_entries(
        self,
        term: int,
        leader_id: int,
        prev_log_index: int,
        prev_log_term: int,
        leader_commit: int,
        entries: Optional[List[dict]] = None,
        leader_index: Optional[dict] = None,
        previous_active_nodes: Optional[List] = None,
        node_states: Optional[dict] = None
    ) -> dict:
        entries_count = len(entries) if entries else 0
        self._log(
            f"AppendEntries recibido de líder {leader_id}: "
            f"term={term}, prev_log_index={prev_log_index}, "
            f"prev_log_term={prev_log_term}, entries={entries_count}"
        )
        from raft.log_utils import log_info, log_warning, log_error
        log_entries = None
        if entries:
            log_entries = [
                LogEntry(term=e["term"], command=e["command"]) for e in entries
            ]

        if term < self.current_term:
            #AQUI USAR SU INDICE PARA MERGEAR ANTES DE RECHAZAR
            log_warning("RAFT CONSENSUS FUNCTION", f"Nodo: {self.node_id} con estado {self.state} recibió append entries de nodo \"leader\" {leader_id} con menor term al propio: propio {self.current_term} vs \"leader\": {term}")
            if leader_index:
                log_info("RAFT CONSENSUS FUNCTION", f"Como llego un leader_index se va a tomar todo lo que este tenga en el propio antes de rechazarlo")
                self._merge_index_info(leader_index)
            log_warning("RAFT CONSENSUS FUNCTION",
                f"Rechazando AppendEntries: term {term} < term actual {self.current_term}"
            )
            return {"term": self.current_term, "success": False}

        global_index_copy = {}
        if term >= self.current_term:
            if self.state == "leader":
                with self._lock:
                    global_index_copy = self.global_index.copy()
                
            if term > self.current_term:
                self._log(
                    f"Actualizando término desde líder {leader_id}: {self.current_term} -> {term}"
                )
            self.become_follower(new_term=term)
            self.current_leader_id = leader_id

        if prev_log_index > 0:
            if prev_log_index > len(self.log):
                self._log(
                    f"Inconsistencia de log: prev_log_index {prev_log_index} > longitud del log {len(self.log)}"
                )
                return {"term": self.current_term, "success": False}
            if self.log[prev_log_index - 1].term != prev_log_term:
                self._log(
                    f"Inconsistencia de log: término no coincide en índice {prev_log_index} "
                    f"(esperado={prev_log_term}, encontrado={self.log[prev_log_index - 1].term})"
                )
                #AQUI TAMBIEN ME QUEDO CON EL INDICE ,OTRA CSOA: UPDATE INDEX SOLO ACTUALIZA NO BORRA
                return {"term": self.current_term, "success": False}

        if log_entries:
            old_log_len = len(self.log)
            if len(self.log) > prev_log_index:
                self.log = self.log[:prev_log_index]
                self._log(f"Log truncado de {old_log_len} a {len(self.log)} entradas")
            self.log.extend(log_entries)
            self._log(
                f"Agregadas {len(log_entries)} entradas al log (total: {len(self.log)})"
            )

        if leader_commit > self.commit_index:
            old_commit = self.commit_index
            self.commit_index = min(leader_commit, len(self.log))

        # Actualizar el índice global desde el líder
        if leader_index is not None:
            self.global_index = leader_index
            self._log(f"Índice global actualizado desde líder (versión: {leader_index.get('version', 0)})")
        
        #AQUI
        from raft.utils import get_raft_server
        server: RaftServer = get_raft_server()
        if previous_active_nodes is not None:
            server.previous_active_nodes = previous_active_nodes
            previous = ", ".join(server.previous_active_nodes)
            self._log(f"Nodos activos anteriores actualizado desde líder (versión: {leader_index.get('version', 0)}) \nNodos: {previous}")

        states = ""
        if node_states is not None:
            server.node_states = node_states
            for key in server.node_states.keys():
                states += "\n" + key + ": " + node_states[key] 
            self._log(f"Estado de nodos actualizado desde líder (versión: {leader_index.get('version', 0)}) \nEstados: {states}")

        old_commit = self.commit_index
        self._log(f"Commit index actualizado: {old_commit} -> {self.commit_index}")    

        return {"term": self.current_term, "global_index": global_index_copy, "success": True}

    def request_vote(
        self,
        term: int,
        candidate_id: int,
        last_log_index: int,
        last_log_term: int,
    ) -> dict:
        self._log(
            f"RequestVote recibido del candidato {candidate_id}: "
            f"term={term}, last_log_index={last_log_index}, last_log_term={last_log_term}"
        )

        if term < self.current_term:
            self._log(f"Rechazando voto: term {term} < term actual {self.current_term}")
            return {"term": self.current_term, "success": False}

        if term > self.current_term:
            self._log(
                f"Nuevo término descubierto: {term} desde candidato {candidate_id}"
            )
            self.become_follower(new_term=term)
            self.voted_for = None
            self.current_leader_id = None

        if self.voted_for is not None and self.voted_for != candidate_id:
            self._log(
                f"Rechazando voto: ya votó por {self.voted_for} en término {self.current_term}"
            )
            return {"term": self.current_term, "success": False}

        local_last_index = len(self.log)
        local_last_term = self.log[-1].term if self.log else 0
        up_to_date = last_log_term > local_last_term or (
            last_log_term == local_last_term and last_log_index >= local_last_index
        )

        if not up_to_date:
            self._log(
                f"Rechazando voto: log del candidato no está actualizado "
                f"(candidato: term={last_log_term}, index={last_log_index}; "
                f"local: term={local_last_term}, index={local_last_index})"
            )
            return {"term": self.current_term, "success": False}

        self.voted_for = candidate_id
        self._start_timer()
        self._log(
            f"Voto concedido al candidato {candidate_id} en término {self.current_term}"
        )

        return {"term": self.current_term, "success": True}

    # CLIENT INTERACTION

    def client_request(self, command: Any) -> dict:
        self._log(f"Solicitud del cliente recibida: {command}")

        if self.state != "leader":
            self._log(
                f"Rechazando solicitud: no soy líder (líder actual: {self.current_leader_id})"
            )
            return {
                "success": False,
                "leader_id": self.current_leader_id,
                "message": "No soy el líder",
            }
        
        # Revisar si ya existe la misma petición en pending
        with self._lock:
            for task in self.pending_tasks:
                if task["status"] == "pending" and task.get("request_data") == command:
                    # Actualizar solo el term
                    task["request_data"]["term"] = self.current_term
                    self._log(f"Petición duplicada detectada, actualizando term a {self.current_term}")
                    return {
                        "success": True,
                        "message": "Request already pending, term updated",
                        "task_id": task["task_id"]
                    }
            

        entry = LogEntry(term=self.current_term, command=command)
        self.log.append(entry)
        log_index = len(self.log)

        self._log(
            f"Comando del cliente aceptado: {command} (índice del log: {log_index}, term={self.current_term})"
        )

        return {"success": True, "log_index": log_index}
        
    # DB MANAGEMENT 
    def _merge_index_info(self, index: dict):
        from raft.log_utils import log_info, log_warning, log_error
        own = self.global_index
        if not isinstance(index, dict):
            return
        
        # Diccionario para rastrear cambios agregados
        added_changes = {
            "version": None,
            "files": {},
            "files_metadata": {},
            "db_nodes": [],
            "node_versions": {},
            "node_shards": {}
        }
        
        try:
            # version → solo si no existe
            if "version" in index and "version" not in own:
                own["version"] = index["version"]
                added_changes["version"] = index["version"]
            
            # files: {node_id: [files]}
            if "files" in index and isinstance(index["files"], dict):
                for node_id, files in index["files"].items():
                    if node_id not in own["files"]:
                        own["files"][node_id] = list(files)
                        added_changes["files"][node_id] = list(files)
                    else:
                        new_files = []
                        for f in files:
                            if f not in own["files"][node_id]:
                                own["files"][node_id].append(f)
                                new_files.append(f)
                        if new_files:
                            added_changes["files"][node_id] = new_files
            
            # files_metadata
            if "files_metadata" in index and isinstance(index["files_metadata"], dict):
                for filename, meta in index["files_metadata"].items():
                    if filename not in own["files_metadata"]:
                        own["files_metadata"][filename] = meta.copy()
                        added_changes["files_metadata"][filename] = meta.copy()
                    else:
                        new_meta = {}
                        for k, v in meta.items():
                            if k not in own["files_metadata"][filename]:
                                own["files_metadata"][filename][k] = v
                                new_meta[k] = v
                        if new_meta:
                            added_changes["files_metadata"][filename] = new_meta
            
            # db_nodes (set)
            if "db_nodes" in index:
                if isinstance(index["db_nodes"], (set, list, tuple)):
                    new_nodes = set(index["db_nodes"]) - own["db_nodes"]
                    own["db_nodes"].update(index["db_nodes"])
                    if new_nodes:
                        added_changes["db_nodes"] = list(new_nodes)
            
            # node_versions
            if "node_versions" in index and isinstance(index["node_versions"], dict):
                for node_id, versions in index["node_versions"].items():
                    if node_id not in own["node_versions"]:
                        own["node_versions"][node_id] = versions.copy()
                        added_changes["node_versions"][node_id] = versions.copy()
                    else:
                        new_versions = {}
                        for k, v in versions.items():
                            if k not in own["node_versions"][node_id]:
                                own["node_versions"][node_id][k] = v
                                new_versions[k] = v
                        if new_versions:
                            added_changes["node_versions"][node_id] = new_versions
            
            # node_shards
            if "node_shards" in index and isinstance(index["node_shards"], dict):
                for node_id, shard_info in index["node_shards"].items():
                    if node_id not in own["node_shards"]:
                        own["node_shards"][node_id] = shard_info.copy()
                        added_changes["node_shards"][node_id] = shard_info.copy()
                    else:
                        own_shard = own["node_shards"][node_id]
                        new_shard_info = {}
                        for k, v in shard_info.items():
                            if k not in own_shard:
                                own_shard[k] = v
                                new_shard_info[k] = v
                            elif k == "shards" and isinstance(v, dict):
                                new_shards = {}
                                for filename, ranges in v.items():
                                    if filename not in own_shard["shards"]:
                                        own_shard["shards"][filename] = list(ranges)
                                        new_shards[filename] = list(ranges)
                                    else:
                                        new_ranges = []
                                        for r in ranges:
                                            if r not in own_shard["shards"][filename]:
                                                own_shard["shards"][filename].append(r)
                                                new_ranges.append(r)
                                        if new_ranges:
                                            new_shards[filename] = new_ranges
                                if new_shards:
                                    new_shard_info["shards"] = new_shards
                        if new_shard_info:
                            added_changes["node_shards"][node_id] = new_shard_info
            
            log_info("RAFT CONSENSUS FUNCTION - MERGE INDEX", f"Se completo con exito el merge de los indices globales")
            
            # Filtrar cambios vacíos y mostrar solo lo que se agregó
            filtered_changes = {}
            if added_changes["version"] is not None:
                filtered_changes["version"] = added_changes["version"]
            if added_changes["files"]:
                filtered_changes["files"] = added_changes["files"]
            if added_changes["files_metadata"]:
                filtered_changes["files_metadata"] = added_changes["files_metadata"]
            if added_changes["db_nodes"]:
                filtered_changes["db_nodes"] = added_changes["db_nodes"]
            if added_changes["node_versions"]:
                filtered_changes["node_versions"] = added_changes["node_versions"]
            if added_changes["node_shards"]:
                filtered_changes["node_shards"] = added_changes["node_shards"]
            
            if filtered_changes:
                log_info("RAFT CONSENSUS FUNCTION - MERGE INDEX", f"Y los cambios que estaban en el índice del otro y no en el propio ya agregados son: {filtered_changes}")
            else:
                log_info("RAFT CONSENSUS FUNCTION - MERGE INDEX", "No se agregaron cambios nuevos (el índice propio ya contenía toda la información)")
                
        except Exception as e:
            log_error("RAFT CONSENSUS FUNCTION - MERGE INDEX", f"Ha ocurrido un error al mergear los indices: {e}")
            
    def _sync_database_on_become_leader(self):
        """
        Da aviso de sincronización para la base de datos cuando este nodo se convierte en líder.
        En LeaderManager ya encuentra el nodo con la db_version más alta y copia su DB y JSON.
        """
        from raft.db_json_manager import DBJsonManager
        from raft.log_utils import log_info, log_warning
        
        # Info - azul para inicio de sincronización
        log_info("DB SYNC ON BECOME LEADER", f"Iniciando sincronización de base de datos para nuevo líder '{self.node_id}'", 
                colorize_full=True, color="blue")
        
        # Obtener instancias necesarias
        json_manager = DBJsonManager()
        
        # Obtener todos los nodos DB del índice global
        db_nodes = list(self.global_index.get("db_nodes", set()))
        
        if not db_nodes:
            log_info("DB SYNC ON BECOME LEADER", "Primer nodo DB detectado, inicializando estructuras", 
                    colorize_full=True, color="blue")
            
            # Inicializar como primer nodo DB
            self._initialize_as_first_db_node()
            return
        
        # Asumir todos los nodos DB son activos
        active_db_nodes = db_nodes

        # Si el nodo actual era DB entonces no hay que hacer nada más
        if self.node_id in active_db_nodes:
            log_info("DB SYNC ON BECOME LEADER", "Nuevo líder detectado como nodo DB, se deja la sincronización al LeaderManager", 
                    colorize_full=True, color="blue")
            return
        
        # Remover el nodo actual de la lista para hacer el aviso de sincronización
        nodes_to_sync = [node for node in active_db_nodes if node != self.node_id]
        
        log_info("DB SYNC ON BECOME LEADER", 
                f"Nodos DB aparentemente disponibles para sincronización: {nodes_to_sync} (de {len(db_nodes)} totales)", 
                colorize_full=True, color="blue")
        
        if not nodes_to_sync:
            log_warning("DB SYNC ON BECOME LEADER", 
                    "No hay nodos DB activos para sincronización, procediendo como nodo independiente", 
                    colorize_full=True, color="yellow")
            
            # Inicializar sin sincronización
            self._initialize_without_sync(json_manager)
            return
        
        # Si el nodo no es DB para empezar y hay otros nodos DB (sin saber si estan vivos o no) entonces se avisa que hay que actualizarlo al LeaderManager y que este se encargue
        json_manager.ensure_json_exists(become_new_leader=True)
        log_info("DB SYNC ON BECOME LEADER", 
                f"Nuevo nodo líder lleva aviso de sincronización en su json", 
                colorize_full=True, color="blue")
    
    def _initialize_as_first_db_node(self):
        from raft.log_utils import log_info, log_warning, log_error
        """Inicializa el nodo como el primer nodo de base de datos"""
        self.global_index["node_versions"][self.node_id] = {
            "read_version": 0,
            "write_version": 0,
            "db_version": 0,
            "db_version_prev": 0,
            "is_db_node": True
        }
        self.global_index["db_nodes"].add(self.node_id)
        self.global_index["version"] += 1
        
        # Asegurar que existe el JSON
        from raft.db_json_manager import DBJsonManager
        json_manager = DBJsonManager()
        json_manager.ensure_json_exists()
        
        log_info("DB SYNC ON BECOME LEADER", "Nodo líder inicializado como primer nodo de base de datos", 
                colorize_full=True, color="blue")

    def _initialize_without_sync(self, json_manager):
        """Inicializa el nodo sin sincronización"""
        from raft.log_utils import log_info
        # Actualizar versiones de DB desde JSON local
        db_version, db_version_prev = json_manager.get_db_versions()
        
        with self._lock:
            self.global_index["node_versions"][self.node_id] = {
                "read_version": 0,
                "write_version": 0,
                "db_version": db_version,
                "db_version_prev": db_version_prev,
                "is_db_node": True
            }
            self.global_index["db_nodes"].add(self.node_id)
            self.global_index["version"] += 1
        
        log_info("DB SYNC ON BECOME LEADER", 
                f"Nodo líder iniciado como DB sin aviso de sincronización (db_version={db_version})", 
                colorize_full=True, color="blue")

    def update_node_version(self, version_type: str, node_id: int = None):
        """
        Actualiza la versión de lectura o escritura de un nodo.
        version_type: 'read' o 'write'
        """
        if node_id is None:
            node_id = self.node_id
        
        if node_id not in self.global_index["node_versions"]:
            self.global_index["node_versions"][node_id] = {
                "read_version": 0,
                "write_version": 0,
                "db_version": 0,
                "db_version_prev": 0,
                "is_db_node": node_id in self.global_index["db_nodes"]
            }
        
        if version_type == "write":
            if self.global_index["node_versions"][node_id]["is_db_node"]:
                self.global_index["node_versions"][node_id]["write_version"] += 1
        elif version_type == "read":
            self.global_index["node_versions"][node_id]["read_version"] += 1


class RaftServer:
    _lock = threading.Lock()
    _instance = None

    def __new__(cls, *args, **kwargs):
        """
        Se ejecuta antes que __init__. Aquí garantizamos el singleton.
        """
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:  # bloqueo de doble verificación
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
        node_id: int,
        host: str,
        port: int,
        on_become_leader: Optional[Callable] = None,
        on_non_being_leader: Optional[Callable] = None,
    ):
        if getattr(self, "_initialized", False):
            return

        self._initialized = True

        self.node_id = node_id
        self.host = host
        self.port = port

        print(f"[Node {node_id}] Inicializando servidor Raft en {host}:{port}")

        self.raft_instance = RaftConsensusFunctions(
            node_id,
            on_timer_end_callback=self.on_timer_end,
            on_become_leader=on_become_leader,
            on_non_being_leader=on_non_being_leader,
        )

        self.db_instance = DBManager()
        self.storage_instance = StorageManager()

        # Tabla de nodos activos
        self.node_states = {}  # {node_id: "DEAD" | "ALIVE" | "RE-SPAWN" | "NEW"}
        self.previous_active_nodes = set()

        self.raft_instance._log(
            f"Inicializando índices de replicación para {len(self._get_client_nodes())} nodos"
        )
        for client_id, _, _ in self._get_client_nodes():
            self.raft_instance.next_index[client_id] = 1
            self.raft_instance.match_index[client_id] = 0
            self.node_states[client_id] = "NEW"

        self.daemon = rpc.Daemon(host=host, port=port)
        uri = self.daemon.register(self.raft_instance, objectId=f"raft.node.{node_id}")
        uri_db = self.daemon.register(self.db_instance, objectId=f"raft.db.{node_id}")
        uri_storage = self.daemon.register(self.storage_instance, objectId=f"raft.storage.{node_id}")

        self.raft_instance._log(f"Objeto RAFT registrado con URI: {uri}")
        self.raft_instance._log(f"Objeto STORAGE registrado con URI: {uri_storage}")
        self.raft_instance._log(f"Objeto DB registrado con URI: {uri_db}")

        # Iniciar hilo de monitoreo de nodos (heartbeat)

        self.heartbeat_thread = threading.Thread(
            target=self._send_heartbeats, daemon=True
        )
        self.heartbeat_thread.start()
        self.raft_instance._log("Hilo de heartbeat iniciado")

        self.client_proxys: Dict[str, rpc.Proxy] = {}

        print(f"[Node {node_id}] Servidor iniciado en {host}:{port}")

    def start(self):
        # Asegurar que el daemon esté creado antes de usarlo
        if not hasattr(self, "daemon") or self.daemon is None:
            self._init_daemon()

        self.raft_instance._log(f"Iniciando daemon de Pyro5 en {self.host}:{self.port}")
        self.daemon.requestLoop()

    def _init_daemon(self):
        """
        Inicializa el daemon de Pyro5 si no se creó antes.
        """

        self.daemon = rpc.Daemon(host=self.host, port=self.port)

    def _is_node_active(self, client_id: int) -> bool:
        """Verifica si un nodo está activo según node_states"""
        with self._lock:
            return self.node_states.get(client_id) != "DEAD"

    def _get_client_server(
        self, client_id: int, client_host: str, client_port: int, type: str = "node", requires_validation:bool=True
    ) -> Optional[rpc.Proxy]:
        """
        Obtiene un proxy para comunicarse con un nodo.
        Retorna None si el nodo no está activo.
        """
        if requires_validation and not self._is_node_active(client_id):
            self.raft_instance._log(f"Nodo {client_id} no está activo, omitiendo proxy")
            return None

        try:
            uri = f"PYRO:raft.{type}.{client_id}@{client_host}:{client_port}"
            proxy = rpc.Proxy(uri)
            proxy._pyroSerializer = "marshal"
            return proxy
        except Exception as e:
            self.raft_instance._log(f"Error al crear proxy para nodo {client_id}: {e}")
            return None

    def on_timer_end(self):
        self.raft_instance._log(
            f"Iniciando elección para término {self.raft_instance.current_term}"
        )

        total_votes = 1
        required_votes = 1
        self.raft_instance._log(
            f"Elección iniciada: votos requeridos={required_votes}, voto propio contado"
        )

        last_log_index = len(self.raft_instance.log)
        last_log_term = self.raft_instance.log[-1].term if self.raft_instance.log else 0

        for client_node_id, client_host, client_port in self._get_client_nodes():
            try:
                self.raft_instance._log(
                    f"Solicitando voto del nodo {client_node_id} en {client_host}:{client_port}"
                )

                client_server = self._get_client_server(
                    client_node_id, client_host, client_port, requires_validation=False
                )

                if client_server is None:
                    self.raft_instance._log(
                        f"Nodo {client_node_id} no está activo, omitiendo solicitud de voto"
                    )
                    continue

                response = client_server.request_vote(
                    self.raft_instance.current_term,
                    self.raft_instance.node_id,
                    last_log_index,
                    last_log_term,
                )

                required_votes += 1

                if response["term"] > self.raft_instance.current_term:
                    self.raft_instance._log(
                        f"Término superior descubierto {response['term']} desde nodo {client_node_id}, actualizando"
                    )
                    self.raft_instance.current_term = response["term"]
                    return

                if response["success"]:
                    total_votes += 1
                    self.raft_instance._log(
                        f"Voto recibido del nodo {client_node_id} (total: {total_votes})"
                    )
                else:
                    self.raft_instance._log(
                        f"Voto denegado por el nodo {client_node_id}"
                    )

            except Exception as err:
                self.raft_instance._log(
                    f"Error al contactar nodo {client_node_id}: {err}"
                )
                # self._set_node_status(client_node_id, False)

        if self.raft_instance.state != "candidate":
            return

        required_votes = required_votes // 2 + 1

        if total_votes >= required_votes:
            self.raft_instance._log(
                f"¡Elección ganada con {total_votes}/{len(self._get_client_nodes()) + 1} votos!"
            )
            self.raft_instance.become_leader()
        else:
            self.raft_instance._log(
                f"Elección perdida: votos={total_votes}, requeridos={required_votes}"
            )
            self.raft_instance.become_follower()

    def _send_heartbeats(self):
        import random
        
        while True:
            heartbeat_interval = random.uniform(HEARTBEAT_INTERVAL[0], HEARTBEAT_INTERVAL[1])  # ✅ Usar uniform
            time.sleep(heartbeat_interval)
            
            if self.raft_instance.state != "leader":
                continue
                
            # # Detectar cambios en nodos activos
            # self._detect_node_state_changes()
            # self._cleanup_completed_tasks()
            
            for client_node_id, client_host, client_port in self._get_client_nodes():
                threading.Thread(
                    target=self._send_append_entries,
                    args=(client_node_id, client_host, client_port),
                    daemon=True,
                ).start()

    def _send_append_entries(
        self, client_node_id: int, client_host: str, client_port: int
    ):
        if self.raft_instance.state != "leader":
            return

        from raft.log_utils import log_info, log_error, log_warning
        try:
            client_server = self._get_client_server(
                client_node_id, client_host, client_port, requires_validation=False
            )

            if client_server is None:
                # Nodo no está activo, no intentar enviar
                return

            next_idx = self.raft_instance.next_index.get(client_node_id, 1)
            prev_log_index = next_idx - 1
            prev_log_term = (
                self.raft_instance.log[prev_log_index - 1].term
                if prev_log_index > 0
                else 0
            )

            entries = []
            if next_idx <= len(self.raft_instance.log):
                entries = [
                    {"term": e.term, "command": e.command}
                    for e in self.raft_instance.log[next_idx - 1 :]
                ]
                self.raft_instance._log(
                    f"Enviando {len(entries)} entradas al nodo {client_node_id} "
                    f"(prev_log_index={prev_log_index}, next_index={next_idx})"
                )

            current_term = self.raft_instance.current_term
            response = client_server.append_entries(
                current_term,
                self.raft_instance.node_id,
                prev_log_index,
                prev_log_term,
                self.raft_instance.commit_index,
                entries,
                self.raft_instance.global_index,
                self.previous_active_nodes,
                self.node_states
            ) 

            if self.raft_instance.state != "leader":
                return

            if response["global_index"]:
                log_warning("RAFT SERVER", f"Nodo leader: {self.host} recibió global index de nodo {client_node_id}. Merge index...")
                self.raft_instance._merge_index_info(response["global_index"])

            if response["success"]:
                if entries:
                    old_match = self.raft_instance.match_index[client_node_id]
                    self.raft_instance.match_index[client_node_id] = (
                        prev_log_index + len(entries)
                    )
                    self.raft_instance.next_index[client_node_id] = (
                        self.raft_instance.match_index[client_node_id] + 1
                    )
                    self.raft_instance._log(
                        f"Nodo {client_node_id} replicó exitosamente: "
                        f"match_index {old_match} -> {self.raft_instance.match_index[client_node_id]}"
                    )
                elif prev_log_index == 0:
                    self.raft_instance.match_index[client_node_id] = 0
                
                if response["global_index"]:
                    log_warning("RAFT SERVER", f"Nodo leader: {self.host} recibió global index de nodo {client_node_id} tras un heartbeat exitoso donde reescribio el suyo por el del lider. Merge index...")
                    self.raft_instance._merge_index_info(response["global_index"])
            else:
                if response["term"] >= self.raft_instance.current_term:
                    log_warning( "RAFT SERVER",
                        f"Término superior descubierto {response['term']} desde nodo {client_node_id}, renunciando"
                    )
                    import logging
                    res_term = response['term']
                    logging.info(f"Término superior descubierto {res_term} desde nodo {client_node_id}, renunciando")
                    # Copio mi indice, creo un cliente
                    self.raft_instance.become_follower(new_term=response["term"])
                else:
                    old_next = next_idx
                    new_next = max(1, next_idx - 1)
                    self.raft_instance.next_index[client_node_id] = new_next
                    if old_next != new_next:
                        self.raft_instance._log(
                            f"AppendEntries rechazado por nodo {client_node_id}: "
                            f"decrementando next_index {old_next} -> {new_next}"
                        )

        except Exception as e:
            log_error( "RAFT SERVER",
                f"Error al enviar AppendEntries a nodo {client_node_id}: {e}"
            )
            # self._set_node_status(client_node_id, False)

    def _set_node_status(self, node_id: str, val: bool):
        import logging
        try:
            with self._lock:
                val = "ALIVE" if val else "DEAD"
                self.node_states[node_id] = val

        except Exception:
            logging.error(f"[RAFT] Error poniendo estado {val} a nodo {node_id}")

    def _update_commit_index(self):
        if self.raft_instance.state != "leader":
            return

        for n in range(
            len(self.raft_instance.log), self.raft_instance.commit_index, -1
        ):
            if n == 0:
                break

            if self.raft_instance.log[n - 1].term != self.raft_instance.current_term:
                continue

            replicated_count = 1
            for match_idx in self.raft_instance.match_index.values():
                if match_idx >= n:
                    replicated_count += 1

            required = (len(self._get_client_nodes()) + 1) // 2 + 1
            if replicated_count >= required:
                old_commit = self.raft_instance.commit_index
                self.raft_instance.commit_index = n
                self.raft_instance._log(
                    f"Commit index actualizado: {old_commit} -> {n} "
                    f"(replicado en {replicated_count}/{len(self._get_client_nodes()) + 1} nodos)"
                )
                break
    
    # Esto no hace lo que dice que hace
    def _cleanup_completed_tasks(self):
        """Mantiene solo las últimas 5 tareas completed en el log"""
        with self.raft_instance._lock:
            completed = [t for t in self.raft_instance.pending_tasks 
                        if t["status"] == "completed"]
            
            if len(completed) > 5:
                # Mantener solo las últimas 5
                to_keep = completed[-5:]
                to_remove = completed[:-5]
                
                for task in to_remove:
                    self.raft_instance.pending_tasks.remove(task)
                
                self.raft_instance._log(f"Limpiadas {len(to_remove)} tareas completed antiguas")

    def _get_client_nodes(self):
        clients = [
            (node.ip, node.hostname, self.port)
            for node in get_service_tasks()
            if node.ip != self.host
        ]

        return clients
        