#db_json_manager.py
import json
import os
import threading
from typing import Optional, Dict, Any
from backend.settings import BASE_DIR

DB_JSON_PATH = os.path.join(BASE_DIR, "db_node_state.json")

class DBJsonManager:
    """Gestiona el archivo JSON local para nodos DB"""
    
    def __init__(self):
        self.lock = threading.Lock()
        # self._ensure_json_exists()
    
    def ensure_json_exists(self):
        """Crea el JSON si no existe"""
        if not os.path.exists(DB_JSON_PATH):
            from raft.utils import get_raft_instance
            raft = get_raft_instance()
            
            initial_data = {
                "term": raft.current_term if raft else 0,
                "node_id": raft.node_id if raft else None,
                "db_version": 0,
                "db_version_prev": 0,
                "log": []
            }
            
            with open(DB_JSON_PATH, 'w') as f:
                json.dump(initial_data, f, indent=2)
    
    def read(self) -> Dict[str, Any]:
        """Lee el contenido del JSON"""
        with self.lock:
            with open(DB_JSON_PATH, 'r') as f:
                return json.load(f)
    
    def write(self, data: Dict[str, Any]):
        """Escribe el contenido del JSON"""
        with self.lock:
            with open(DB_JSON_PATH, 'w') as f:
                json.dump(data, f, indent=2)
    
    def add_operation(self, term: int, task_id: str, sql_operation: str):
        """Agrega una operación al log como pending"""
        data = self.read()
        
        # Buscar si ya existe
        for op in data["log"]:
            if op["task_id"] == task_id:
                op["status"] = "pending"
                op["term"] = term
                self.write(data)
                return
        
        # Agregar nueva
        data["log"].append({
            "term": term,
            "task_id": task_id,
            "sql_operation": sql_operation,
            "status": "pending"
        })

        self.update_json_term(term)

        from raft.utils import get_raft_instance
        raft = get_raft_instance()

        if raft and raft.node_id:
            self.update_node_id(raft.node_id)
        
        self.write(data)
    
    def mark_completed(self, task_id: str):
        """Marca una operación como completed"""
        data = self.read()
        
        for op in data["log"]:
            if op["task_id"] == task_id:
                op["status"] = "completed"
                break
        
        self.write(data)
    
    def update_db_version_on_commit(self):
        """Actualiza versiones tras commit exitoso"""
        data = self.read()
        data["db_version_prev"] = data["db_version"]
        data["db_version"] = data["db_version"] + 1
        self.write(data)
    
    def update_json_term(self, new_term):
        data = self.read()
        data["term"] = new_term
        self.write(data)

    def update_node_id(self, node_id):
        data = self.read()
        data["node_id"] = node_id
        self.write(data)

    def get_db_versions(self) -> tuple:
        """Retorna (db_version, db_version_prev)"""
        data = self.read()
        return data["db_version"], data["db_version_prev"]
    
    def get_pending_operations(self) -> list:
        """Retorna operaciones en estado pending"""
        data = self.read()
        return [op for op in data["log"] if op["status"] == "pending"]
    
    def get_completed_operations(self) -> list:
        """Retorna operaciones en estado completed"""
        data = self.read()
        return [op for op in data["log"] if op["status"] == "completed"]
    
    def get_all_operations(self) -> list:
        """Retorna operaciones en estado completed"""
        data = self.read()
        return [op for op in data["log"]]
    
    def exists(self) -> bool:
        """Verifica si el JSON existe (indica que es nodo DB)"""
        return os.path.exists(DB_JSON_PATH)
    
    def copy_from_remote(self, remote_json_data: Dict[str, Any]):
        """Copia el JSON desde un nodo remoto"""
        self.write(remote_json_data)
    
    def get_last_5_completed(self) -> list:
        """Retorna las últimas 5 operaciones completed"""
        data = self.read()
        completed = [op for op in data["log"] if op["status"] == "completed"]
        return completed[-5:]
    
    def get_last_operation(self) -> Optional[Dict[str, Any]]:
        """Retorna la última operación del log (o None si está vacío)"""
        data = self.read()
        if data["log"]:
            return data["log"][-1]
        return None
    
    def get_last_completed_operation(self) -> Optional[Dict[str, Any]]:
        """Retorna la última operación completed del log (o None si está vacía)"""
        data = self.read()
        completed_ops = [op for op in data["log"] if op.get("status") == "completed"]
        if completed_ops:
            return completed_ops[-1]
        return None