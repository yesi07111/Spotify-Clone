#utils.py

RAFT_INSTANCE = None
RAFT_SERVER = None

LEADER_MANAGER = None

def set_leader_manager(lm):
    global LEADER_MANAGER
    LEADER_MANAGER = lm

def get_leader_manager():
    return LEADER_MANAGER

def set_raft_instance(instance):
    global RAFT_INSTANCE
    RAFT_INSTANCE = instance

def set_raft_server(server):
    global RAFT_SERVER
    RAFT_SERVER = server

def get_raft_instance():
    return RAFT_INSTANCE

def get_raft_server():
    return RAFT_SERVER

def get_current_term() -> int:
    raft = get_raft_instance()
    return raft.current_term


def next_term() -> int:
    raft = get_raft_instance()
    if raft.state != "leader":
        raise Exception("Solo puede actualizar el termino el lider")
    raft.next_term()
    return raft.current_term


def am_i_leader() -> bool:
    raft = get_raft_instance()
    if raft is None:
        import logging
        logging.warning("get_raft_instance() returned None in am_i_leader()")
        return False
    return raft.state == "leader"


def get_leader_id() -> str:
    raft = get_raft_instance()
    if raft is None:
        return -1
    return raft.current_leader_id

def get_db_nodes() -> list:
    """
    Retorna la lista de nodos que contienen la base de datos de metadatos.
    """
    raft = get_raft_instance()
    return list(raft.global_index.get("db_nodes", set()))


def is_db_node(node_id: str = None) -> bool:
    """
    Verifica si un nodo es nodo de base de datos.
    Si no se proporciona node_id, verifica el nodo actual.
    """
    raft = get_raft_instance()
    if node_id is None:
        raft_server = get_raft_server()
        node_id = raft_server.host
    
    return node_id in raft.global_index.get("db_nodes", set())


def update_db_version(version_type: str):
    """
    Actualiza la versión de lectura o escritura del nodo actual.
    version_type: 'read' o 'write'
    """
    raft = get_raft_instance()
    raft.update_node_version(version_type)
