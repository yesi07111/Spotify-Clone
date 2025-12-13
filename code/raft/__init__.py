#__init__.py
import socket
import threading

import logging

import dns

logger = logging.getLogger(__name__)

MAX_NODES = 10
COMMON_PORT = 5000  # todos usan el mismo puerto


def get_my_ip():
    ips = [a.address for a in dns.resolver.resolve("tasks.spotify_clone", "A")]
    hostname = socket.gethostname()
    local_ips = socket.getaddrinfo(hostname, None)
    local_ips = {addr[4][0] for addr in local_ips}

    for ip in ips:
        if ip in local_ips:
            return ip


def initialize_node():
    from .leader_manager import LeaderManager
    from .raft import RaftServer
    from .utils import (
        set_raft_instance,
        set_raft_server,
        set_leader_manager,
    )

    host = get_my_ip()
    port = COMMON_PORT

    logger.info(f"Inicializando nodo {host} en {host}:{port}")

    # 1) Crear LeaderManager primero PERO sin depender (todavía) del Raft real
    leader_manager = LeaderManager(host, port)

    # 2) Crear RaftServer con callbacks
    raft = RaftServer(
        node_id=host,
        host=host,
        port=port,
        on_become_leader=leader_manager.start,
        on_non_being_leader=leader_manager.stop,
    )

    # 3) Interconectar objetos
    leader_manager.raft = raft.raft_instance
    leader_manager.raft_server = raft

    # 4) Guardar globales accesibles para las vistas
    set_raft_server(raft)
    set_raft_instance(raft.raft_instance)
    set_leader_manager(leader_manager)

    # 5) Iniciar hilo Raft
    thread = threading.Thread(target=raft.start, daemon=True)
    thread.start()

    logger.info("Nodo Raft y LeaderManager iniciados correctamente.")
    return raft, leader_manager
