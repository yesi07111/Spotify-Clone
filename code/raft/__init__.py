import socket
import threading
import logging
import dns

logger = logging.getLogger(__name__)

MAX_NODES = 10


def get_my_ip():
    """
    Obtiene la IP real del contenedor dentro del cluster Swarm/overlay.
    No usa tasks.<service>. Se basa en discovery por DNS fijo y Docker API.
    """

    from .discovery import discover_active_clients

    # 1. Obtener IPs de todos los contenedores detectados por los métodos robustos
    cluster_ips = discover_active_clients()

    if not cluster_ips:
        raise RuntimeError("No se pudo descubrir otros contenedores para determinar mi propia IP")

    # 2. Obtener todas las IPs locales del contenedor (hostname + interfaces docker)
    hostname = socket.gethostname()

    local_ips = set()
    try:
        addrs = socket.getaddrinfo(hostname, None)
        for addr in addrs:
            local_ips.add(addr[4][0])
    except Exception:
        pass

    # Interfaces adicionales (por si Swarm crea más)
    try:
        for iface in socket.if_nameindex():
            name = iface[1]
            try:
                for fam, _, _, _, sockaddr in socket.getaddrinfo(None, 0, proto=socket.IPPROTO_TCP):
                    if sockaddr:
                        local_ips.add(sockaddr[0])
            except Exception:
                continue
    except Exception:
        pass

    # 3. Buscar coincidencia entre IPs descubiertas y las IPs locales
    for ip in cluster_ips:
        if ip in local_ips:
            return ip

    raise RuntimeError(
        f"No se pudo determinar cuál de las IP {cluster_ips} pertenece a este contenedor. "
        f"IPs locales detectadas: {local_ips}"
    )


# def get_my_port():
#     """
#     Asigna un puerto único y estable dentro del rango DEFAULT_TCP_PORT_RANGE.
#     El cálculo se basa en el hostname del contenedor (docker container ID corto).
#     """

#     from .discovery import DEFAULT_TCP_PORT_RANGE

#     hostname = socket.gethostname()

#     # Convertir hostname a número estable
#     h = abs(hash(hostname))

#     # Seleccionar puerto dentro del rango
#     ports = list(DEFAULT_TCP_PORT_RANGE)
#     port = ports[h % len(ports)]

#     return port

def find_free_internal_port(base=5001, limit=5010):
    import socket
    for port in range(base, limit+1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("0.0.0.0", port))
                return port
            except OSError:
                continue
    raise RuntimeError("No hay puertos internos disponibles")


def initialize_node():
    from .leader_manager import LeaderManager
    from .raft import RaftServer
    from .utils import (
        set_raft_instance,
        set_raft_server,
        set_leader_manager,
    )

    host = get_my_ip()
    port = 5000 #find_free_internal_port()   # AHORA USAMOS PUERTO DINÁMICO Y ÚNICO

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
    leader_manager.set_raft(raft)

    # 4) Guardar globales accesibles para las vistas
    set_raft_server(raft)
    set_raft_instance(raft.raft_instance)
    set_leader_manager(leader_manager)

    # 5) Iniciar hilo Raft
    thread = threading.Thread(target=raft.start, daemon=True)
    thread.start()

    logger.info("Nodo Raft y LeaderManager iniciados correctamente.")
    return raft, leader_manager
