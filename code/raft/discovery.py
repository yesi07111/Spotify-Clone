# discovery.py
from dataclasses import dataclass
from typing import List, Optional
import dns.resolver
import dns.reversename
import docker
import socket
import time

# CONFIG
DEFAULT_NETWORK = "teamnet"
DEFAULT_TCP_PORT_RANGE = range(30000, 30010)  # 10 puertos fijos


@dataclass
class ContainerInfo:
    ip: str
    hostname: Optional[str] = None
    port: Optional[int] = None


def _discover_by_alias_dns(
    alias: str = "spotify_cluster",
    retries: int = 3,
) -> List[ContainerInfo]:

    for attempt in range(retries):
        try:
            _, _, ips = socket.gethostbyname_ex(alias)

            valid_ips = [
                ip for ip in ips
                if ip and ip != "127.0.0.1"
            ]

            return [
                ContainerInfo(ip=ip, hostname=ip)
                for ip in valid_ips
            ]

        except socket.gaierror:
            import logging
            logging.error(f"[Attempt {attempt+1} of {retries}]No se encontraron IP's para el alias {alias}")

            if attempt == retries - 1:
                return []

    return []


def get_service_tasks(service_name: str = "spotify_clone") -> List[ContainerInfo]:
    """
    Intenta descubrir contenedores usando:
    1. DNS con nombres fijos (backend_X_#, frontend_X_#)
    2. Docker Engine API como fallback

    Mantiene el mismo formato original:
    Lista de ContainerInfo(ip=..., hostname=...)
    """

    # 1. Intentar DNS discovery con nombres fijos
    try:
        dns_containers = _discover_by_alias_dns()
        if dns_containers:
            return dns_containers
    except Exception:
        import logging

        logging.info("[Discover] No se encontro otros nodos por DNS")
        pass

    raise RuntimeError("No se pudo descubrir nodos usando DNS ni Docker API")


def discover_active_clients(service_name: str = "spotify_clone") -> List[str]:
    """
    Mantiene la firma y retorno original:
    Devuelve lista de IPs.
    """
    containers = get_service_tasks(service_name)
    return [c.ip for c in containers]
