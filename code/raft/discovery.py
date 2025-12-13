# discovery.py

from dataclasses import dataclass

from typing import List, Optional

import dns.resolver

import dns.reversename

import docker

import socket



# CONFIG

DEFAULT_NETWORK = "teamnet"

DEFAULT_TCP_PORT_RANGE = range(30000, 30010)  # 10 puertos fijos





@dataclass

class ContainerInfo:

    ip: str

    hostname: Optional[str] = None

    port: Optional[int] = None 





# ============================================================

#   MÉTODO 1: DNS DISCOVERY (primero)

# ============================================================



def _discover_by_dns_fixed_names() -> List[ContainerInfo]:

    """

    Intentar descubrir contenedores usando nombres DNS fijos:

    backend_A_1, backend_A_2, ..., frontend_B_10, etc.

    Solo devuelve los que respondan.

    """

    import logging

    PREFIXES = ["backend_A_", "backend_B_", "frontend_A_", "frontend_B_"]



    found = []



    for prefix in PREFIXES:

        for i in range(1, 11):

            hostname = f"{prefix}{i}"

            if hostname == "backend_A_1":

                logging.info(f"[Discover] Intentando encontrar ip para contenedor de nombre {hostname}")

            try:

                answers = dns.resolver.resolve(hostname, "A")

                for ans in answers:

                    ip = ans.address

                    found.append(ContainerInfo(ip=ip, hostname=hostname))

            except Exception:

                if hostname == "backend_A_1":

                    logging.info(f"[Discover] No encontro el ip para ese hostname: {hostname}")

                continue  # no existe ese contenedor en DNS



    return found





# ============================================================

#   MÉTODO 2: FALLBACK DOCKER ENGINE API

# ============================================================



def _discover_by_docker_api(network_name: str = DEFAULT_NETWORK) -> List[ContainerInfo]:

    """

    Descubre contenedores en la red overlay mediante Docker Engine API,

    sin depender del DNS de Docker.

    """



    client = docker.from_env()



    # En docker el hostname == container_id corto

    try:

        this_container_id = socket.gethostname()

        this_container = client.containers.get(this_container_id)

    except Exception:

        raise RuntimeError("Este contenedor no puede acceder a la API de Docker")



    if network_name not in this_container.attrs["NetworkSettings"]["Networks"]:

        raise RuntimeError(f"El contenedor actual no está en la red '{network_name}'")



    net = client.networks.get(network_name)

    containers = []



    for info in net.attrs["Containers"].values():

        ip = info["IPv4Address"].split("/")[0]

        name = info["Name"]

        containers.append(ContainerInfo(ip=ip, hostname=name))



    return containers





# ============================================================

#   MÉTODO ORIGINAL: get_service_tasks

#   NO SE MODIFICA SU FIRMA NI SU RETORNO

#   Solo se modifica CÓMO obtiene la información.

# ============================================================



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

        dns_containers = _discover_by_dns_fixed_names()

        if dns_containers:

            return dns_containers

    except Exception:

        import logging

        logging.info("[Discover] No se encontro otros nodos por DNS")

        pass



    # # 2. FALLBACK — Docker API

    # try:

    #     api_containers = _discover_by_docker_api()

    #     if api_containers:

    #         return api_containers

    # except Exception:

    #     pass



    # Si no hay nada, se comporta igual que antes: error DNS

    raise RuntimeError(f"No se pudo descubrir nodos usando DNS ni Docker API")





# ============================================================

#   MÉTODO ORIGINAL: discover_active_clients

#   NO SE MODIFICA SU FIRMA NI SU RETORNO

# ============================================================



def discover_active_clients(service_name: str = "spotify_clone") -> List[str]:

    """

    Mantiene la firma y retorno original:

    Devuelve lista de IPs.

    """

    containers = get_service_tasks(service_name)

    return [c.ip for c in containers]

