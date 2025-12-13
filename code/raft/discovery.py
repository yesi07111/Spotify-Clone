#discovery.py
from dataclasses import dataclass
from typing import List, Optional
import dns.resolver
import dns.reversename


@dataclass
class ContainerInfo:
    ip: str
    hostname: Optional[str] = None


def get_service_tasks(service_name: str = "spotify_clone") -> List[ContainerInfo]:
    """
    Devuelve una lista de diccionarios con:
    - 'ip': IP de cada contenedor del servicio
    - 'hostname': nombre DNS del contenedor (si está disponible)

    service_name → nombre del servicio en Docker Swarm

    Requiere dnspython instalado.
    """
    tasks_record = f"tasks.{service_name}"

    try:
        answers = dns.resolver.resolve(tasks_record, "A")
    except Exception as e:
        raise RuntimeError(f"No se pudo resolver DNS: {tasks_record}") from e

    containers = []

    for ans in answers:
        ip = ans.address
        hostname = None

        # Intentar resolver nombre DNS inverso
        try:
            rev_name = dns.reversename.from_address(ip)
            rev_answers = dns.resolver.resolve(rev_name, "PTR")
            hostname = str(rev_answers[0]).rstrip(".")
        except Exception:
            hostname = None  # Puede fallar si Docker no lo expone

        containers.append(ContainerInfo(ip=ip, hostname=hostname))

    return containers


def discover_active_clients(service_name: str = "spotify_clone") -> List[str]:
    """
    Descubre las IPs de los contenedores activos de un servicio Docker Swarm.

    service_name → nombre del servicio en Docker Swarm
    """
    containers = get_service_tasks(service_name)
    return [container.ip for container in containers]
