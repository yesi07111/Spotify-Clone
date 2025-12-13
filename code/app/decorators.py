import logging
import requests
from functools import wraps
from django.http import HttpResponse

from raft.utils import am_i_leader, get_leader_id
import time 

logger = logging.getLogger(__name__)


def leader_only(cls):
    """
    Decorador aplicado a una clase APIView o ViewSet.
    Decora automáticamente los métodos CRUD para redirigir al líder.
    """

    CRUD_METHODS = {
        "get",
        "post",
        "put",
        "patch",
        "delete",
        "list",
        "retrieve",
        "create",
        "update",
        "partial_update",
        "destroy",
    }

    logger.info(f"[leader_only] Decorating class {cls.__name__}")

    for attr_name in CRUD_METHODS:
        if hasattr(cls, attr_name):
            original = getattr(cls, attr_name)
            wrapped = _wrap_with_leader_redirect(original)
            setattr(cls, attr_name, wrapped)
            logger.debug(
                f"[leader_only] Wrapped method {cls.__name__}.{attr_name} with leader redirect"
            )

    return cls


def _wrap_with_leader_redirect(view_func):
    @wraps(view_func)
    def wrapper(self, request, *args, **kwargs):
        logger.debug(
            f"[leader_redirect] Received {request.method} {request.get_full_path()}"
        )

        # 1. Si este nodo es el líder -> ejecutar normalmente
        if am_i_leader():
            logger.debug("[leader_redirect] This node IS leader → executing locally")
            return view_func(self, request, *args, **kwargs)

        logger.debug("[leader_redirect] This node is NOT leader → redirecting")

        # 2. Obtener IP del líder
        leader_ip = get_leader_id()
        start = time.time()
        while leader_ip == -1:
            time.sleep(0.5)
            leader_ip = get_leader_id()
            if leader_ip != -1:
                break

            now = time.time()
            if now - start >= 7:
                break

        if not leader_ip or leader_ip == -1:
            logger.error(
                "[leader_redirect] No leader available (get_leader_id returned None)"
            )
            return HttpResponse("No leader available", status=503)

        leader_url = f"http://{leader_ip}:8000{request.get_full_path()}"
        logger.info(f"[leader_redirect] Redirecting request to leader at {leader_url}")

        try:
            # Logging detalles de la petición reenviada
            logger.debug(
                f"[leader_redirect] Forwarding request:"
                f"\n  method={request.method}"
                f"\n  headers={_filtered_headers(request)}"
                f"\n  params={dict(request.GET)}"
                f"\n  body={request.body[:200]!r}..."
                if request.body
                else "[no body]"
            )

            # 3. Reenviar petición al líder
            response = requests.request(
                method=request.method,
                url=leader_url,
                headers=_filtered_headers(request),
                data=request.body or None,
                params=request.GET,
                timeout=5,
            )

            logger.debug(
                f"[leader_redirect] Leader responded with status={response.status_code}"
            )

            django_response = HttpResponse(
                content=response.content, status=response.status_code
            )

            # Copiar headers válidos
            for k, v in response.headers.items():
                if k.lower() not in [
                    "transfer-encoding",
                    "content-encoding",
                    "connection",
                ]:
                    django_response[k] = v

            return django_response

        except Exception as e:
            logger.exception(f"[leader_redirect] Error forwarding to leader: {e}")
            return HttpResponse(f"Leader redirect error: {str(e)}", status=502)

    return wrapper


def _filtered_headers(request):
    excluded = {
        "host",
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
    }

    headers = {}
    for k, v in request.headers.items():
        if k.lower() not in excluded:
            headers[k] = v

    logger.debug(f"[leader_redirect] Filtered headers: {headers}")
    return headers
