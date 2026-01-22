# middleware.py
import logging
import requests
import time
from django.http import HttpResponse
from django.utils.deprecation import MiddlewareMixin

from raft.utils import am_i_leader, get_leader_id

logger = logging.getLogger(__name__)


class LeaderRedirectMiddleware(MiddlewareMixin):
    """
    Middleware que redirige las peticiones al nodo líder del cluster.
    Si este nodo es el líder, procesa la petición normalmente.
    Si no es el líder, redirige la petición al líder y retorna su respuesta.
    """
    
    def process_view(self, request, view_func, view_args, view_kwargs):
        """
        Intercepta la petición antes de que llegue a la vista.
        Retorna None para continuar con el procesamiento normal,
        o retorna una HttpResponse para interrumpir el flujo.
        """
        logger.debug(f"[LeaderMiddleware] Processing {request.method} {request.get_full_path()}")
        
        # 1. Si este nodo es el líder -> continuar procesamiento normal
        if am_i_leader():
            logger.debug("[LeaderMiddleware] This node IS leader → processing normally")
            return None  # Continuar con el flujo normal
        
        logger.debug("[LeaderMiddleware] This node is NOT leader → redirecting to leader")
        
        # 2. Obtener IP del líder
        leader_ip = get_leader_id()
        start = time.time()
        
        # Esperar por el líder con timeout
        while leader_ip == -1:
            time.sleep(0.5)
            leader_ip = get_leader_id()
            if leader_ip != -1:
                break
            
            # Timeout de 7 segundos
            if time.time() - start >= 7:
                break
        
        if not leader_ip or leader_ip == -1:
            logger.error("[LeaderMiddleware] No leader available")
            return HttpResponse("No leader available", status=503)
        
        # 3. Construir URL del líder
        leader_url = f"http://{leader_ip}:8000{request.get_full_path()}"
        logger.info(f"[LeaderMiddleware] Redirecting to leader at {leader_url}")
        
        # 4. Reenviar la petición al líder
        return self._forward_to_leader(request, leader_url)
    
    def _forward_to_leader(self, request, leader_url):
        """Reenvía la petición al nodo líder y retorna su respuesta."""
        try:
            # Logging de detalles de la petición
            logger.debug(
                f"[LeaderMiddleware] Forwarding request:"
                f"\n  method={request.method}"
                f"\n  headers={self._filtered_headers(request)}"
                f"\n  params={dict(request.GET)}"
                f"\n  body={request.body[:200]!r}..."
                if request.body
                else "[no body]"
            )
            
            # Reenviar petición al líder
            response = requests.request(
                method=request.method,
                url=leader_url,
                headers=self._filtered_headers(request),
                data=request.body or None,
                params=request.GET,
                timeout=5,
            )
            
            logger.debug(f"[LeaderMiddleware] Leader responded with status={response.status_code}")
            
            # Crear respuesta Django con el contenido del líder
            django_response = HttpResponse(
                content=response.content,
                status=response.status_code
            )
            
            # Copiar headers válidos del líder
            for header, value in response.headers.items():
                header_lower = header.lower()
                # Excluir headers relacionados con la transferencia
                if header_lower not in [
                    'transfer-encoding',
                    'content-encoding',
                    'connection',
                    'content-length',  # Django lo manejará automáticamente
                ]:
                    django_response[header] = value
            
            return django_response
            
        except requests.Timeout:
            logger.error("[LeaderMiddleware] Timeout connecting to leader")
            return HttpResponse("Leader timeout", status=504)
        except requests.ConnectionError:
            logger.error("[LeaderMiddleware] Connection error to leader")
            return HttpResponse("Cannot connect to leader", status=502)
        except Exception as e:
            logger.exception(f"[LeaderMiddleware] Error forwarding to leader: {e}")
            return HttpResponse(f"Leader redirect error: {str(e)}", status=502)
    
    def _filtered_headers(self, request):
        """Filtra headers que no deben ser reenviados al líder."""
        excluded = {
            'host',
            'connection',
            'keep-alive',
            'proxy-authenticate',
            'proxy-authorization',
            'te',
            'trailers',
            'transfer-encoding',
            'upgrade',
            'content-length',  # Django/requests lo manejarán
        }
        
        headers = {}
        for k, v in request.headers.items():
            if k.lower() not in excluded:
                headers[k] = v
        
        logger.debug(f"[LeaderMiddleware] Filtered headers: {headers}")
        return headers