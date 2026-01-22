# authentication.py
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework.exceptions import AuthenticationFailed
from rest_framework import status
import json
import copy
import logging


class UserIdJWTAuthentication(JWTAuthentication):
    """
    Extensión de JWTAuthentication que inyecta el ID del usuario autenticado
    en las peticiones:
    - Operaciones de escritura (POST, PUT, PATCH): en el body JSON
    - Operaciones de lectura (GET): como parámetro de query 'user'
    """
    
    def authenticate(self, request):
        # Primero, autenticar usando el método padre
        auth_result = super().authenticate(request)
        
        if auth_result is not None:
            user, token = auth_result
            
            # Si la autenticación fue exitosa, modificar la request
            self._inject_user_id_to_request(request, user)
            
            return user, token
        
        return None
    
    def _inject_user_id_to_request(self, request, user):
        """
        Inyecta el ID del usuario en la request según el método HTTP.
        """
        method = request.method.upper()
        user_id = str(user.id)  # Convertir a string para consistencia
        
        if method in ['POST', 'PUT', 'PATCH']:
            # Operaciones de escritura: inyectar en el body JSON
            self._inject_user_to_body(request, user_id)
            
        elif method == 'GET':
            # Operaciones de lectura: inyectar como parámetro de query
            self._inject_user_to_query_params(request, user_id)
        
        # Para DELETE, decidir según tu lógica de negocio
        # Puedes agregar más métodos según necesites
    
    def _inject_user_to_body(self, request, user_id):
        """
        Inyecta el user_id en el body JSON de la request.
        Maneja diferentes formatos de contenido.
        """
        content_type = request.content_type or ''
        
        if 'application/json' in content_type and request.body:
            try:
                # Parsear el body JSON
                body_data = json.loads(request.body)
                
                # Crear una copia modificada del body
                modified_body = copy.deepcopy(body_data)
                
                # Añadir el user_id al body
                # Nota: Si ya existe 'user', lo sobrescribimos
                modified_body['user'] = user_id
                
                # Convertir de nuevo a JSON y actualizar request
                request._body = json.dumps(modified_body).encode('utf-8')
                
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                # Si el body no es JSON válido, no hacemos nada
                # pero podrías lanzar una excepción si lo prefieres
                pass
        elif not request.body:
            # Si no hay body (por ejemplo, POST vacío), crear uno
            request._body = json.dumps({'user': user_id}).encode('utf-8')
    
    def _inject_user_to_query_params(self, request, user_id):
        """
        Inyecta el user_id como parámetro de query.
        """
        # Obtener una copia mutable de los query params
        query_params = request.GET.copy()
        
        # Añadir o sobrescribir el parámetro 'user'
        query_params['user'] = user_id
        
        # Actualizar request.GET (esto es un poco hacky pero funciona)
        request.GET = query_params
        
        # También actualizar request.query_params para DRF
        request._request.GET = query_params
        
        # Reconstruir la URL completa con el nuevo parámetro
        # Esto es útil si la request se reenvía posteriormente
        self._reconstruct_full_path(request)
    
    def _reconstruct_full_path(self, request):
        """
        Reconstruye el full_path con los nuevos query parameters.
        """
        # Construir la nueva cadena de query
        query_string = request.GET.urlencode()
        
        # Obtener la ruta sin query parameters
        path = request.path_info
        
        # Reconstruir el full path
        if query_string:
            request.META['QUERY_STRING'] = query_string
            request.META['REQUEST_URI'] = f"{path}?{query_string}"
            request._current_scheme_host = None  # Invalidar caché
        else:
            request.META['QUERY_STRING'] = ''
            request.META['REQUEST_URI'] = path
