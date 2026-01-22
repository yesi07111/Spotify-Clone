# code/app/views.py
import uuid
import logging
import hashlib


from rest_framework import viewsets, status
from rest_framework.views import APIView
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import AllowAny, IsAuthenticated

from .decorators import leader_only
from .models import Artist, Album, Track
from .serializers import (
    ArtistSerializer,
    AlbumSerializer,
    AudioStreamerSerializer,
    TrackSerializer,
)

from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView
from rest_framework_simplejwt.tokens import RefreshToken
from django.utils import timezone
from django.core.mail import send_mail
from django.conf import settings

from .decorators import leader_only
from .models import User
from .serializers import (
    CustomTokenObtainPairSerializer,
    RegisterSerializer,
    UserSerializer,
    ChangePasswordSerializer,
    VerifyEmailSerializer,
    PasswordResetRequestSerializer,
    PasswordResetConfirmSerializer
)

logger = logging.getLogger(__name__)

# @leader_only
class CustomTokenObtainPairView(TokenObtainPairView):
    serializer_class = CustomTokenObtainPairSerializer
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from raft.utils import get_leader_manager
        self.leader_manager = get_leader_manager()

# @leader_only
class CustomTokenRefreshView(TokenRefreshView):
    def post(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        
        try:
            serializer.is_valid(raise_exception=True)
        except Exception as e:
            return Response(
                {"error": "Token de refresh inválido o expirado"},
                status=status.HTTP_401_UNAUTHORIZED
            )
        
        # Verificar versión del refresh token
        refresh_token = request.data.get('refresh')
        if refresh_token:
            try:
                token = RefreshToken(refresh_token)
                user_id = token['user_id']
                token_version = token.get('token_version', 1)
                
                user = User.objects.get(id=user_id)
                if user.refresh_token_version != token_version:
                    return Response(
                        {"error": "Token de refresh ha sido invalidado"},
                        status=status.HTTP_401_UNAUTHORIZED
                    )
            except Exception as e:
                logger.error(f"Error verificando versión de token: {e}")
        
        return Response(serializer.validated_data, status=status.HTTP_200_OK)

# @leader_only
class RegisterView(APIView):
    permission_classes = [AllowAny]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from raft.utils import get_leader_manager
        self.leader_manager = get_leader_manager()
    
    def post(self, request):
        serializer = RegisterSerializer(data=request.data)
        
        if serializer.is_valid():
            try:
                # ✅ NO guardar todavía, solo validar
                validated_data = serializer.validated_data
                logger.info(f"[REGISTER] Datos validados: {validated_data.get('username')}")
                
                # ✅ Crear instancia de usuario SIN guardar en BD
                validated_data["id"] = str(uuid.uuid4())

                user = User(**validated_data)
                user.set_password(validated_data['password'])  # Hashear contraseña
                
                if User.objects.filter(username=user.username).exists() or (validated_data.get("email") and validated_data["email"] is not None and validated_data["email"] != "" and User.objects.filter(email=user.email).exists()):
                    return Response({"error": "Ya existe un usuario con esas credenciales"},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                # ✅ Coordinar creación distribuida (esto creará el usuario en la BD)
                result = self.leader_manager.manage_metadata(user, "create")
                
                if not result.get("success", False):
                    return Response(
                        {"error": "Error en la creación distribuida del usuario"},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR
                    )
                
                # ✅ Recuperar usuario creado por el sistema distribuido
                # Necesitamos obtener el ID asignado
                try:
                    user_created = User.objects.get(username=validated_data['username'])
                except User.DoesNotExist:
                    return Response(
                        {"error": "Usuario no encontrado después de creación"},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR
                    )
                
                # Generar tokens JWT
                refresh = RefreshToken.for_user(user_created)
                refresh['token_version'] = user_created.refresh_token_version
                
                send_user = UserSerializer(user_created).data
                # Esto es solo para mostrar en frontend, no afecta la BD
                import random
                send_user["id"] = "USER_" + str(random.randint(1_000_000_000, 9_999_999_999))
                
                response_data = {
                    'user': send_user,
                    'access': str(refresh.access_token),
                    'refresh': str(refresh),
                    'message': 'Usuario registrado exitosamente.'
                }
                
                return Response(response_data, status=status.HTTP_201_CREATED)
                
            except Exception as e:
                logger.error(f"Error en registro: {e}", exc_info=True)
                return Response(
                    {"error": f"Error en el registro: {str(e)}"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

# @leader_only
class LogoutView(APIView):
    permission_classes = [IsAuthenticated]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from raft.utils import get_leader_manager
        self.leader_manager = get_leader_manager()
    
    def post(self, request):
        try:
            refresh_token = request.data.get("refresh")
            
            if refresh_token:
                try:
                    token = RefreshToken(refresh_token)
                    token.blacklist()
                except Exception as e:
                    logger.warning(f"Error blacklisting token: {e}")
            
            # Invalidar todos los refresh tokens del usuario
            request.user.invalidate_refresh_tokens()
            
            # Coordinar invalidación distribuida
            self.leader_manager.manage_metadata(request.user, "update")
            
            return Response(
                {"message": "Sesión cerrada exitosamente"},
                status=status.HTTP_200_OK
            )
        except Exception as e:
            logger.error(f"Error en logout: {e}")
            return Response(
                {"error": "Error al cerrar sesión"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

# @leader_only
class ProfileView(APIView):
    permission_classes = [IsAuthenticated]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from raft.utils import get_leader_manager
        self.leader_manager = get_leader_manager()
    
    def get(self, request):
        """Obtener perfil del usuario actual"""
        try:
            # Leer desde nodo óptimo
            query_data = {
                "model": "user",
                "filters": {"id": str(request.user.id)}
            }
            
            data = self.leader_manager.read_metadata(query_data)
            
            if data:
                return Response(data[0], status=status.HTTP_200_OK)
            else:
                return Response(
                    {"error": "Usuario no encontrado"},
                    status=status.HTTP_404_NOT_FOUND
                )
                
        except Exception as e:
            logger.error(f"Error obteniendo perfil: {e}")
            return Response(
                {"error": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    def patch(self, request):
        """Actualizar perfil del usuario"""
        serializer = UserSerializer(
            request.user,
            data=request.data,
            partial=True
        )
        
        if serializer.is_valid():
            try:
                # Actualizar instancia sin guardar
                for attr, value in serializer.validated_data.items():
                    setattr(request.user, attr, value)
                
                # Coordinar actualización distribuida
                result = self.leader_manager.manage_metadata(request.user, "update")
                
                return Response(
                    UserSerializer(request.user).data,
                    status=status.HTTP_200_OK
                )
                
            except Exception as e:
                logger.error(f"Error actualizando perfil: {e}")
                return Response(
                    {"error": "Error en actualización distribuida"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

# @leader_only
class ChangePasswordView(APIView):
    permission_classes = [IsAuthenticated]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from raft.utils import get_leader_manager
        self.leader_manager = get_leader_manager()
    
    def post(self, request):
        serializer = ChangePasswordSerializer(data=request.data)
        
        if serializer.is_valid():
            try:
                # ✅ Recibimos TEXTO PLANO del frontend
                current_password = serializer.validated_data['current_password']
                new_password = serializer.validated_data['new_password']
                
                logger.info(f"[CHANGE PASSWORD] Usuario: {request.user.username}")
                logger.info(f"[CHANGE PASSWORD] Current password length: {len(current_password)}")
                logger.info(f"[CHANGE PASSWORD] New password length: {len(new_password)}")
                logger.info(f"[CHANGE PASSWORD] Password en BD (primeros 20): {request.user.password[:20]}")
                
                # ✅ Verificar contraseña actual (texto plano vs pbkdf2)
                password_check = request.user.check_password(current_password)
                logger.info(f"[CHANGE PASSWORD] check_password result: {password_check}")
                
                if not password_check:
                    logger.warning(f"[CHANGE PASSWORD] Contraseña actual incorrecta para {request.user.username}")
                    return Response(
                        {"current_password": ["Contraseña actual incorrecta"]},
                        status=status.HTTP_400_BAD_REQUEST
                    )
                
                # ✅ NO guardar todavía - solo modificar en memoria
                request.user.set_password(new_password)
                request.user.invalidate_refresh_tokens()
                
                logger.info(f"[CHANGE PASSWORD] Nueva contraseña seteada (primeros 20): {request.user.password[:20]}")
                logger.info(f"[CHANGE PASSWORD] Iniciando manage_metadata...")
                
                # ✅ Coordinar actualización distribuida (esto guardará)
                result = self.leader_manager.manage_metadata(request.user, "update")
                
                if not result:
                    logger.error(f"[CHANGE PASSWORD] manage_metadata falló")
                    return Response(
                        {"error": "Error en actualización distribuida"},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR
                    )
                
                logger.info(f"[CHANGE PASSWORD] Contraseña cambiada exitosamente para {request.user.username}")
                
                return Response(
                    {"message": "Contraseña cambiada exitosamente"},
                    status=status.HTTP_200_OK
                )
                
            except Exception as e:
                logger.error(f"[CHANGE PASSWORD] Error completo: {e}", exc_info=True)
                import traceback
                logger.error(f"[CHANGE PASSWORD] Traceback: {traceback.format_exc()}")
                return Response(
                    {"error": f"Error al cambiar contraseña: {str(e)}"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
        
        logger.error(f"[CHANGE PASSWORD] Serializer errors: {serializer.errors}")
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    
# @leader_only
class VerifyEmailView(APIView):
    permission_classes = [AllowAny]
    
    def post(self, request):
        serializer = VerifyEmailSerializer(data=request.data)
        
        if serializer.is_valid():
            try:
                token = serializer.validated_data['token']
                
                # Buscar usuario con este token
                import hashlib
                token_hash = hashlib.sha256(token.encode()).hexdigest()
                
                user = User.objects.filter(
                    verification_token=token_hash,
                    verification_token_expires__gt=timezone.now()
                ).first()
                
                if user:
                    user.is_verified = True
                    user.verification_token = ''
                    user.verification_token_expires = None
                    user.save()
                    
                    # Coordinar actualización distribuida
                    from raft.utils import get_leader_manager
                    leader_manager = get_leader_manager()
                    leader_manager.manage_metadata(user, "update")
                    
                    return Response(
                        {"message": "Email verificado exitosamente"},
                        status=status.HTTP_200_OK
                    )
                else:
                    return Response(
                        {"error": "Token de verificación inválido o expirado"},
                        status=status.HTTP_400_BAD_REQUEST
                    )
                    
            except Exception as e:
                logger.error(f"Error verificando email: {e}")
                return Response(
                    {"error": "Error al verificar email"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

# @leader_only
class PasswordResetRequestView(APIView):
    permission_classes = [AllowAny]
    
    def post(self, request):
        serializer = PasswordResetRequestSerializer(data=request.data)
        
        if serializer.is_valid():
            try:
                email = serializer.validated_data['email']
                
                # Buscar usuario por email
                user = User.objects.filter(email=email).first()
                
                if user:
                    # Generar token de reset
                    token = user.generate_verification_token()
                    
                    # En proyecto universitario, solo logueamos el token
                    logger.info(f"Token de reset de contraseña para {email}: {token}")
                    
                    # En producción enviaríamos un email:
                    # send_mail(
                    #     'Reset de Contraseña - Spotify Clone',
                    #     f'Tu token de reset es: {token}',
                    #     'noreply@spotifyclone.com',
                    #     [email],
                    #     fail_silently=False,
                    # )
                
                # Siempre retornamos éxito por seguridad
                return Response(
                    {"message": "Si el email existe, recibirás instrucciones para resetear tu contraseña"},
                    status=status.HTTP_200_OK
                )
                
            except Exception as e:
                logger.error(f"Error solicitando reset de contraseña: {e}")
                return Response(
                    {"error": "Error al procesar la solicitud"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

# @leader_only
class PasswordResetConfirmView(APIView):
    permission_classes = [AllowAny]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from raft.utils import get_leader_manager
        self.leader_manager = get_leader_manager()
    
    def post(self, request):
        serializer = PasswordResetConfirmSerializer(data=request.data)
        
        if serializer.is_valid():
            try:
                token = serializer.validated_data['token']
                new_password_hash = serializer.validated_data['new_password']
                
                # Buscar usuario por token
                import hashlib
                token_hash = hashlib.sha256(token.encode()).hexdigest()
                
                user = User.objects.filter(
                    verification_token=token_hash,
                    verification_token_expires__gt=timezone.now()
                ).first()
                
                if user:
                    # Cambiar contraseña
                    from django.contrib.auth.hashers import make_password
                    user.password = make_password(new_password_hash)
                    user.verification_token = ''
                    user.verification_token_expires = None
                    user.invalidate_refresh_tokens()
                    
                    # Coordinar actualización distribuida
                    self.leader_manager.manage_metadata(user, "update")
                    
                    return Response(
                        {"message": "Contraseña restablecida exitosamente"},
                        status=status.HTTP_200_OK
                    )
                else:
                    return Response(
                        {"error": "Token de reset inválido o expirado"},
                        status=status.HTTP_400_BAD_REQUEST
                    )
                    
            except Exception as e:
                logger.error(f"Error confirmando reset de contraseña: {e}")
                return Response(
                    {"error": "Error al restablecer contraseña"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

# @leader_only
class DeleteAccountView(APIView):
    permission_classes = [IsAuthenticated]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from raft.utils import get_leader_manager
        self.leader_manager = get_leader_manager()
    
    def delete(self, request):
        try:
            user = request.user
            user_id = user.id
            
            # ✅ Coordinar eliminación distribuida ANTES de invalidar tokens
            self.leader_manager.manage_metadata(user, "delete")
            
            # Invalidar todos los tokens
            user.invalidate_refresh_tokens()
            
            logger.info(f"[DELETE ACCOUNT] Cuenta {user.username} eliminada exitosamente")
            
            user.delete()
            return Response(
                {"message": "Cuenta eliminada exitosamente"},
                status=status.HTTP_200_OK
            )
            
        except Exception as e:
            logger.error(f"Error eliminando cuenta: {e}", exc_info=True)
            return Response(
                {"error": f"Error al eliminar la cuenta: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        
# # Añadir estas vistas al UserViewSet si se quiere manejar usuarios como API REST
class UserViewSet(viewsets.ModelViewSet):
    queryset = User.objects.all()
    serializer_class = UserSerializer
    lookup_field = "id"
    permission_classes = [IsAuthenticated]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from raft.utils import get_leader_manager
        self.leader_manager = get_leader_manager()
    
    @action(detail=False, methods=['get'])
    def me(self, request):
        serializer = self.get_serializer(request.user)
        return Response(serializer.data)
    
    @action(detail=False, methods=['post'])
    def change_password(self, request):
        view = ChangePasswordView()
        view.request = request
        view.format_kwarg = self.format_kwarg
        return view.post(request)

# @leader_only
class AudioStreamerView(APIView):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from raft.utils import get_leader_manager
        self.leader_manager = get_leader_manager()

    def get(self, request):
        try:
            user = request.user
            if not user.is_authenticated:
                return Response(
                    {"error": "Usuario no autenticado"},
                    status=status.HTTP_401_UNAUTHORIZED
                )
            
            query_params = {
                "chunk_index": int(request.GET.get("chunk_index", 0)),
                "chunk_count": int(request.GET.get("chunk_count", 1)),
                "audio_id": request.GET.get("audio_id", ""),
                "include_metadata": request.GET.get("include_metadata", "false").lower() == "true",
            }

            # Verificar que el track pertenezca al usuario
            try:
                track = Track.objects.get(id=query_params["audio_id"], user=user)
            except Track.DoesNotExist:
                return Response(
                    {"error": "Track no encontrado o no tienes permisos"},
                    status=status.HTTP_404_NOT_FOUND
                )

            serializer = AudioStreamerSerializer(data=query_params)
            serializer.is_valid(raise_exception=True)

            response = serializer.handle_request(
                self.leader_manager,
                serializer.validated_data,
            )

            return Response(response, status=status.HTTP_200_OK)

        except Exception as e:
            return Response(
                {"error": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
        
# @leader_only
class ArtistViewSet(viewsets.ModelViewSet):
    queryset = Artist.objects.all()
    serializer_class = ArtistSerializer
    lookup_field = "id"
    permission_classes = [IsAuthenticated]  # Cambiado de AllowAny a IsAuthenticated

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from raft.leader_manager import LeaderManager
        from raft.utils import get_leader_manager
        self.leader_manager: LeaderManager = get_leader_manager()

    def get_queryset(self):
        """Filtrar artistas por usuario actual"""
        user = self.request.user
        if user.is_authenticated:
            queryset = Artist.objects.filter(user=user)
            name = self.request.query_params.get("name", None)
            if name is not None:
                queryset = queryset.filter(name__icontains=name)
            return queryset
        return Artist.objects.none()
    
    def list(self, request, *args, **kwargs):
        """GET - Lectura distribuida de metadatos filtrada por usuario"""
        try:
            user = request.user
            if not user.is_authenticated:
                return Response(
                    {"error": "Usuario no autenticado"},
                    status=status.HTTP_401_UNAUTHORIZED
                )
            
            query_data = {
                "model": "artist",
                "filters": {"user_id": str(user.id)}
            }
            
            name = request.query_params.get("name")
            if name:
                query_data["filters"]["name__icontains"] = name
            
            # Leer desde nodo óptimo
            data = self.leader_manager.read_metadata(query_data)
            
            return Response(data, status=status.HTTP_200_OK)
        
        except Exception as e:
            return Response(
                {"error": str(e)}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    def create(self, request, *args, **kwargs):
        """POST - Escritura distribuida con usuario"""
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        
        try:
            # Crear instancia sin guardar
            artist_data = serializer.validated_data.copy()
            artist_data['user'] = request.user  # Asignar usuario
            
            artist = Artist(**artist_data)
            import logging
            if not artist.id:
                artist.id = str(uuid.uuid4())
            
            # Coordinar escritura distribuida
            result = self.leader_manager.manage_metadata(artist, "create")

            if not result:
                return Response({"error": "No se pudo crear el artista por problemas de servidor"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
            return Response(result, status=status.HTTP_201_CREATED)
        
        except Exception as e:
            return Response(
                {"error": str(e)}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        
    def destroy(self, request, *args, **kwargs):
        """
        DELETE - Elimina un artista usando escritura distribuida (2PC)
        """
        try:
            user = request.user
            if not user.is_authenticated:
                return Response(
                    {"error": "Usuario no autenticado"},
                    status=status.HTTP_401_UNAUTHORIZED
                )

            artist_id = kwargs["id"]

            # Verificar que el artista existe y pertenece al usuario
            artist_data = self.leader_manager.read_metadata({
                "model": "artist",
                "filters": {"id": artist_id, "user_id": str(user.id)}
            })

            if not artist_data:
                return Response(
                    {"error": "Artist no encontrado"},
                    status=status.HTTP_404_NOT_FOUND
                )

            # Crear objeto Artist mínimo para delete
            artist = Artist(id=artist_id, user=user, name=artist_data[0]["name"])

            # Eliminar metadata con 2PC
            delete_result = self.leader_manager.manage_metadata(
                artist, "delete"
            )

            return Response(
                {
                    "success": True,
                    "message": "Artista eliminado correctamente",
                    "data": delete_result.get("data")
                },
                status=status.HTTP_200_OK
            )

        except Exception as e:
            import traceback
            return Response(
                {
                    "error": str(e),
                    "traceback": traceback.format_exc()
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


# @leader_only
class AlbumViewSet(viewsets.ModelViewSet):
    queryset = Album.objects.all()
    serializer_class = AlbumSerializer
    lookup_field = "id"
    permission_classes = [IsAuthenticated]  # Cambiado a IsAuthenticated

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from raft.utils import get_leader_manager
        from raft.leader_manager import LeaderManager
        self.leader_manager: LeaderManager = get_leader_manager()

    def get_queryset(self):
        """Filtrar álbumes por usuario actual"""
        user = self.request.user
        if user.is_authenticated:
            queryset = Album.objects.filter(user=user)
            name = self.request.query_params.get("name")
            if name:
                queryset = queryset.filter(name__icontains=name)
            return queryset
        return Album.objects.none()

    def list(self, request, *args, **kwargs):
        try:
            user = request.user
            if not user.is_authenticated:
                return Response(
                    {"error": "Usuario no autenticado"},
                    status=status.HTTP_401_UNAUTHORIZED
                )
            
            query_data = {
                "model": "album",
                "filters": {"user_id": str(user.id)}
            }
            
            name = request.query_params.get("name")
            if name:
                query_data["filters"]["name__icontains"] = name

            # Leer desde nodo óptimo
            data = self.leader_manager.read_metadata(query_data)

            return Response(data, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def retrieve(self, request, *args, **kwargs):
        try:
            user = request.user
            if not user.is_authenticated:
                return Response(
                    {"error": "Usuario no autenticado"},
                    status=status.HTTP_401_UNAUTHORIZED
                )
            
            data = self.leader_manager.read_metadata({
                "model": "album",
                "filters": {"id": kwargs["id"], "user_id": str(user.id)}
            })
            if not data:
                return Response({"error": "Album no encontrado"}, status=status.HTTP_404_NOT_FOUND)

            return Response(data[0], status=status.HTTP_200_OK)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        try:
            validated = serializer.validated_data.copy()
            validated['user'] = request.user  # Asignar usuario
            
            if "id" not in validated:
                validated["id"] = str(uuid.uuid4())

            obj = Album(**validated)
            result = self.leader_manager.manage_metadata(obj, "create")

            if not result:
                return Response({"error": "No se pudo crear el album por problemas de servidor"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            return Response(result, status=status.HTTP_201_CREATED)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
    def destroy(self, request, *args, **kwargs):
        """
        DELETE - Elimina un álbum usando escritura distribuida (2PC)
        """
        try:
            user = request.user
            if not user.is_authenticated:
                return Response(
                    {"error": "Usuario no autenticado"},
                    status=status.HTTP_401_UNAUTHORIZED
                )

            album_id = kwargs["id"]

            # Verificar que el álbum existe y pertenece al usuario
            album_data = self.leader_manager.read_metadata({
                "model": "album",
                "filters": {"id": album_id, "user_id": str(user.id)}
            })

            if not album_data:
                return Response(
                    {"error": "Album no encontrado"},
                    status=status.HTTP_404_NOT_FOUND
                )

            # Crear objeto Album mínimo para delete
            album = Album(id=album_id, user=user, name=album_data[0]["name"])

            # Eliminar metadata con 2PC
            delete_result = self.leader_manager.manage_metadata(
                album, "delete"
            )

            return Response(
                {
                    "success": True,
                    "message": "Álbum eliminado correctamente",
                    "data": delete_result.get("data")
                },
                status=status.HTTP_200_OK
            )

        except Exception as e:
            import traceback
            return Response(
                {
                    "error": str(e),
                    "traceback": traceback.format_exc()
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


# @leader_only
class TrackViewSet(viewsets.ModelViewSet):
    queryset = Track.objects.all()
    serializer_class = TrackSerializer
    lookup_field = "id"
    permission_classes = [IsAuthenticated]  # Cambiado a IsAuthenticated

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from raft.utils import get_leader_manager
        from raft.leader_manager import LeaderManager
        self.leader_manager: LeaderManager = get_leader_manager()

    def get_queryset(self):
        """Filtrar tracks por usuario actual"""
        user = self.request.user
        if user.is_authenticated:
            queryset = Track.objects.filter(user=user)

            title = self.request.query_params.get("title")
            if title:
                queryset = queryset.filter(title__icontains=title)

            artist_ids = self.request.query_params.getlist("artist[]")
            if artist_ids:
                queryset = queryset.filter(artist__id__in=artist_ids)

            album_id = self.request.query_params.get("album")
            if album_id:
                queryset = queryset.filter(album__id=album_id)

            return queryset
        return Track.objects.none()

    def list(self, request, *args, **kwargs):
        try:
            user = request.user
            if not user.is_authenticated:
                return Response(
                    {"error": "Usuario no autenticado"},
                    status=status.HTTP_401_UNAUTHORIZED
                )
            
            filters = {"user_id": str(user.id)}

            if "title" in request.query_params:
                filters["title__icontains"] = request.query_params["title"]

            artist_ids = request.query_params.getlist("artist[]")
            if artist_ids:
                filters["artist__id__in"] = artist_ids

            album_id = request.query_params.get("album")
            if album_id:
                filters["album__id"] = album_id

            data = self.leader_manager.read_metadata({
                "model": "track",
                "filters": filters
            })

            complete_data = []
            for track_data in data:
                artist_names_arr = []
                artist_names = ""

                artist_ids = track_data.get("artist", [])
                if artist_ids:
                    artists = self.leader_manager.read_metadata({
                        "model": "artist",
                        "filters": {
                            "id__in": artist_ids,
                            "user_id": str(user.id)
                        }
                    })
                    artist_names_arr = [a["name"] for a in artists]
                    artist_names = ", ".join(artist_names_arr)

                track_data["artist_names"] = artist_names

                album_name = ""
                album_id = track_data.get("album")

                if album_id:
                    album_data = self.leader_manager.read_metadata({
                        "model": "album",
                        "filters": {
                            "id": album_id,
                            "user_id": str(user.id)
                        }
                    })
                    if album_data:
                        album_name = album_data[0]["name"]

                track_data["album_name"] = album_name
                complete_data.append(track_data)


            return Response(data, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def retrieve(self, request, *args, **kwargs):
        try:
            user = request.user
            if not user.is_authenticated:
                return Response(
                    {"error": "Usuario no autenticado"},
                    status=status.HTTP_401_UNAUTHORIZED
                )
            
            track_id = kwargs["id"]

            data = self.leader_manager.read_metadata({
                "model": "track",
                "filters": {"id": track_id, "user_id": str(user.id)}
            })

            if not data:
                return Response({"error": "Audio no encontrado"}, status=status.HTTP_404_NOT_FOUND)

            track_data = data[0]

            artist_names_arr = []
            artist_names = ""

            artist_ids = track_data.get("artist", [])
            if artist_ids:
                artists = self.leader_manager.read_metadata({
                    "model": "artist",
                    "filters": {
                        "id__in": artist_ids,
                        "user_id": str(user.id)
                    }
                })
                artist_names_arr = [a["name"] for a in artists]
                artist_names = ", ".join(artist_names_arr)

            track_data["artist_names"] = artist_names

            album_name = ""
            album_id = track_data.get("album")

            if album_id:
                album_data = self.leader_manager.read_metadata({
                    "model": "album",
                    "filters": {
                        "id": album_id,
                        "user_id": str(user.id)
                    }
                })
                if album_data:
                    album_name = album_data[0]["name"]

            track_data["album_name"] = album_name

            # include_audio
            include_audio = request.query_params.get("include_audio", "false").lower() == "true"

            if include_audio:
                chunk_index = int(request.query_params.get("chunk_index", 0))
                chunk_count = int(request.query_params.get("chunk_count", 10))

                chunks = self.leader_manager.read_file_chunks(track_id, chunk_index, chunk_count)

                import base64
                track_data["audio_chunks"] = [base64.b64encode(c).decode() for c in chunks]
                track_data["chunk_index"] = chunk_index
                track_data["chunk_count"] = len(chunks)
                # track_data["artist_names"] =  
                # track_data["album_name"] = 

            return Response(track_data, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        import base64
        
        try:
            user = request.user
            if not user.is_authenticated:
                return Response(
                    {"error": "Usuario no autenticado"},
                    status=status.HTTP_401_UNAUTHORIZED
                )
            
            file_base64 = request.data.get("file_base64")
            if not file_base64:
                return Response({"error": "file_base64 es campo requerido"}, status=status.HTTP_400_BAD_REQUEST)
            
            file_data = base64.b64decode(file_base64)
            
            # Obtener metadata del audio
            temp_serializer = TrackSerializer()
            meta = temp_serializer.get_audio_info(file_data)
            
            # Obtener datos validados
            track_data = serializer.validated_data
            
            # EXTRAER file_base64 si está en validated_data
            track_data.pop('file_base64', None)
            
            # EXTRAER album_name y artist_names si están
            track_data.pop('album_name', None)
            track_data.pop('artist_names', None)
            
            # EXTRAER LOS ARTISTAS ANTES de crear el objeto Track
            artists_data = track_data.pop('artist', [])
            
            # Convertir a lista de IDs
            artist_ids = [artist.id for artist in artists_data]
            
            # Agregar metadata del audio
            track_data["duration_seconds"] = meta.duration_seconds
            track_data["bitrate"] = meta.bitrate
            track_data["extension"] = meta.extension
            
            # ASIGNAR USUARIO
            track_data["user"] = user
            
            # **NUEVO: Generar ID como hash(archivo + user_id)**
            # Calcular hash SHA256 del archivo
            file_hash = hashlib.sha256(file_data).hexdigest()
            
            # Combinar hash del archivo con user_id
            combined_string = f"{file_hash}_{str(user.id)}"
            track_id = hashlib.sha256(combined_string.encode()).hexdigest()

            # Comprobar si ya existe una canción con ese id (se subió el mismo archivo)
            exist = self.leader_manager.read_metadata({
                "model": "track",
                "filters": {"id": track_id, "user_id": str(user.id)}
            }) 

            if exist:
                return Response({"error": "Ese archivo de audio ya fue subido."}, status=status.HTTP_201_CREATED)
            
            # Asignar ID generado
            track_data["id"] = track_id
            
            # Crear el Track SIN el campo artist (ManyToMany)
            track = Track(**track_data)
            
            # Agregar los artist_ids como atributo temporal
            track._artist_ids = artist_ids
            
            # 1. Primero escribir el archivo distribuido
            manage_file = self.leader_manager.manage_file(track.id, file_data, operation="create", real_name=track.title)
            
            # 2. Luego escribir metadata con 2PC
            write_meta = self.leader_manager.manage_metadata(track, "create")

            if not write_meta:
                return Response({"error": "No se pudo crear la canción por problemas de servidor"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
            return Response(
                {
                    "success": True,
                    "data": write_meta.get("data"),
                    "file_distribution": manage_file.get("distribution"),
                    "track_id": track.id  # Retornar ID generado
                },
                status=status.HTTP_201_CREATED
            )
            
        except Exception as e:
            import traceback
            return Response(
                {
                    "error": str(e),
                    "traceback": traceback.format_exc()
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    def update(self, request, *args, **kwargs):
        """
        Actualiza un track usando LeaderManager. Funciona para PUT y PATCH.
        """
        import logging
        from app.models import Track

        try:
            user = request.user

            if not user.is_authenticated:
                return Response(
                    {"error": "Usuario no autenticado"},
                    status=status.HTTP_401_UNAUTHORIZED
                )

            track_id = kwargs.get("id")
            if not track_id:
                return Response({"error": "ID de track requerido"}, status=status.HTTP_400_BAD_REQUEST)

            # Obtener metadata actual del track
            existing_data = self.leader_manager.read_metadata({
                "model": "track",
                "filters": {"id": track_id, "user_id": str(user.id)}
            })

            if not existing_data:
                return Response({"error": "Track no encontrado"}, status=status.HTTP_404_NOT_FOUND)

            track_instance_dict = existing_data[0]

            # Convertir dict a objeto Track usando _deserialize_to_object
            track_instance = self.leader_manager.raft_server.db_instance._deserialize_to_object(
                track_instance_dict, "track"
            )

            # Validar datos entrantes con serializer
            serializer = self.get_serializer(data=request.data, partial=True)  # partial=True permite PATCH
            serializer.is_valid(raise_exception=True)
            validated_data = serializer.validated_data

            # No permitir cambiar ciertos campos sensibles
            validated_data.pop("file_base64", None)
            validated_data.pop("user", None)
            validated_data.pop("id", None)

            # Extraer artistas si vienen
            artist_data = validated_data.pop("artist", None)
            if artist_data:
                artist_ids = [a.id for a in artist_data]
                setattr(track_instance, "_artist_ids", artist_ids)

            # Actualizar los campos permitidos en el objeto Track
            for k, v in validated_data.items():
                setattr(track_instance, k, v)

            # Llamar a LeaderManager para escribir metadata (update)
            update_result = self.leader_manager.manage_metadata(track_instance, "update")

            if not update_result:
                return Response({"error": "No se pudo actualizar la canción por problemas de servidor"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            return Response({
                "success": True,
                "data": update_result.get("data")
            }, status=status.HTTP_200_OK)

        except Exception as e:
            import traceback
            return Response({
                "error": str(e),
                "traceback": traceback.format_exc()
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def destroy(self, request, *args, **kwargs):
        """
        Elimina un track completo (metadatos + archivo de audio)
        """
        try:
            user = request.user
            if not user.is_authenticated:
                return Response(
                    {"error": "Usuario no autenticado"},
                    status=status.HTTP_401_UNAUTHORIZED
                )
            
            track_id = kwargs["id"]

            # Verificar que el track existe y pertenece al usuario
            track_data = self.leader_manager.read_metadata({
                "model": "track",
                "filters": {"id": track_id, "user_id": str(user.id)}
            })

            if not track_data:
                return Response(
                    {"error": "Track no encontrado"},
                    status=status.HTTP_404_NOT_FOUND
                )

            # Deserializar el track completo en lugar de crear uno vacío
            track = self.leader_manager.raft_server.db_instance._deserialize_to_object(
                track_data[0], "track"
            )

            # 1. Eliminar archivo de audio distribuido
            delete_file_result = self.leader_manager.manage_file(track_id, None, operation="delete", real_name=track.title)
            
            # 2. Eliminar metadatos con 2PC
            delete_meta_result = self.leader_manager.manage_metadata(track, "delete")
            
            return Response(
                {
                    "success": True,
                    "message": "Track eliminado correctamente",
                    "file_deletion": delete_file_result,
                    "metadata_deletion": delete_meta_result
                },
                status=status.HTTP_200_OK
            )
            
        except Exception as e:
            import traceback
            return Response(
                {
                    "error": str(e),
                    "traceback": traceback.format_exc()
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        
    # Para soportar PATCH explícitamente
    def partial_update(self, request, *args, **kwargs):
        return self.update(request, *args, **kwargs)
    




