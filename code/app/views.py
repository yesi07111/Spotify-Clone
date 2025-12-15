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

@leader_only
class CustomTokenObtainPairView(TokenObtainPairView):
    serializer_class = CustomTokenObtainPairSerializer
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from raft.utils import get_leader_manager
        self.leader_manager = get_leader_manager()

@leader_only
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

@leader_only
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
                # ✅ USAR EL SERIALIZER PARA CREAR EL USUARIO
                user = serializer.save()  # Esto ejecuta RegisterSerializer.create()
                
                logger.info(f"[REGISTER] Usuario creado por serializer: {user.username}")
                logger.info(f"[REGISTER] Password después de create: {user.password[:80]}...")
                
                # Coordinar escritura distribuida (si es necesario)
                result = self.leader_manager.write_metadata(user, "create")
                
                # Generar tokens JWT
                refresh = RefreshToken.for_user(user)
                refresh['token_version'] = user.refresh_token_version
                
                response_data = {
                    'user': UserSerializer(user).data,
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
@leader_only
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
            self.leader_manager.write_metadata(request.user, "update")
            
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

@leader_only
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
                result = self.leader_manager.write_metadata(request.user, "update")
                
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

@leader_only
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
                # Verificar contraseña actual
                current_password_hash = serializer.validated_data['current_password']
                new_password_hash = serializer.validated_data['new_password']
                
                # El frontend envía hash SHA256, aplicamos verificación
                if not request.user.check_password(current_password_hash):
                    return Response(
                        {"current_password": ["Contraseña actual incorrecta"]},
                        status=status.HTTP_400_BAD_REQUEST
                    )
                
                # Cambiar contraseña
                from django.contrib.auth.hashers import make_password
                request.user.password = make_password(new_password_hash)
                request.user.invalidate_refresh_tokens()  # Invalidar tokens existentes
                
                # Coordinar actualización distribuida
                self.leader_manager.write_metadata(request.user, "update")
                
                return Response(
                    {"message": "Contraseña cambiada exitosamente"},
                    status=status.HTTP_200_OK
                )
                
            except Exception as e:
                logger.error(f"Error cambiando contraseña: {e}")
                return Response(
                    {"error": "Error al cambiar contraseña"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

@leader_only
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
                    leader_manager.write_metadata(user, "update")
                    
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

@leader_only
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

@leader_only
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
                    self.leader_manager.write_metadata(user, "update")
                    
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

@leader_only
class DeleteAccountView(APIView):
    permission_classes = [IsAuthenticated]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from raft.utils import get_leader_manager
        self.leader_manager = get_leader_manager()
    
    def delete(self, request):
        try:
            user = request.user
            
            # Coordinar eliminación distribuida
            self.leader_manager.write_metadata(user, "delete")
            
            # Invalidar todos los tokens
            user.invalidate_refresh_tokens()
            
            # En un sistema real, podríamos marcar como inactivo en lugar de eliminar
            # user.is_active = False
            # user.save()
            
            return Response(
                {"message": "Cuenta eliminada exitosamente"},
                status=status.HTTP_200_OK
            )
            
        except Exception as e:
            logger.error(f"Error eliminando cuenta: {e}")
            return Response(
                {"error": "Error al eliminar la cuenta"},
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

@leader_only
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
        
@leader_only
class ArtistViewSet(viewsets.ModelViewSet):
    queryset = Artist.objects.all()
    serializer_class = ArtistSerializer
    lookup_field = "id"
    permission_classes = [IsAuthenticated]  # Cambiado de AllowAny a IsAuthenticated

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from raft.utils import get_leader_manager
        self.leader_manager = get_leader_manager()

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
                logging.info("\nArtistView - create - Asignando nuevo UUID al artista")
                artist.id = str(uuid.uuid4())
            
            logging.info("\nArtistView - create - A punto de llamar al write_metadata")
            # Coordinar escritura distribuida
            result = self.leader_manager.write_metadata(artist, "create")
            
            return Response(result, status=status.HTTP_201_CREATED)
        
        except Exception as e:
            return Response(
                {"error": str(e)}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
@leader_only
class AlbumViewSet(viewsets.ModelViewSet):
    queryset = Album.objects.all()
    serializer_class = AlbumSerializer
    lookup_field = "id"
    permission_classes = [IsAuthenticated]  # Cambiado a IsAuthenticated

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from raft.utils import get_leader_manager
        self.leader_manager = get_leader_manager()

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

            import logging
            logging.info(f"\nAlbumView - list")
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
                return Response({"error": "Album not found"}, status=status.HTTP_404_NOT_FOUND)

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
            import logging
            logging.info("\nAlbumView - create - Antes de llamar a write_metadata")
            result = self.leader_manager.write_metadata(obj, "create")

            return Response(result, status=status.HTTP_201_CREATED)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        

@leader_only
class TrackViewSet(viewsets.ModelViewSet):
    queryset = Track.objects.all()
    serializer_class = TrackSerializer
    lookup_field = "id"
    permission_classes = [IsAuthenticated]  # Cambiado a IsAuthenticated

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from raft.utils import get_leader_manager
        self.leader_manager = get_leader_manager()

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
                return Response({"error": "Track not found"}, status=status.HTTP_404_NOT_FOUND)

            track_data = data[0]

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
                return Response({"error": "file_base64 is required"}, status=status.HTTP_400_BAD_REQUEST)
            
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
            
            # Asignar ID generado
            track_data["id"] = track_id
            
            # Crear el Track SIN el campo artist (ManyToMany)
            track = Track(**track_data)
            
            # Agregar los artist_ids como atributo temporal
            track._artist_ids = artist_ids
            
            # 1. Primero escribir el archivo distribuido
            filename = f"{track.id}.{track.extension}"
            import logging 
            logging.info(f"\nTrackView - create - Antes de llamar a write_file con ID generado: {track.id}")
            write_file = self.leader_manager.write_file(filename, file_data)
            
            # 2. Luego escribir metadata con 2PC
            write_meta = self.leader_manager.write_metadata(track, "create")
            
            return Response(
                {
                    "success": True,
                    "data": write_meta.get("data"),
                    "file_distribution": write_file.get("distribution"),
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

