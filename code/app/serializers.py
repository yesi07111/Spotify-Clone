# code/app/serializers.py
import io
import os
import uuid
import base64

from pydub import AudioSegment
from dataclasses import dataclass
from rest_framework import serializers
from .models import Artist, Album, Track
from backend.settings import CHUNK_SIZE
# Añade al archivo serializers.py
import uuid
import hashlib
from rest_framework import serializers
from rest_framework.validators import UniqueValidator
from django.contrib.auth import authenticate
from django.utils import timezone
from .models import User
from rest_framework_simplejwt.tokens import RefreshToken
from rest_framework_simplejwt.serializers import TokenObtainPairSerializer

class CustomTokenObtainPairSerializer(TokenObtainPairSerializer):
    def validate(self, attrs):
        import logging
        logger = logging.getLogger(__name__)
        
        username = attrs.get('username')
        password = attrs.get('password')
        
        logger.info(f"=== DEBUG LOGIN ===")
        logger.info(f"Username recibido: {username}")
        logger.info(f"Password recibido: {password}")
        logger.info(f"Password length: {len(password)}")
        
        # PRUEBA: Ver el usuario en la BD en este momento
        try:
            user = User.objects.get(username=username)
            logger.info(f"Usuario encontrado en BD: {user.username}")
            logger.info(f"Password en BD (primeros 80 chars): {user.password[:80]}")
            logger.info(f"¿Password es hash? pbkdf2_: {user.password.startswith('pbkdf2_')}")
            logger.info(f"¿Password es hash? argon2: {user.password.startswith('argon2')}")
            
            # Probar check_password directamente
            from django.contrib.auth.hashers import check_password
            password_match = check_password(password, user.password)
            logger.info(f"check_password result: {password_match}")
            
        except User.DoesNotExist:
            logger.error(f"Usuario {username} NO EXISTE en BD")
        
        logger.info("Intentando super().validate()...")
        
        try:
            data = super().validate(attrs)
            import json
            data_info = json.dumps(data)
            logger.info(f"✅ super().validate() EXITOSO con data: {data_info}")
        except Exception as e:
            logger.error(f"❌ ERROR en super().validate(): {str(e)}")
            raise
        
        logger.info(f"User id {str(user.id)}")
        send_user = UserSerializer(user).data
        import random
        send_user["id"] = "USER_" + str(random.randint(1_000_000_000, 9_999_999_999))
        data["user"] = send_user
        return data

class RegisterSerializer(serializers.ModelSerializer):
    password = serializers.CharField(
        write_only=True,
        required=True,
        style={'input_type': 'password'},
        min_length=8
    )

    username = serializers.CharField(
        required=True,
    )
    
    class Meta:
        model = User
        fields = [
            'id', 'username', 'email', 'password',
            'first_name', 'last_name'
        ]
    
    # def create(self, validated_data):
    #     import logging
    #     logger = logging.getLogger(__name__)
        
    #     logger.info(f"[REGISTER] Creando usuario: {validated_data.get('username')}")
        
    #     # Extraer la contraseña
    #     password = validated_data.pop('password')
        
    #     # ✅ CONVERTIR EMAIL VACÍO A NULL
    #     if 'email' in validated_data and not validated_data['email']:
    #         validated_data['email'] = None
        
    #     logger.info(f"[REGISTER] Password recibido: {password[:3]}... (len: {len(password)})")
        
    #     # Crear usuario sin contraseña
    #     # user = User.objects.create(**validated_data)
    #     # logger.info(f"[REGISTER] Usuario creado: {user.username}")
        
    #     # Establecer contraseña con hashing
    #     # user.set_password(password)
    #     # user.save()
        
    #     logger.info(f"[REGISTER] Password después de set_password: {user.password[:80]}...")
        
    #     # Generar token de verificación solo si hay email
    #     if user.email:
    #         user.generate_verification_token()
        
    #     logger.info(f"[REGISTER] Usuario {user.username} creado exitosamente")
    #     user.id = str(user.id)
    #     return user
    
class UserSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = [
            'id', 'username', 'email', 'first_name', 'last_name',
            'is_verified', 'date_joined', 'last_login', 'preferences'
        ]
        read_only_fields = ['id', 'date_joined', 'last_login', 'is_verified']

class ChangePasswordSerializer(serializers.Serializer):
    current_password = serializers.CharField(required=True, write_only=True)
    new_password = serializers.CharField(required=True, write_only=True, min_length=8)
    
    def validate(self, attrs):        
        # Validar que la nueva contraseña sea diferente a la actual
        if attrs['current_password'] == attrs['new_password']:
            raise serializers.ValidationError({
                "new_password": "La nueva contraseña debe ser diferente a la actual"
            })
        
        # Validar fortaleza de la nueva contraseña
        new_password = attrs['new_password']
        errors = []
        
        if not any(c.islower() for c in new_password):
            errors.append("La nueva contraseña debe contener al menos una letra minúscula")
        if not any(c.isupper() for c in new_password):
            errors.append("La nueva contraseña debe contener al menos una letra mayúscula")
        if not any(c.isdigit() for c in new_password):
            errors.append("La nueva contraseña debe contener al menos un número")
        
        if errors:
            raise serializers.ValidationError({"new_password": errors})
        
        return attrs

class VerifyEmailSerializer(serializers.Serializer):
    token = serializers.CharField(required=True)

class PasswordResetRequestSerializer(serializers.Serializer):
    email = serializers.EmailField(required=True)

class PasswordResetConfirmSerializer(serializers.Serializer):
    token = serializers.CharField(required=True)
    new_password = serializers.CharField(required=True, write_only=True, min_length=8)
    
    def validate(self, attrs):
        return attrs



class AudioStreamerSerializer(serializers.Serializer):
    chunk_index = serializers.IntegerField()
    chunk_count = serializers.IntegerField()
    audio_id = serializers.CharField(max_length=100)
    include_metadata = serializers.BooleanField(default=False)

    def handle_request(self, leader_manager, data):
        import base64

        chunk_index = data["chunk_index"]
        chunk_count = data["chunk_count"]
        audio_id = data["audio_id"]
        include_metadata = data["include_metadata"]

        # LECTURA DISTRIBUIDA DE CHUNKS 
        raw_chunks = leader_manager.read_file_chunks(
            audio_id,
            chunk_index,
            chunk_count,
        )

        encoded_chunks = [base64.b64encode(c).decode() for c in raw_chunks]

        response = {
            "chunk_index": chunk_index,
            "chunk_count": len(encoded_chunks),
            "chunks": encoded_chunks,
        }

        # METADATOS
        if include_metadata:
            try:
                track = Track.objects.get(id=audio_id)

                # Archivo en nodos (info del índice)
                total_chunks = leader_manager._get_file_info_from_index(audio_id) or 0

                response["metadata"] = {
                    "id": track.id,
                    "title": track.title,
                    "album": track.album.name if track.album else None,
                    "artists": list(track.artist.values_list("name", flat=True)),
                    "extension": track.extension,

                    # del modelo
                    "duration_seconds": track.duration_seconds,
                    "bitrate": track.bitrate,

                    # del índice distribuido (si están)
                    "total_chunks": total_chunks,

                    # fijo del sistema
                    "chunk_size": CHUNK_SIZE,
                }

            except Track.DoesNotExist:
                pass

        return response

class ArtistSerializer(serializers.ModelSerializer):
    class Meta:
        model = Artist
        fields = ["id", "name"]

        extra_kwargs = {
            "name": {"required": True, "min_length": 1},
            "id": {"required": False},
        }

    def create(self, validated_data):
        if "id" not in validated_data:
            validated_data["id"] = str(uuid.uuid4())
        return super().create(validated_data)

class AlbumSerializer(serializers.ModelSerializer):
    class Meta:
        model = Album
        fields = ["id", "name", "date", "author"]

        extra_kwargs = {
            "name": {"required": True, "min_length": 1},
            "date": {"required": True},
            "author": {"required": False},
            "id": {"required": False},
        }

    def create(self, validated_data):
        if "id" not in validated_data:
            validated_data["id"] = str(uuid.uuid4())
        return super().create(validated_data)

class TrackSerializer(serializers.ModelSerializer):
    file_base64 = serializers.CharField(write_only=True, required=False)
    album_name = serializers.SerializerMethodField(required=False)
    artist_names = serializers.SerializerMethodField(required=False)
    
    # Cambiar artist para aceptar lista de IDs en lugar de objetos
    artist = serializers.PrimaryKeyRelatedField(
        many=True, 
        queryset=Artist.objects.all(),
        required=False,
        allow_empty=True
    )
    
    class Meta:
        model = Track
        fields = [
            "id",
            "title",
            "album",
            "artist",
            "album_name",
            "artist_names",
            "file_base64",
            "duration_seconds",
            "bitrate",
            "extension",
        ]
        extra_kwargs = {
            "title": {"required": False, "allow_null": True, "allow_blank": True},
            "artist": {"required": False},
            "album": {"required": False, "allow_null": True},
            "id": {"required": False, "allow_null": True},  
            "duration_seconds": {"required": False},
            "bitrate": {"required": False},
            "extension": {"required": False},
        }
    
    def get_audio_info(self, audio_bytes: bytes):
        audio = AudioSegment.from_file(io.BytesIO(audio_bytes))
        duration_seconds = len(audio) / 1000.0
        bitrate = audio.frame_rate * audio.frame_width * audio.channels
        extension = "unknown"
        channels = audio.channels
        frame_rate = audio.frame_rate
        sample_width = audio.sample_width
        
        @dataclass
        class AudioMetadata:
            duration_seconds: int
            bitrate: int
            extension: str
            channels: int
            frame_rate: int
            sample_width: int
        
        metadata = AudioMetadata(
            duration_seconds, bitrate, extension, channels, frame_rate, sample_width
        )
        return metadata
    
    def get_album_name(self, obj):
        return obj.album.name if obj.album else None
    
    def get_artist_names(self, obj):
        return [artist.name for artist in obj.artist.all()]















































