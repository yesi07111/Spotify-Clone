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
        # El frontend envía el hash SHA256 de la contraseña
        # Aquí aplicamos un segundo hash para seguridad adicional
        password = attrs.get('password')
        
        # Aplicar hash PBKDF2 al hash recibido
        import hashlib
        import base64
        
        # Primero decodificamos el hash SHA256 (viene en hex)
        sha256_hash = password
        # Aplicamos un segundo hash (SHA256 del SHA256)
        double_hash = hashlib.sha256(sha256_hash.encode()).hexdigest()
        
        # Actualizamos la contraseña para la autenticación
        attrs['password'] = double_hash
        
        data = super().validate(attrs)
        
        # Añadir información adicional del usuario
        user = self.user
        refresh = self.get_token(user)
        
        data.update({
            'user': {
                'id': str(user.id),
                'username': user.username,
                'email': user.email,
                'first_name': user.first_name,
                'last_name': user.last_name,
                'is_verified': user.is_verified,
            },
            'refresh_token_version': user.refresh_token_version,
        })
        
        return data

class RegisterSerializer(serializers.ModelSerializer):
    password = serializers.CharField(
        write_only=True,
        required=True,
        style={'input_type': 'password'},
        min_length=8
    )
    confirm_password = serializers.CharField(
        write_only=True,
        required=True,
        style={'input_type': 'password'}
    )
    
    class Meta:
        model = User
        fields = [
            'id', 'username', 'email', 'password', 'confirm_password',
            'first_name', 'last_name'
        ]
        read_only_fields = ['id']
        extra_kwargs = {
            'email': {'required': False},
            'username': {
                'validators': [UniqueValidator(queryset=User.objects.all())]
            }
        }
    
    def validate(self, attrs):
        if attrs['password'] != attrs['confirm_password']:
            raise serializers.ValidationError({
                "confirm_password": "Las contraseñas no coinciden"
            })
        
        # Validar fortaleza de contraseña
        password = attrs['password']
        errors = []
        
        if not any(c.islower() for c in password):
            errors.append("La contraseña debe contener al menos una letra minúscula")
        if not any(c.isupper() for c in password):
            errors.append("La contraseña debe contener al menos una letra mayúscula")
        if not any(c.isdigit() for c in password):
            errors.append("La contraseña debe contener al menos un número")
        
        if errors:
            raise serializers.ValidationError({"password": errors})
        
        return attrs
    
    def create(self, validated_data):
        validated_data.pop('confirm_password')
        
        # Hash de la contraseña antes de guardar
        # El frontend ya envió SHA256, aplicamos PBKDF2
        from django.contrib.auth.hashers import make_password
        
        # El password viene como hash SHA256 del frontend
        sha256_hash = validated_data['password']
        # Aplicamos PBKDF2 para almacenamiento seguro
        validated_data['password'] = make_password(sha256_hash)
        
        user = User.objects.create_user(**validated_data)
        
        # Generar token de verificación
        user.generate_verification_token()
        
        return user

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
    confirm_new_password = serializers.CharField(required=True, write_only=True)
    
    def validate(self, attrs):
        if attrs['new_password'] != attrs['confirm_new_password']:
            raise serializers.ValidationError({
                "confirm_new_password": "Las nuevas contraseñas no coinciden"
            })
        
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
    confirm_new_password = serializers.CharField(required=True, write_only=True)
    
    def validate(self, attrs):
        if attrs['new_password'] != attrs['confirm_new_password']:
            raise serializers.ValidationError({
                "confirm_new_password": "Las contraseñas no coinciden"
            })
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
                file_info = leader_manager._get_file_info_from_index(audio_id) or {}

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
                    "file_size": file_info.get("size", 0),
                    "total_chunks": file_info.get("total_chunks", 0),
                    "channels": file_info.get("channels", 2),
                    "sample_rate": file_info.get("sample_rate", None),

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
    album_name = serializers.SerializerMethodField()
    artist_names = serializers.SerializerMethodField()
    
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
    
    