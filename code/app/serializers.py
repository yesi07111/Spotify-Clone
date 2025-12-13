import io
import os
import uuid
import base64

from pydub import AudioSegment
from dataclasses import dataclass
from rest_framework import serializers
from .models import Artist, Album, Track
from backend.settings import CHUNK_SIZE


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
            "id": {"required": True},  # ID es requerido según tu swagger
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
    
    