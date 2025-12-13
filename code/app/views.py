import uuid

from rest_framework import viewsets, status
from rest_framework.views import APIView
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import AllowAny

from .decorators import leader_only
from .models import Artist, Album, Track
from .serializers import (
    ArtistSerializer,
    AlbumSerializer,
    AudioStreamerSerializer,
    TrackSerializer,
)

@leader_only
class AudioStreamerView(APIView):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from raft.utils import get_leader_manager
        self.leader_manager = get_leader_manager()

    def get(self, request):
        try:
            query_params = {
                "chunk_index": int(request.GET.get("chunk_index", 0)),
                "chunk_count": int(request.GET.get("chunk_count", 1)),
                "audio_id": request.GET.get("audio_id", ""),
                "include_metadata": request.GET.get("include_metadata", "false").lower() == "true",
            }

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
    permission_classes = [AllowAny]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from raft.utils import get_leader_manager
        self.leader_manager = get_leader_manager()

    def get_queryset(self):
        queryset = Artist.objects.all()
        name = self.request.query_params.get("name", None)
        if name is not None:
            queryset = queryset.filter(name__icontains=name)
        return queryset
    
    def list(self, request, *args, **kwargs):
        """GET - Lectura distribuida de metadatos"""
        try:
            query_data = {
                "model": "artist",
                "filters": {}
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
        """POST - Escritura distribuida con commit en dos fases"""
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        
        try:
            # Crear instancia sin guardar
            artist = Artist(**serializer.validated_data)
            import logging
            if not artist.id:
                logging.info("\nArtistView - create -  Asignando nuevo UUID al artista que no tiene (?)")
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
    
    def update(self, request, *args, **kwargs):
        """PUT - Actualización distribuida"""
        partial = kwargs.pop('partial', False)
        instance = self.get_object()
        serializer = self.get_serializer(instance, data=request.data, partial=partial)
        serializer.is_valid(raise_exception=True)
        
        try:
            # Actualizar instancia sin guardar
            for attr, value in serializer.validated_data.items():
                setattr(instance, attr, value)
            
            # Coordinar escritura distribuida
            result = self.leader_manager.write_metadata(instance, "update")
            
            return Response(result, status=status.HTTP_200_OK)
        
        except Exception as e:
            return Response(
                {"error": str(e)}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    def destroy(self, request, *args, **kwargs):
        """DELETE - Eliminación distribuida"""
        instance = self.get_object()
        
        try:
            # Coordinar eliminación distribuida
            self.leader_manager.write_metadata(instance, "delete")
            
            return Response(status=status.HTTP_204_NO_CONTENT)
        
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
    permission_classes = [AllowAny]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from raft.utils import get_leader_manager
        self.leader_manager = get_leader_manager()

    def get_queryset(self):
        queryset = Album.objects.all()
        name = self.request.query_params.get("name")
        if name:
            queryset = queryset.filter(name__icontains=name)
        return queryset

    def list(self, request, *args, **kwargs):
        try:
            # filters = {}
            # if "name" in request.query_params:
            #     filters["name__icontains"] = request.query_params["name"]

            # data = self.leader_manager.read_metadata({
            #     "model": "album",
            #     "filters": filters
            # })

            query_data = {
                "model": "album",
                "filters": {}
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
            data = self.leader_manager.read_metadata({
                "model": "album",
                "filters": {"id": kwargs["id"]}
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
            validated = serializer.validated_data
            if "id" not in validated:
                validated["id"] = str(uuid.uuid4())

            obj = Album(**validated)
            import logging
            logging.info("\nAlbumView - create - Antes de llamar a write_metadata")
            result = self.leader_manager.write_metadata(obj, "create")

            return Response(result, status=status.HTTP_201_CREATED)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def update(self, request, *args, **kwargs):
        instance = self.get_object()
        serializer = self.get_serializer(instance, data=request.data, partial=kwargs.get("partial", False))
        serializer.is_valid(raise_exception=True)

        try:
            for field, value in serializer.validated_data.items():
                setattr(instance, field, value)

            result = self.leader_manager.write_metadata(instance, "update")

            return Response(result, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def destroy(self, request, *args, **kwargs):
        instance = self.get_object()
        try:
            self.leader_manager.write_metadata(instance, "delete")
            return Response(status=status.HTTP_204_NO_CONTENT)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@leader_only
class TrackViewSet(viewsets.ModelViewSet):
    queryset = Track.objects.all()
    serializer_class = TrackSerializer
    lookup_field = "id"
    permission_classes = [AllowAny]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from raft.utils import get_leader_manager
        self.leader_manager = get_leader_manager()

    def get_queryset(self):
        queryset = Track.objects.all()

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

    def list(self, request, *args, **kwargs):
        try:
            filters = {}

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
            track_id = kwargs["id"]

            data = self.leader_manager.read_metadata({
                "model": "track",
                "filters": {"id": track_id}
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
            file_base64 = request.data.get("file_base64")
            if not file_base64:
                return Response({"error": "file_base64 is required"}, status=status.HTTP_400_BAD_REQUEST)
            
            file_data = base64.b64decode(file_base64)
            
            # Obtener metadata del audio
            temp_serializer = TrackSerializer()
            meta = temp_serializer.get_audio_info(file_data)
            
            # Obtener datos validados
            track_data = serializer.validated_data
            
            # EXTRAER file_base64 si está en validated_data (no es campo del modelo)
            track_data.pop('file_base64', None)
            
            # EXTRAER album_name y artist_names si están (son SerializerMethodField)
            track_data.pop('album_name', None)
            track_data.pop('artist_names', None)
            
            # EXTRAER LOS ARTISTAS ANTES de crear el objeto Track
            # validated_data['artist'] es una lista de objetos Artist
            artists_data = track_data.pop('artist', [])
            
            # Convertir a lista de IDs
            artist_ids = [artist.id for artist in artists_data]
            
            # Agregar metadata del audio
            track_data["duration_seconds"] = meta.duration_seconds
            track_data["bitrate"] = meta.bitrate
            track_data["extension"] = meta.extension
            
            # Crear el Track SIN el campo artist (ManyToMany)
            track = Track(**track_data)
            
            # Agregar los artist_ids como atributo temporal para el DBManager
            track._artist_ids = artist_ids
            
            # 1. Primero escribir el archivo distribuido
            filename = f"{track.id}.{track.extension}"
            import logging 
            logging.info(f"\nTrackView - create -  Antes de llamar a write_file con filename: {filename}")
            logging.info(f"\nTrackView - create -  Antes de llamar a write_file con file_data de tipo: {type(file_data)}")
            write_file = self.leader_manager.write_file(filename, file_data)
            
            # 2. Luego escribir metadata con 2PC
            write_meta = self.leader_manager.write_metadata(track, "create")
            
            return Response(
                {
                    "success": True,
                    "data": write_meta.get("data"),
                    "file_distribution": write_file.get("distribution")
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
        instance = self.get_object()
        serializer = self.get_serializer(instance, data=request.data, partial=kwargs.get("partial", False))
        serializer.is_valid(raise_exception=True)

        try:
            # Extraer artistas si vienen
            artists_data = serializer.validated_data.pop('artist', None)
            
            # Actualizar campos normales
            for field, value in serializer.validated_data.items():
                setattr(instance, field, value)
            
            # Si hay artistas, agregarlos como atributo temporal
            if artists_data is not None:
                artist_ids = [artist.id for artist in artists_data]
                instance._artist_ids = artist_ids

            result = self.leader_manager.write_metadata(instance, "update")
            return Response(result, status=status.HTTP_200_OK)

        except Exception as e:
            import traceback
            return Response(
                {
                    "error": str(e),
                    "traceback": traceback.format_exc()
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        
    def destroy(self, request, *args, **kwargs):
        instance = self.get_object()

        try:
            track_id = instance.id

            try:
                self._delete_distributed_file(track_id)
            except:
                pass

            self.leader_manager.write_metadata(instance, "delete")

            return Response(status=status.HTTP_204_NO_CONTENT)

        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @action(detail=False, methods=["get"])
    def by_artist(self, request):
        artist_id = request.query_params.get("artist_id")
        if not artist_id:
            return Response({"error": "artist_id is required"}, status=400)

        try:
            data = self.leader_manager.read_metadata({
                "model": "track",
                "filters": {"artist__id": artist_id}
            })
            return Response(data)
        except Exception as e:
            return Response({"error": str(e)}, status=500)

    @action(detail=False, methods=["get"])
    def by_album(self, request):
        album_id = request.query_params.get("album_id")
        if not album_id:
            return Response({"error": "album_id is required"}, status=400)

        try:
            data = self.leader_manager.read_metadata({
                "model": "track",
                "filters": {"album__id": album_id}
            })
            return Response(data)
        except Exception as e:
            return Response({"error": str(e)}, status=500)
        

