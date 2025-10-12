from rest_framework import viewsets, status
from rest_framework.views import APIView
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import AllowAny
from .models import Artist, Album, Track
from .serializers import (
    ArtistSerializer,
    AlbumSerializer,
    AudioStreamerSerializer,
    TrackSerializer,
)


class AudioStreamerView(APIView):
    def get(self, request):
        query_params = {
            "chunk_index": int(request.GET.get("chunk_index", 0)),
            "chunk_count": int(request.GET.get("chunk_count", 1)),
            "audio_id": request.GET.get("audio_id", ""),
            "client_id": request.GET.get("client_id", ""),
            "include_header": request.GET.get("include_header", "false").lower()
            == "true",
            "include_metadata": request.GET.get("include_metadata", "false").lower()
            == "true",
        }

        serializer = AudioStreamerSerializer(data=query_params)

        response = serializer.handle_request(query_params)
        return Response(response, status=status.HTTP_200_OK)


class ArtistViewSet(viewsets.ModelViewSet):
    queryset = Artist.objects.all()
    serializer_class = ArtistSerializer
    lookup_field = "id"
    permission_classes = [AllowAny]

    def get_queryset(self):
        queryset = Artist.objects.all()
        name = self.request.query_params.get("name", None)
        if name is not None:
            queryset = queryset.filter(name__icontains=name)
        return queryset


class AlbumViewSet(viewsets.ModelViewSet):
    queryset = Album.objects.all()
    serializer_class = AlbumSerializer
    lookup_field = "id"
    permission_classes = [AllowAny]

    def get_queryset(self):
        queryset = Album.objects.all()
        name = self.request.query_params.get("name", None)
        if name is not None:
            queryset = queryset.filter(name__icontains=name)
        return queryset


class TrackViewSet(viewsets.ModelViewSet):
    queryset = Track.objects.all()
    serializer_class = TrackSerializer
    lookup_field = "id"
    permission_classes = [AllowAny]

    def get_queryset(self):
        queryset = Track.objects.all()

        # Filtrar por título de la canción
        title = self.request.query_params.get("title", None)
        if title is not None:
            queryset = queryset.filter(title__icontains=title)

        # Filtrar por artistas
        artist_ids = self.request.query_params.getlist("artist[]", None)
        if artist_ids:
            queryset = queryset.filter(artist__id__in=artist_ids)

        # Filtrar por álbum
        album_id = self.request.query_params.get("album", None)
        if album_id:
            queryset = queryset.filter(album__id=album_id)

        return queryset

    @action(detail=True, methods=["get"])
    def audio_file(self, request, id=None):
        """Endpoint para obtener el archivo de audio de un track"""
        try:
            track = self.get_object()
            file_path = f"../audios/{track.id}"

            with open(file_path, "rb") as f:
                audio_data = f.read()

            # Convertir a base64 para enviar
            import base64

            audio_base64 = base64.b64encode(audio_data).decode("utf-8")

            return Response(
                {
                    "audio_base64": audio_base64,
                    "track_id": track.id,
                    "format": track.extension,
                }
            )

        except FileNotFoundError:
            return Response(
                {"error": "Audio file not found"}, status=status.HTTP_404_NOT_FOUND
            )
        except Exception as e:
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    @action(detail=False, methods=["get"])
    def by_artist(self, request):
        """Endpoint para obtener tracks por artista"""
        artist_id = request.query_params.get("artist_id")
        if not artist_id:
            return Response(
                {"error": "artist_id parameter is required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        tracks = Track.objects.filter(artist__id=artist_id)
        serializer = self.get_serializer(tracks, many=True)
        return Response(serializer.data)

    @action(detail=False, methods=["get"])
    def by_album(self, request):
        """Endpoint para obtener tracks por álbum"""
        album_id = request.query_params.get("album_id")
        if not album_id:
            return Response(
                {"error": "album_id parameter is required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        tracks = Track.objects.filter(album_id=album_id)
        serializer = self.get_serializer(tracks, many=True)
        return Response(serializer.data)
