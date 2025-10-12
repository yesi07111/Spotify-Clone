from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import ArtistViewSet, AlbumViewSet, AudioStreamerView, TrackViewSet

router = DefaultRouter()
router.register(r"artists", ArtistViewSet)
router.register(r"albums", AlbumViewSet)
router.register(r"tracks", TrackViewSet)

urlpatterns = [
    path("", include(router.urls)),
    path("streamer/", AudioStreamerView.as_view(), name="streaming_endpoint"),
]
