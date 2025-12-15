from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import (
    CustomTokenObtainPairView,
    CustomTokenRefreshView,
    RegisterView,
    LogoutView,
    ProfileView,
    ChangePasswordView,
    VerifyEmailView,
    PasswordResetRequestView,
    PasswordResetConfirmView,
    DeleteAccountView,
    UserViewSet,
    ArtistViewSet,
    AlbumViewSet,
    TrackViewSet,
    AudioStreamerView
)

router = DefaultRouter()
router.register(r'users', UserViewSet)
router.register(r'artists', ArtistViewSet)
router.register(r'albums', AlbumViewSet)
router.register(r'tracks', TrackViewSet)

urlpatterns = [
    # Autenticación
    path('auth/login/', CustomTokenObtainPairView.as_view(), name='login'),
    path('auth/token/refresh/', CustomTokenRefreshView.as_view(), name='token_refresh'),
    path('auth/register/', RegisterView.as_view(), name='register'),
    path('auth/logout/', LogoutView.as_view(), name='logout'),
    path('auth/profile/', ProfileView.as_view(), name='profile'),
    path('auth/change-password/', ChangePasswordView.as_view(), name='change_password'),
    path('auth/verify-email/', VerifyEmailView.as_view(), name='verify_email'),
    path('auth/password-reset/', PasswordResetRequestView.as_view(), name='password_reset'),
    path('auth/password-reset-confirm/', PasswordResetConfirmView.as_view(), name='password_reset_confirm'),
    path('auth/delete-account/', DeleteAccountView.as_view(), name='delete_account'),
    
    # Streaming
    path('streamer/', AudioStreamerView.as_view(), name='streamer'),
    
    # API REST
    path('', include(router.urls)),
]