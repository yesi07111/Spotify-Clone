# code/app/models.py
from django.db import models
import uuid
import hashlib
import secrets
from django.db import models
from django.contrib.auth.models import AbstractBaseUser, BaseUserManager, PermissionsMixin
from django.utils import timezone
from django.core.validators import RegexValidator

class UserManager(BaseUserManager):
    def create_user(self, username, password=None, **extra_fields):
        if not username:
            raise ValueError('El nombre de usuario es obligatorio')
        
        user = self.model(
            username=username,
            **extra_fields
        )
        
        if password:
            # Hash seguro de la contraseña
            user.set_password(password)
        
        user.save(using=self._db)
        return user
    
    def create_superuser(self, username, password=None, **extra_fields):
        extra_fields.setdefault('is_staff', True)
        extra_fields.setdefault('is_superuser', True)
        extra_fields.setdefault('is_active', True)
        
        return self.create_user(username, password, **extra_fields)

class User(AbstractBaseUser, PermissionsMixin):
    id = models.CharField(primary_key=True, default="string", editable=False, max_length=36)
    username = models.CharField(
        max_length=50,
        unique=True,
        validators=[
            RegexValidator(
                regex='^[a-zA-Z0-9_]+$',
                message='El nombre de usuario solo puede contener letras, números y guiones bajos'
            )
        ]
    )
    email = models.EmailField(unique=False, blank=True, null=True)
    
    # Información personal
    first_name = models.CharField(max_length=30, blank=True)
    last_name = models.CharField(max_length=30, blank=True)
    
    # Estados
    is_active = models.BooleanField(default=True)
    is_staff = models.BooleanField(default=False)
    is_verified = models.BooleanField(default=False)
    
    # Fechas
    date_joined = models.DateTimeField(default=timezone.now)
    last_login = models.DateTimeField(null=True, blank=True)
    
    # Token de verificación
    verification_token = models.CharField(max_length=100, blank=True)
    verification_token_expires = models.DateTimeField(null=True, blank=True)
    
    # Token de refresh (para invalidación distribuida)
    refresh_token_version = models.IntegerField(default=1)
    
    # Configuración de preferencias
    preferences = models.JSONField(default=dict, blank=True)
    
    objects = UserManager()
    
    USERNAME_FIELD = 'username'
    REQUIRED_FIELDS = []
    
    class Meta:
        ordering = ['-date_joined']
        verbose_name = 'Usuario'
        verbose_name_plural = 'Usuarios'
    
    def __str__(self):
        return self.username
    
    def generate_verification_token(self):
        """Genera un token seguro para verificación"""
        token = secrets.token_urlsafe(32)
        self.verification_token = hashlib.sha256(token.encode()).hexdigest()
        self.verification_token_expires = timezone.now() + timezone.timedelta(hours=24)
        self.save()
        return token
    
    def verify_token(self, token):
        """Verifica un token de verificación"""
        if not self.verification_token or not self.verification_token_expires:
            return False
        
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        is_valid = (
            secrets.compare_digest(token_hash, self.verification_token) and
            timezone.now() <= self.verification_token_expires
        )
        
        if is_valid:
            self.is_verified = True
            self.verification_token = ''
            self.verification_token_expires = None
            self.save()
        
        return is_valid
    
    def invalidate_refresh_tokens(self):
        """Incrementa la versión para invalidar todos los refresh tokens"""
        self.refresh_token_version += 1
        self.save()
    
    
    @property
    def full_name(self):
        return f"{self.first_name} {self.last_name}".strip()



class Artist(models.Model):
    id = models.CharField(max_length=100, primary_key=True)
    name = models.CharField(max_length=100)
    user = models.ForeignKey(
        User, 
        on_delete=models.CASCADE,
        related_name='artists',
        null=False,
        blank=False
    )
   

    def __str__(self) -> str:
        return f'<artist_id={self.id} | {self.name}>'
    
    class Meta:
        ordering = ['-name']

class Album(models.Model):
    id = models.CharField(max_length=100, primary_key=True)
    name = models.CharField(max_length=100)
    date = models.DateField()
    author = models.ForeignKey(to=Artist, null=True, on_delete=models.SET_NULL)
    user = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name='albums',
        null=False,
        blank=False
    )

    def __str__(self) -> str:
        return f'<album_id={self.id} | {self.name} | {self.date}>'

    class Meta:
        ordering = ['-author', '-date']

class Track(models.Model):
    id = models.CharField(max_length=100, primary_key=True)
    title = models.CharField(max_length=100, null=True)
    album = models.ForeignKey(to=Album, null=True, on_delete=models.SET_NULL)
    artist = models.ManyToManyField(to=Artist, blank=True)
    duration_seconds = models.IntegerField(null=False)
    bitrate = models.IntegerField(null=False)
    extension = models.CharField(max_length=10)
    user = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name='tracks',
        null=False,
        blank=False
    )


    def __str__(self) -> str:
        return f'<track_id={self.id} | {self.title}>'
    
    class Meta:
        ordering = ['-title']

