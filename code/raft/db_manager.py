#db_manager.py
import datetime
import json
import logging
import threading
import Pyro5.api as rpc
from typing import TYPE_CHECKING
from django.db import transaction, connection
from django.core.exceptions import ObjectDoesNotExist
from raft.log_utils import log_info, log_success, log_error, log_warning

if TYPE_CHECKING:
    from app.models import Artist, Album, Track

@rpc.expose
class DBManager:
    """
    Manager remoto/local para CRUD de metadatos (Artist, Album, Track).
    Equivalente a StorageManager pero para SQLite via Django ORM.
    """

    def __init__(self):
        from raft.db_json_manager import DBJsonManager
        # Almacén temporal para operaciones sin commit
        self.pending_operations = {}  # {task_id: {"operation": ..., "data": ...}}
        self.pending_lock = threading.Lock()
        self.json_manager = DBJsonManager()
        self.logger = logging.getLogger("DB_Manager")

    # GET
    def get_data(self, query_data: dict):
        """
        query_data:
            {
                "model": "user" | "artist" | "album" | "track",
                "filters": { field: value }
            }
        """
        from app.models import Artist, Album
        model = query_data.get("model")
        filters = query_data.get("filters", {})

        model_cls = self._select_model(model)
        if model_cls is None:
            return None

        try:
            objs = model_cls.objects.filter(**filters)
            return [self._serialize(obj) for obj in objs]
        except Exception as e:
            return {"error": str(e)}

    # EXISTS
    def exists(self, metadata) -> bool:
        """
        Verifica existencia por ID si lo tiene,
        si no, por sus campos principales.
        """
        from app.models import Artist, Album, Track
        
        # Normalizar metadata a objeto si es necesario
        metadata_obj = self._normalize_to_object(metadata_obj=metadata)
        if metadata_obj is None:
            return False
            
        model_cls = metadata_obj.__class__

        if hasattr(metadata_obj, "id") and metadata_obj.id:
            return model_cls.objects.filter(id=metadata_obj.id).exists()

        if isinstance(metadata_obj, Artist):
            return Artist.objects.filter(name=metadata_obj.name).exists()

        elif isinstance(metadata_obj, Album):
            return Album.objects.filter(
                name=metadata_obj.name,
                author=metadata_obj.author,
                date=metadata_obj.date,
            ).exists()

        elif isinstance(metadata_obj, Track):
            qs = Track.objects.filter(title=metadata_obj.title)
            if metadata_obj.album:
                qs = qs.filter(album=metadata_obj.album)
            return qs.exists()

        return False

    # CREATE / UPDATE
    def create_data(self, metadata):
        """
        Crea o actualiza datos.
        Si metadata.id existe → update
        Si no → create
        """
        from app.models import Artist, Album, Track
        
        # Normalizar metadata a objeto si es necesario
        metadata_obj = self._normalize_to_object(metadata_obj=metadata)
        if metadata_obj is None:
            return {"error": "Invalid metadata"}
            
        model_cls = metadata_obj.__class__
        
        try:
            # Extraer los artist_ids si es un Track
            artist_ids = []
            if isinstance(metadata_obj, Track):
                # Buscar artist_ids en atributos temporales
                if hasattr(metadata_obj, '_artist_ids'):
                    artist_ids = metadata_obj._artist_ids
                # También verificar si viene en los campos extraídos
                elif hasattr(metadata_obj, 'artist_ids'):
                    artist_ids = metadata_obj.artist_ids
            
            # Extraer campos para crear/actualizar (excluye ManyToMany)
            data = self._extract_fields(metadata_obj)
            
            # Si estamos actualizando, remover artist de data para evitar error
            if 'artist' in data:
                del data['artist']
            
            # ============================================================
            # UPDATE: Si el objeto ya existe
            # ============================================================
            if metadata_obj.id and model_cls.objects.filter(id=metadata_obj.id).exists():
                obj = model_cls.objects.get(id=metadata_obj.id)

                # Actualizar todos los campos normales
                for k, v in data.items():
                    setattr(obj, k, v)

                obj.save()

                # Manejar artistas para Track en UPDATE
                if isinstance(obj, Track):
                    if artist_ids:  
                        # Si hay artistas, actualizarlos
                        obj.artist.set(Artist.objects.filter(id__in=artist_ids))
                    elif hasattr(metadata_obj, '_artist_ids'):  
                        # Si _artist_ids existe pero está vacío [], limpiar artistas
                        obj.artist.clear()
                    # Si no existe _artist_ids, no tocar la relación artist

                return {"updated": True, "data": self._serialize(obj)}
            
            # ============================================================
            # CREATE: Si el objeto NO existe
            # ============================================================
            else:
                # Crear el objeto sin el campo artist (ManyToMany no se puede asignar en __init__)
                if 'artist' in data:
                    del data['artist']
                
                obj = model_cls.objects.create(**data)

                # Manejar artistas para Track en CREATE
                if isinstance(obj, Track):
                    if artist_ids:
                        # Si hay artistas, asignarlos
                        obj.artist.set(Artist.objects.filter(id__in=artist_ids))
                    # Si artist_ids está vacío, no hacer nada (ya está vacío por defecto)

                return {"created": True, "data": self._serialize(obj)}

        except Exception as e:
            import traceback
            return {"error": str(e), "traceback": traceback.format_exc()}
   
    # DELETE
    def delete_data(self, metadata) -> bool:
        from app.models import Artist, Album, Track
        
        try:
            # Normalizar metadata a objeto si es necesario
            metadata_obj = self._normalize_to_object(metadata_obj=metadata)
            if metadata_obj is None:
                return False
                
            model_cls = metadata_obj.__class__
            if metadata_obj.id:
                deleted_count, _ = model_cls.objects.filter(id=metadata_obj.id).delete()
                return deleted_count > 0
            return False
        except Exception:
            return False
        
    def get_full_dump(self) -> dict:
        """Exporta toda la base de datos como diccionario serializado"""
        from app.models import Artist, Album, Track
        
        dump = {
            "artists": [self._serialize(a) for a in Artist.objects.all()],
            "albums": [self._serialize(a) for a in Album.objects.all()],
            "tracks": [self._serialize(t) for t in Track.objects.all()],
        }
        
        return dump

    def restore_from_dump(self, db_dump: dict):
        """Restaura la base de datos desde un dump"""
        from app.models import Artist, Album, Track
        
        try:
            with transaction.atomic():
                # Limpiar tablas existentes
                Track.objects.all().delete()
                Album.objects.all().delete()
                Artist.objects.all().delete()
                
                # Restaurar artistas
                for artist_data in db_dump.get("artists", []):
                    Artist.objects.create(**artist_data)
                
                # Restaurar álbumes
                for album_data in db_dump.get("albums", []):
                    Album.objects.create(**album_data)
                
                # Restaurar tracks
                for track_data in db_dump.get("tracks", []):
                    artist_ids = track_data.pop("artist", [])
                    track = Track.objects.create(**track_data)
                    
                    if artist_ids:
                        track.artist.set(Artist.objects.filter(id__in=artist_ids))
                
                return {"success": True}
        
        except Exception as e:
            return {"success": False, "error": str(e)}

    # HELPERS PARA SERIALIZACIÓN/DESERIALIZACIÓN
    def _normalize_to_object(self, metadata_obj=None, data_dict=None, model_name=None):
        """
        Normaliza la entrada a un objeto del modelo Django.
        Puede recibir un objeto directamente o un diccionario serializado.
        """
        if metadata_obj is not None and not isinstance(metadata_obj, dict):
            # Ya es un objeto
            return metadata_obj
            
        elif data_dict is not None and model_name is not None:
            # Es un diccionario, deserializar
            return self._deserialize_to_object(data_dict, model_name)
            
        elif isinstance(metadata_obj, dict):
            # metadata_obj es un diccionario con estructura {model: ..., data: ...}
            model_name = metadata_obj.get("model")
            data_dict = metadata_obj.get("data", {})
            return self._deserialize_to_object(data_dict, model_name)
            
        return None

    # SERIALIZACIÓN / DESERIALIZACIÓN CON USUARIO
    def _serialize(self, obj):
        """Serializa Artist, Album, Track o User a un dict simple."""
        from app.models import Artist, Album, Track, User

        if isinstance(obj, Artist):
            return {
                "id": obj.id,
                "name": obj.name,
                "user": obj.user.id if obj.user else None
            }

        if isinstance(obj, Album):
            date = None
            if obj.date:
                if isinstance(obj.date, datetime.date):
                    date = obj.date.isoformat()
                elif isinstance(obj.date, str):
                    date = obj.date
                else:
                    date = str(obj.date)

            return {
                "id": obj.id,
                "name": obj.name,
                "date": date,
                "author": getattr(obj.author, 'id', None) if obj.author else None,
                "user": getattr(obj.user, 'id', None) if obj.user else None
            }

        if isinstance(obj, Track):
            artist_ids = getattr(obj, "_artist_ids", None)
            if artist_ids is None and hasattr(obj, "artist"):
                artist_ids = list(obj.artist.values_list("id", flat=True)) if obj.artist else []

            return {
                "id": obj.id,
                "title": obj.title,
                "album": obj.album.id if obj.album else None,
                "artist": artist_ids,
                "duration_seconds": obj.duration_seconds,
                "bitrate": obj.bitrate,
                "extension": obj.extension,
                "user": getattr(obj.user, 'id', None) if obj.user else None

            }

        if isinstance(obj, User):
            if obj.date_joined: 
                if isinstance(obj.date_joined, datetime.date):
                    date_joined = obj.date_joined.isoformat()
                elif isinstance(obj.date_joined, str):
                        date_joined = obj.date_joined
                else:
                    date_joined = str(obj.date_joined)
            else:
                date_joined = None
            
            if obj.last_login: 
                if isinstance(obj.last_login, datetime.date):
                    last_login = obj.last_login.isoformat()
                elif isinstance(obj.last_login, str):
                        last_login = obj.last_login
                else:
                    last_login = str(obj.last_login)
            else:
                last_login = None

            if obj.verification_token_expires: 
                if isinstance(obj.verification_token_expires, datetime.date):
                    verification_token_expires = obj.verification_token_expires.isoformat()
                elif isinstance(obj.verification_token_expires, str):
                        verification_token_expires = obj.verification_token_expires
                else:
                    verification_token_expires = str(obj.verification_token_expires)
            else:
                verification_token_expires = None

            return {
                "id": obj.id,
                "username": obj.username,
                "email": obj.email,
                "first_name": obj.first_name,
                "last_name": obj.last_name,
                "password": obj.password,  # hash directo
                "is_verified": obj.is_verified,
                "date_joined": date_joined,
                "last_login": last_login,
                "preferences": obj.preferences,
                "verification_token": obj.verification_token,
                "verification_token_expires": verification_token_expires,
                "refresh_token_version": obj.refresh_token_version
            }

        return None


    def _deserialize_to_object(self, data_dict: dict, model_name: str):
        """Convierte un dict serializado de vuelta a un objeto Django, con user asignado."""
        from app.models import Artist, Album, Track, User

        if model_name == "artist":
            artist = Artist()
            artist.id = data_dict.get("id")
            artist.name = data_dict.get("name")
            user_id = data_dict.get("user")
            if user_id:
                try:
                    artist.user = User.objects.get(id=user_id)
                except User.DoesNotExist:
                    artist.user = None
            return artist

        elif model_name == "album":
            album = Album()
            album.id = data_dict.get("id")
            album.name = data_dict.get("name")

            date_val = data_dict.get("date")
            if date_val:
                album.date = datetime.date.fromisoformat(date_val) if isinstance(date_val, str) else date_val

            author_id = data_dict.get("author")
            if author_id:
                try:
                    from app.models import Artist
                    album.author = Artist.objects.get(id=author_id)
                except Artist.DoesNotExist:
                    album.author = None
            else:
                album.author = None

            user_id = data_dict.get("user")
            if user_id:
                try:
                    album.user = User.objects.get(id=user_id)
                except User.DoesNotExist:
                    album.user = None

            return album

        elif model_name == "track":
            track = Track()
            track.id = data_dict.get("id")
            track.title = data_dict.get("title")
            track.duration_seconds = data_dict.get("duration_seconds")
            track.bitrate = data_dict.get("bitrate")
            track.extension = data_dict.get("extension")

            album_id = data_dict.get("album")
            if album_id:
                try:
                    from app.models import Album
                    track.album = Album.objects.get(id=album_id)
                except Album.DoesNotExist:
                    track.album = None

            artist_ids = data_dict.get("artist", []) or []
            track._artist_ids = artist_ids

            user_id = data_dict.get("user")
            if user_id:
                try:
                    track.user = User.objects.get(id=user_id)
                except User.DoesNotExist:
                    track.user = None

            return track

        elif model_name == "user":
            user = User()
            user.id = data_dict.get("id")
            user.username = data_dict.get("username")
            user.email = data_dict.get("email")
            user.first_name = data_dict.get("first_name")
            user.last_name = data_dict.get("last_name")
            user.password = data_dict.get("password")  # hash directo
            user.is_verified = data_dict.get("is_verified", False)

            dj = data_dict.get("date_joined")
            if dj:
                user.date_joined = datetime.datetime.fromisoformat(dj)

            lj = data_dict.get("last_login")
            if lj:
                user.last_login = datetime.datetime.fromisoformat(lj)

            user.preferences = data_dict.get("preferences", {})
            user.verification_token = data_dict.get("verification_token", "")
            vte = data_dict.get("verification_token_expires")
            if vte:
                user.verification_token_expires = datetime.datetime.fromisoformat(vte)

            user.refresh_token_version = data_dict.get("refresh_token_version", 1)
            return user

        return None

    def serialize_for_transfer(self, metadata):
        """
        Serializa un objeto para transferencia RPC.
        """
        # Si ya es un diccionario, devolverlo tal cual
        if isinstance(metadata, dict):
            return metadata
            
        return {
            "model": metadata.__class__.__name__.lower(),
            "data": self._serialize(metadata)
        }

    # OTROS HELPERS
    def _select_model(self, model_name: str):
        from app.models import User, Artist, Album, Track

        if model_name == "user":
            return User
        if model_name == "artist":
            return Artist
        if model_name == "album":
            return Album
        if model_name == "track":
            return Track
        return None

    def _extract_fields(self, metadata_obj):
        """Extrae solo los atributos de Django ORM, excluyendo ManyToManyField."""
        fields = {}
        for field in metadata_obj._meta.fields:
            fname = field.name
            value = getattr(metadata_obj, fname)
            
            # Manejar fechas
            if isinstance(value, datetime.date):
                value = value.isoformat()
                
            fields[fname] = value
        
        # Excluir campos ManyToManyField para evitar el error de asignación directa
        # Nota: 'artist' es un ManyToManyField en Track
        if hasattr(metadata_obj, '_meta'):
            for field in metadata_obj._meta.many_to_many:
                # Si el objeto tiene un atributo temporal con los datos de ManyToMany, 
                # lo guardamos pero no lo incluimos en fields para la creación
                temp_attr_name = f'_{field.name}_ids'
                if hasattr(metadata_obj, temp_attr_name):
                    # No agregamos al fields, ya se manejará por separado
                    continue
        
        return fields


    # COMMIT EN DOS FASES
    def prepare_create(self, task_id: str, metadata_obj = None, data: dict = None, model_name: str = None, term: int = 0):
        """
        Fase 1: Prepara una creación sin hacer commit.
        Retorna True si se puede hacer, False si hay error.
        """
        from app.models import Artist, Track, User, Album

        # Info - dorado para inicio de operación
        log_info("DB PREPARE", f"Iniciando prepare_create con task_id: {task_id}, term: {term}", 
                colorize_full=True, logger=self.logger, color="gold")
        
        # Normalizar entrada a objeto
        obj_to_process = self._normalize_to_object(
            metadata_obj=metadata_obj, 
            data_dict=data, 
            model_name=model_name
        )
        
        if obj_to_process is None:
            log_error("DB PREPARE", "No metadata nor data provided", 
                    colorize_full=True, logger=self.logger, color="red")
            return {"success": False, "error": "No metadata nor data provided"}
        
        # Info - dorado para detalles del objeto
        model_cls = obj_to_process.__class__
        
        # Log informativo según el tipo de objeto
        if isinstance(obj_to_process, Track):
            title = obj_to_process.title if obj_to_process.title else "Sin título"
            user = obj_to_process.user.username if hasattr(obj_to_process, 'user') and obj_to_process.user else "Usuario desconocido"
            log_info("DB PREPARE", f"Usuario '{user}' está creando una nueva canción con título '{title}'", 
                    colorize_full=True, logger=self.logger, color="gold")
        
        elif isinstance(obj_to_process, Album):
            album_name = obj_to_process.name if obj_to_process.name else "Sin nombre"
            author_name = obj_to_process.author.name if obj_to_process.author else "Autor desconocido"
            user = obj_to_process.user.username if hasattr(obj_to_process, 'user') and obj_to_process.user else "Usuario desconocido"
            log_info("DB PREPARE", f"Usuario '{user}' está creando un nuevo álbum '{album_name}' del autor '{author_name}'", 
                    colorize_full=True, logger=self.logger, color="gold")
        
        elif isinstance(obj_to_process, Artist):
            artist_name = obj_to_process.name if obj_to_process.name else "Sin nombre"
            user = obj_to_process.user.username if hasattr(obj_to_process, 'user') and obj_to_process.user else "Usuario desconocido"
            log_info("DB PREPARE", f"Usuario '{user}' está creando un nuevo artista con nombre '{artist_name}'", 
                    colorize_full=True, logger=self.logger, color="gold")
        
        elif isinstance(obj_to_process, User):
            username = obj_to_process.username if obj_to_process.username else "Sin username"
            log_info("DB PREPARE", f"Creando nuevo usuario '{username}'", 
                    colorize_full=True, logger=self.logger, color="gold")
        
        # Después de normalizar a objeto
        if isinstance(obj_to_process, User):
            # Ya viene con hash? Si no, aplicar
            password_hash = obj_to_process.password
            if password_hash and not password_hash.startswith('pbkdf2_'):
                obj_to_process.set_password()
                log_info("DB PREPARE", "Contraseña de usuario encriptada", 
                        colorize_full=True, logger=self.logger, color="gold")

        model_name_lower = model_cls.__name__.lower()
        
        # Extraer artist_ids si es un Track
        artist_ids = []
        if isinstance(obj_to_process, Track):
            if hasattr(obj_to_process, '_artist_ids'):
                artist_ids = obj_to_process._artist_ids
        
        # Crear operation_info para JSON (incluye artist_ids si es Track)
        serialized_data = self._serialize(obj_to_process)
        
        operation_info = {
            "operation": "create",
            "function": "create_data",
            "model": model_cls.__name__,
            "params": {
                "metadata_dict": serialized_data
            }
        }

        self.json_manager.add_operation(
            term=term,
            task_id=task_id,
            sql_operation=json.dumps(operation_info)
        )
        log_info("DB PREPARE", f"Operación agregada al JSON con task_id: {task_id}", 
                colorize_full=True, logger=self.logger, color="gold")

        try:
            # Extraer campos excluyendo ManyToMany
            data_dict = self._extract_fields(obj_to_process)
            
            # Si es Track, quitar 'artist' de data_dict para evitar error
            if isinstance(obj_to_process, Track) and 'artist' in data_dict:
                del data_dict['artist']
                log_info("DB PREPARE", "Campo 'artist' removido de data_dict (se manejará separadamente)", 
                        colorize_full=True, logger=self.logger, color="gold")
            
            # Iniciar transacción sin commit
            # with transaction.atomic():
            # Crear o actualizar el objeto
            if (data_dict.get("id") and model_cls.objects.filter(id=data_dict["id"]).exists()) or (isinstance(obj_to_process, User) and data_dict.get("username") and model_cls.objects.filter(username=data_dict["username"]).exists()):
                obj = model_cls.objects.get(id=data_dict["id"])
                for k, v in data_dict.items():
                    if k != "id":  # No actualizar el ID
                        setattr(obj, k, v)
                obj.save()
                
                # Log de actualización según tipo
                if isinstance(obj, Track):
                    user = obj.user.username if obj.user else "Usuario desconocido"
                    log_info("DB PREPARE", f"Usuario '{user}' actualizó canción existente '{obj.title}'", 
                            colorize_full=True, logger=self.logger, color="gold")
                elif isinstance(obj, Album):
                    user = obj.user.username if obj.user else "Usuario desconocido"
                    author = obj.author.name if obj.author else "Autor desconocido"
                    log_info("DB PREPARE", f"Usuario '{user}' actualizó álbum existente '{obj.name}' del autor '{author}'", 
                            colorize_full=True, logger=self.logger, color="gold")
                elif isinstance(obj, Artist):
                    user = obj.user.username if obj.user else "Usuario desconocido"
                    log_info("DB PREPARE", f"Usuario '{user}' actualizó artista existente '{obj.name}'", 
                            colorize_full=True, logger=self.logger, color="gold")
            else:
                # Asegurarse de no incluir campo artist en la creación
                if 'artist' in data_dict:
                    del data_dict['artist']
                obj = model_cls.objects.create(**data_dict)
                
                # Log de creación según tipo
                if isinstance(obj, Track):
                    user = obj.user.username if obj.user else "Usuario desconocido"
                    log_info("DB PREPARE", f"Usuario '{user}' creó nueva canción '{obj.title}'", 
                            colorize_full=True, logger=self.logger, color="gold")
                elif isinstance(obj, Album):
                    user = obj.user.username if obj.user else "Usuario desconocido"
                    author = obj.author.name if obj.author else "Autor desconocido"
                    log_info("DB PREPARE", f"Usuario '{user}' creó nuevo álbum '{obj.name}' del autor '{author}'", 
                            colorize_full=True, logger=self.logger, color="gold")
                elif isinstance(obj, Artist):
                    user = obj.user.username if obj.user else "Usuario desconocido"
                    log_info("DB PREPARE", f"Usuario '{user}' creó nuevo artista '{obj.name}'", 
                            colorize_full=True, logger=self.logger, color="gold")
                elif isinstance(obj, User):
                    log_info("DB PREPARE", f"Se creó nuevo usuario '{obj.username}'", 
                            colorize_full=True, logger=self.logger, color="gold")

            # Manejar relaciones ManyToMany para Track (después de crear el objeto)
            if isinstance(obj, Track) and artist_ids:
                artists = Artist.objects.filter(id__in=artist_ids)
                obj.artist.set(artists)
                obj.save()
                artist_names = [artist.name for artist in artists]
                artist_str = ", ".join(artist_names) if artist_names else "Artista desconocido"
                log_info("DB PREPARE", f"Canción '{obj.title}' asociada con artista(s): {artist_str}", 
                        colorize_full=True, logger=self.logger, color="gold")
            
            # Asegurar usuario
            if hasattr(obj_to_process, 'user') and obj_to_process.user is None:
                user_id = getattr(obj_to_process, '_user_id', None)
                if user_id:
                    try:
                        obj_to_process.user = User.objects.get(id=user_id)
                        log_info("DB PREPARE", f"Usuario '{obj_to_process.user.username}' asignado al objeto", 
                                colorize_full=True, logger=self.logger, color="gold")
                    except User.DoesNotExist:
                        obj_to_process.user = None
                        log_warning("DB PREPARE", f"Usuario ID {user_id} no encontrado", 
                                colorize_full=True, logger=self.logger, color="yellow")

            # Guardar en pending (antes del commit)
            with self.pending_lock:
                self.pending_operations[task_id] = {
                    "operation": "create",
                    "model": model_name_lower,
                    "object_id": obj.id,
                    "data": self._serialize(obj),
                    "savepoint": transaction.savepoint()
                }
            
            # Success - dorado para operación exitosa
            if isinstance(obj, Track):
                user = obj.user.username if obj.user else "Usuario desconocido"
                artist_names = [artist.name for artist in obj.artist.all()]
                artist_str = ", ".join(artist_names) if artist_names else "Artista desconocido"
                album_name = obj.album.name if obj.album else "Álbum desconocido"
                log_success("DB PREPARE", f"Usuario '{user}' preparó creación de canción '{obj.title}' con artista(s) '{artist_str}' y álbum '{album_name}' (task_id: {task_id})", 
                        colorize_full=True, logger=self.logger, color="gold")
            elif isinstance(obj, Album):
                user = obj.user.username if obj.user else "Usuario desconocido"
                author_name = obj.author.name if obj.author else "Autor desconocido"
                log_success("DB PREPARE", f"Usuario '{user}' preparó creación de álbum '{obj.name}' del autor '{author_name}' (task_id: {task_id})", 
                        colorize_full=True, logger=self.logger, color="gold")
            elif isinstance(obj, Artist):
                user = obj.user.username if obj.user else "Usuario desconocido"
                log_success("DB PREPARE", f"Usuario '{user}' preparó creación de artista '{obj.name}' (task_id: {task_id})", 
                        colorize_full=True, logger=self.logger, color="gold")
            elif isinstance(obj, User):
                log_success("DB PREPARE", f"Preparó creación de usuario '{obj.username}' (task_id: {task_id})", 
                        colorize_full=True, logger=self.logger, color="gold")
            
            return {"success": True, "prepared": True, "data": self._serialize(obj)}
        
        except Exception as e:
            log_error("DB PREPARE", f"Error en prepare_create con task_id {task_id}: {e}", 
                    colorize_full=True, logger=self.logger, color="red")
            import traceback
            return {"success": False, "error": str(e), "traceback": traceback.format_exc()}

    def prepare_update(self, task_id: str, metadata_obj=None, data: dict = None, model_name: str = None, term: int = 0):
        """
        Fase 1: Prepara un update sin hacer commit.
        Retorna True si se puede hacer, False si hay error.
        Similar a prepare_create pero solo actualiza los campos proporcionados.
        """
        from app.models import Artist, Track, User, Album

        # Info - dorado para inicio de operación
        log_info("DB PREPARE", f"Iniciando prepare_update con task_id: {task_id}, term: {term}", 
                colorize_full=True, logger=self.logger, color="gold")
        
        # Normalizar entrada a objeto
        obj_to_process = self._normalize_to_object(
            metadata_obj=metadata_obj,
            data_dict=data,
            model_name=model_name
        )

        if obj_to_process is None:
            log_error("DB PREPARE", "No metadata nor data provided", 
                    colorize_full=True, logger=self.logger, color="red")
            return {"success": False, "error": "No metadata nor data provided"}

        model_cls = obj_to_process.__class__
        model_name_lower = model_cls.__name__.lower()
        
        # Log informativo según el tipo de objeto
        user_info = f"'{obj_to_process.user.username}'" if hasattr(obj_to_process, 'user') and obj_to_process.user else "Usuario desconocido"
        
        if isinstance(obj_to_process, Track):
            title = obj_to_process.title if obj_to_process.title else "Sin título"
            log_info("DB PREPARE", f"Usuario {user_info} está actualizando canción '{title}'", 
                    colorize_full=True, logger=self.logger, color="gold")
        
        elif isinstance(obj_to_process, Album):
            album_name = obj_to_process.name if obj_to_process.name else "Sin nombre"
            log_info("DB PREPARE", f"Usuario {user_info} está actualizando álbum '{album_name}'", 
                    colorize_full=True, logger=self.logger, color="gold")
        
        elif isinstance(obj_to_process, Artist):
            artist_name = obj_to_process.name if obj_to_process.name else "Sin nombre"
            log_info("DB PREPARE", f"Usuario {user_info} está actualizando artista '{artist_name}'", 
                    colorize_full=True, logger=self.logger, color="gold")
        
        elif isinstance(obj_to_process, User):
            username = obj_to_process.username if obj_to_process.username else "Sin username"
            log_info("DB PREPARE", f"Actualizando usuario '{username}'", 
                    colorize_full=True, logger=self.logger, color="gold")

        # Extraer artist_ids si es Track
        artist_ids = []
        if isinstance(obj_to_process, Track) and hasattr(obj_to_process, "_artist_ids"):
            artist_ids = obj_to_process._artist_ids

        # Crear operación JSON para registro
        serialized_data = self._serialize(obj_to_process)
        operation_info = {
            "operation": "update",
            "function": "create_data",
            "model": model_cls.__name__,
            "params": {
                "metadata_dict": serialized_data
            }
        }

        self.json_manager.add_operation(
            term=term,
            task_id=task_id,
            sql_operation=json.dumps(operation_info)
        )
        log_info("DB PREPARE", f"Operación UPDATE agregada al JSON con task_id: {task_id}", 
                colorize_full=True, logger=self.logger, color="gold")

        try:
            # Extraer solo campos que se quieren actualizar
            data_dict = self._extract_fields(obj_to_process)

            # No permitir actualizar ID
            if "id" in data_dict:
                del data_dict["id"]
                log_info("DB PREPARE", "Campo 'id' removido de data_dict (no se puede actualizar)", 
                        colorize_full=True, logger=self.logger, color="gold")

            # Si es Track, quitar 'artist' del diccionario para actualizarlo separadamente
            if isinstance(obj_to_process, Track) and "artist" in data_dict:
                del data_dict["artist"]
                log_info("DB PREPARE", "Campo 'artist' removido de data_dict (se manejará separadamente)", 
                        colorize_full=True, logger=self.logger, color="gold")

            # Iniciar transacción sin commit
            # with transaction.atomic():
            # Buscar objeto existente
            if not model_cls.objects.filter(id=obj_to_process.id).exists():
                if isinstance(obj_to_process, Track):
                    log_error("DB PREPARE", f"Canción '{obj_to_process.title}' no encontrada para actualizar", 
                            colorize_full=True, logger=self.logger, color="red")
                elif isinstance(obj_to_process, Album):
                    log_error("DB PREPARE", f"Álbum '{obj_to_process.name}' no encontrado para actualizar", 
                            colorize_full=True, logger=self.logger, color="red")
                elif isinstance(obj_to_process, Artist):
                    log_error("DB PREPARE", f"Artista '{obj_to_process.name}' no encontrado para actualizar", 
                            colorize_full=True, logger=self.logger, color="red")
                return {"success": False, "error": "Object not found"}

            obj = model_cls.objects.get(id=obj_to_process.id)
            
            # Log de objeto encontrado
            if isinstance(obj, Track):
                user = obj.user.username if obj.user else "Usuario desconocido"
                log_info("DB PREPARE", f"Canción encontrada para actualizar: '{obj.title}' del usuario '{user}'", 
                        colorize_full=True, logger=self.logger, color="gold")
            elif isinstance(obj, Album):
                user = obj.user.username if obj.user else "Usuario desconocido"
                log_info("DB PREPARE", f"Álbum encontrado para actualizar: '{obj.name}' del usuario '{user}'", 
                        colorize_full=True, logger=self.logger, color="gold")
            elif isinstance(obj, Artist):
                user = obj.user.username if obj.user else "Usuario desconocido"
                log_info("DB PREPARE", f"Artista encontrado para actualizar: '{obj.name}' del usuario '{user}'", 
                        colorize_full=True, logger=self.logger, color="gold")

            # Actualizar solo los campos que vienen en data_dict
            updated_fields = []
            for k, v in data_dict.items():
                setattr(obj, k, v)
                updated_fields.append(k)
            obj.save()
            
            if updated_fields:
                log_info("DB PREPARE", f"Campos actualizados: {', '.join(updated_fields)}", 
                        colorize_full=True, logger=self.logger, color="gold")

            # Manejar relaciones ManyToMany para Track si vienen artist_ids
            if isinstance(obj, Track) and artist_ids:
                artists = Artist.objects.filter(id__in=artist_ids)
                obj.artist.set(artists)
                obj.save()
                artist_names = [artist.name for artist in artists]
                artist_str = ", ".join(artist_names) if artist_names else "Artista desconocido"
                log_info("DB PREPARE", f"Canción '{obj.title}' ahora tiene artista(s): {artist_str}", 
                        colorize_full=True, logger=self.logger, color="gold")

            # Guardar en pending_operations antes del commit
            with self.pending_lock:
                self.pending_operations[task_id] = {
                    "operation": "update",
                    "model": model_name_lower,
                    "object_id": obj.id,
                    "data": self._serialize(obj),
                    "savepoint": transaction.savepoint()
                }
            
            # Success - dorado para operación exitosa
            if isinstance(obj, Track):
                user = obj.user.username if obj.user else "Usuario desconocido"
                artist_names = [artist.name for artist in obj.artist.all()]
                artist_str = ", ".join(artist_names) if artist_names else "Artista desconocido"
                album_name = obj.album.name if obj.album else "Álbum desconocido"
                log_success("DB PREPARE", f"Usuario '{user}' preparó actualización de canción '{obj.title}' con artista(s) '{artist_str}' y álbum '{album_name}' (task_id: {task_id})", 
                        colorize_full=True, logger=self.logger, color="gold")
            elif isinstance(obj, Album):
                user = obj.user.username if obj.user else "Usuario desconocido"
                author_name = obj.author.name if obj.author else "Autor desconocido"
                log_success("DB PREPARE", f"Usuario '{user}' preparó actualización de álbum '{obj.name}' del autor '{author_name}' (task_id: {task_id})", 
                        colorize_full=True, logger=self.logger, color="gold")
            elif isinstance(obj, Artist):
                user = obj.user.username if obj.user else "Usuario desconocido"
                log_success("DB PREPARE", f"Usuario '{user}' preparó actualización de artista '{obj.name}' (task_id: {task_id})", 
                        colorize_full=True, logger=self.logger, color="gold")
            elif isinstance(obj, User):
                log_success("DB PREPARE", f"Preparó actualización de usuario '{obj.username}' (task_id: {task_id})", 
                        colorize_full=True, logger=self.logger, color="gold")
            
            return {"success": True, "prepared": True, "data": self._serialize(obj)}

        except Exception as e:
            log_error("DB PREPARE", f"Error en prepare_update con task_id {task_id}: {e}", 
                    colorize_full=True, logger=self.logger, color="red")
            import traceback
            return {"success": False, "error": str(e), "traceback": traceback.format_exc()}

    def prepare_delete(self, task_id: str, metadata_obj=None, data: dict = None, model_name: str = None, term: int = 0):
        """
        Fase 1: Prepara una eliminación sin hacer commit.
        """
        from app.models import Artist, Track, User, Album

        # Info - dorado para inicio de operación
        log_info("DB PREPARE", f"Iniciando prepare_delete con task_id: {task_id}, term: {term}", 
                colorize_full=True, logger=self.logger, color="gold")
        
        # Normalizar entrada a objeto
        obj_to_process = self._normalize_to_object(
            metadata_obj=metadata_obj,
            data_dict=data,
            model_name=model_name
        )
        
        if obj_to_process is None:
            log_error("DB PREPARE", "No metadata provided", 
                    colorize_full=True, logger=self.logger, color="red")
            return {"success": False, "error": "No metadata provided"}

        model_cls = obj_to_process.__class__
        serialized = self._serialize(obj_to_process)
        
        # Log informativo según el tipo de objeto
        user = obj_to_process.user.username if hasattr(obj_to_process, 'user') and obj_to_process.user else "Usuario desconocido"
        
        if isinstance(obj_to_process, Track):
            title = obj_to_process.title if obj_to_process.title else "Sin título"
            log_info("DB PREPARE", f"Usuario '{user}' está eliminando canción '{title}'", 
                    colorize_full=True, logger=self.logger, color="gold")
        
        elif isinstance(obj_to_process, Album):
            album_name = obj_to_process.name if obj_to_process.name else "Sin nombre"
            log_info("DB PREPARE", f"Usuario '{user}' está eliminando álbum '{album_name}'", 
                    colorize_full=True, logger=self.logger, color="gold")
        
        elif isinstance(obj_to_process, Artist):
            artist_name = obj_to_process.name if obj_to_process.name else "Sin nombre"
            log_info("DB PREPARE", f"Usuario '{user}' está eliminando artista '{artist_name}'", 
                    colorize_full=True, logger=self.logger, color="gold")
        
        elif isinstance(obj_to_process, User):
            username = obj_to_process.username if obj_to_process.username else "Sin username"
            log_info("DB PREPARE", f"Eliminando usuario '{username}'", 
                    colorize_full=True, logger=self.logger, color="gold")

        # Guardar operation_info en JSON
        operation_info = {
            "operation": "delete",
            "function": "delete_data",
            "model": model_cls.__name__,
            "params": {
                "metadata_dict": serialized
            }
        }

        self.json_manager.add_operation(
            term=term,
            task_id=task_id,
            sql_operation=json.dumps(operation_info)
        )
        log_info("DB PREPARE", f"Operación DELETE agregada al JSON con task_id: {task_id}", 
                colorize_full=True, logger=self.logger, color="gold")

        try:
            obj_id = obj_to_process.id

            if not obj_id:
                log_error("DB PREPARE", "ID missing in metadata", 
                        colorize_full=True, logger=self.logger, color="red")
                return {"success": False, "error": "ID missing in metadata"}

            # Fase 1: PREPARE
            # with transaction.atomic():
            # Verificar que el objeto existe
            
            if not model_cls.objects.filter(id=obj_id).exists():
                if isinstance(obj_to_process, Track):
                    log_error("DB PREPARE", f"Canción '{obj_to_process.title}' no encontrada para eliminar", 
                            colorize_full=True, logger=self.logger, color="red")
                elif isinstance(obj_to_process, Album):
                    log_error("DB PREPARE", f"Álbum '{obj_to_process.name}' no encontrado para eliminar", 
                            colorize_full=True, logger=self.logger, color="red")
                elif isinstance(obj_to_process, Artist):
                    log_error("DB PREPARE", f"Artista '{obj_to_process.name}' no encontrado para eliminar", 
                            colorize_full=True, logger=self.logger, color="red")
                elif isinstance(obj_to_process, User):
                    log_error("DB PREPARE", f"Usuario '{obj_to_process.username}' no encontrado para eliminar", 
                            colorize_full=True, logger=self.logger, color="red")
                return {"success": False, "error": "Object not found"}

            obj = model_cls.objects.get(id=obj_id)
            
            # Log de objeto encontrado
            if isinstance(obj, Track):
                user = obj.user.username if obj.user else "Usuario desconocido"
                artist_names = [artist.name for artist in obj.artist.all()]
                artist_str = ", ".join(artist_names) if artist_names else "Artista desconocido"
                album_name = obj.album.name if obj.album else "Álbum desconocido"
                log_info("DB PREPARE", f"Canción encontrada para eliminar: '{obj.title}' con artista(s) '{artist_str}' y álbum '{album_name}' del usuario '{user}'", 
                        colorize_full=True, logger=self.logger, color="gold")
            elif isinstance(obj, Album):
                user = obj.user.username if obj.user else "Usuario desconocido"
                author_name = obj.author.name if obj.author else "Autor desconocido"
                log_info("DB PREPARE", f"Álbum encontrado para eliminar: '{obj.name}' del autor '{author_name}' del usuario '{user}'", 
                        colorize_full=True, logger=self.logger, color="gold")
            elif isinstance(obj, Artist):
                user = obj.user.username if obj.user else "Usuario desconocido"
                log_info("DB PREPARE", f"Artista encontrado para eliminar: '{obj.name}' del usuario '{user}'", 
                        colorize_full=True, logger=self.logger, color="gold")
            elif isinstance(obj, User):
                log_info("DB PREPARE", f"Usuario encontrado para eliminar: '{obj.username}'", 
                        colorize_full=True, logger=self.logger, color="gold")

            # Crear savepoint y agregar a pending_operations
            with self.pending_lock:
                self.pending_operations[task_id] = {
                    "operation": "delete",
                    "model": model_cls.__name__.lower(),
                    "object_id": obj.id,
                    "backup_data": self._serialize(obj),
                    "savepoint": transaction.savepoint()    
                }
            
            # Realizar la eliminación (aún no confirmada)
            if isinstance(obj, Track):
                user = obj.user.username if obj.user else "Usuario desconocido"
                log_info("DB PREPARE", f"Canción '{obj.title}' del usuario '{user}' marcada para eliminación", 
                        colorize_full=True, logger=self.logger, color="gold")
            elif isinstance(obj, Album):
                user = obj.user.username if obj.user else "Usuario desconocido"
                log_info("DB PREPARE", f"Álbum '{obj.name}' del usuario '{user}' marcado para eliminación", 
                        colorize_full=True, logger=self.logger, color="gold")
            elif isinstance(obj, Artist):
                user = obj.user.username if obj.user else "Usuario desconocido"
                log_info("DB PREPARE", f"Artista '{obj.name}' del usuario '{user}' marcado para eliminación", 
                        colorize_full=True, logger=self.logger, color="gold")
            elif isinstance(obj, User):
                log_info("DB PREPARE", f"Usuario '{obj.username}' marcado para eliminación", 
                        colorize_full=True, logger=self.logger, color="gold")
            
            obj.delete()
            
            # Success - dorado para operación exitosa
            if isinstance(obj_to_process, Track):
                user = obj_to_process.user.username if hasattr(obj_to_process, 'user') and obj_to_process.user else "Usuario desconocido"
                artist_names = []
                if hasattr(obj_to_process, 'artist'):
                    artist_names = [artist.name for artist in obj_to_process.artist.all()]
                artist_str = ", ".join(artist_names) if artist_names else "Artista desconocido"
                album_name = obj_to_process.album.name if obj_to_process.album else "Álbum desconocido"
                log_success("DB PREPARE", f"Usuario '{user}' preparó eliminación de canción '{obj_to_process.title}' con artista(s) '{artist_str}' y álbum '{album_name}' (task_id: {task_id})", 
                        colorize_full=True, logger=self.logger, color="gold")
            elif isinstance(obj_to_process, Album):
                user = obj_to_process.user.username if hasattr(obj_to_process, 'user') and obj_to_process.user else "Usuario desconocido"
                author_name = obj_to_process.author.name if obj_to_process.author else "Autor desconocido"
                log_success("DB PREPARE", f"Usuario '{user}' preparó eliminación de álbum '{obj_to_process.name}' del autor '{author_name}' (task_id: {task_id})", 
                        colorize_full=True, logger=self.logger, color="gold")
            elif isinstance(obj_to_process, Artist):
                user = obj_to_process.user.username if hasattr(obj_to_process, 'user') and obj_to_process.user else "Usuario desconocido"
                log_success("DB PREPARE", f"Usuario '{user}' preparó eliminación de artista '{obj_to_process.name}' (task_id: {task_id})", 
                        colorize_full=True, logger=self.logger, color="gold")
            elif isinstance(obj_to_process, User):
                log_success("DB PREPARE", f"Preparó eliminación de usuario '{obj_to_process.username}' (task_id: {task_id})", 
                        colorize_full=True, logger=self.logger, color="gold")
            
            return {"success": True, "prepared": True}

        except Exception as e:
            log_error("DB PREPARE", f"Error en prepare_delete con task_id {task_id}: {e}", 
                    colorize_full=True, logger=self.logger, color="red")
            return {"success": False, "error": str(e)}

    def commit_operation(self, task_id: str, node_id: str = None):
        """
        Fase 2: Hace commit de una operación preparada.
        """
        from app.models import Artist, Track, User, Album

        log_info("DB COMMIT", f"Iniciando commit para task_id: {task_id}, node_id: {node_id if node_id else 'local'}", 
                colorize_full=True, logger=self.logger, color="gold")
        
        with self.pending_lock:
            if task_id not in self.pending_operations:
                log_error("DB COMMIT", f"Task {task_id} no encontrada en operaciones pendientes", 
                        colorize_full=True, logger=self.logger, color="red")
                return {"success": False, "error": "Task not found"}
            
            pending_op = self.pending_operations[task_id]
            
            # ✅ FIX: Inicializar TODAS las variables antes de los bloques condicionales
            operation_type = pending_op['operation']
            model_name = pending_op['model']
            username = "Usuario desconocido"
            title_str = "Sin título"
            artist_str = "Artista desconocido"
            album_name = "Álbum desconocido"
            name_str = "Sin nombre"
            username_str = "Sin username"
            author_name = "Autor desconocido"
            
            try:
                # Extraer información según el tipo de operación
                if operation_type in ['create', 'update'] and 'data' in pending_op:
                    data = pending_op['data']
                    user_id = data.get('user')
                    if user_id:
                        try:
                            user = User.objects.get(id=user_id)
                            username = user.username
                        except:
                            username = f"ID:{user_id}"
                    
                    if model_name == 'track':
                        title = data.get('title', 'Sin título')
                        title_str = title if title else "Sin título"
                        
                        # Intentar obtener artistas
                        if 'object_id' in pending_op:
                            try:
                                track = Track.objects.get(id=pending_op['object_id'])
                                artist_names = [artist.name for artist in track.artist.all()]
                                artist_str = ", ".join(artist_names) if artist_names else "Artista desconocido"
                            except:
                                pass
                        
                        # Intentar obtener álbum
                        album_id = data.get('album')
                        if album_id:
                            try:
                                album = Album.objects.get(id=album_id)
                                album_name = album.name
                            except:
                                pass
                        
                        action = "creó" if operation_type == 'create' else "actualizó"
                        log_info("DB COMMIT", f"Usuario '{username}' {action} canción '{title_str}' con artista(s) '{artist_str}' y álbum '{album_name}'", 
                                colorize_full=True, logger=self.logger, color="gold")
                    
                    elif model_name == 'album':
                        name = data.get('name', 'Sin nombre')
                        name_str = name if name else "Sin nombre"
                        
                        # Intentar obtener autor
                        author_id = data.get('author')
                        if author_id:
                            try:
                                author = Artist.objects.get(id=author_id)
                                author_name = author.name
                            except:
                                pass
                        
                        action = "creó" if operation_type == 'create' else "actualizó"
                        log_info("DB COMMIT", f"Usuario '{username}' {action} álbum '{name_str}' del autor '{author_name}'", 
                                colorize_full=True, logger=self.logger, color="gold")
                    
                    elif model_name == 'artist':
                        name = data.get('name', 'Sin nombre')
                        name_str = name if name else "Sin nombre"
                        action = "creó" if operation_type == 'create' else "actualizó"
                        log_info("DB COMMIT", f"Usuario '{username}' {action} artista '{name_str}'", 
                                colorize_full=True, logger=self.logger, color="gold")
                    
                    elif model_name == 'user':
                        username_data = data.get('username', 'Sin username')
                        username_str = username_data if username_data else "Sin username"
                        action = "creó" if operation_type == 'create' else "actualizó"
                        log_info("DB COMMIT", f"{action} usuario '{username_str}'", 
                                colorize_full=True, logger=self.logger, color="gold")
                
                elif operation_type == 'delete' and 'backup_data' in pending_op:
                    data = pending_op['backup_data']
                    user_id = data.get('user')
                    if user_id:
                        try:
                            user = User.objects.get(id=user_id)
                            username = user.username
                        except:
                            username = f"ID:{user_id}"
                    
                    if model_name == 'track':
                        title = data.get('title', 'Sin título')
                        title_str = title if title else "Sin título"
                        
                        # Intentar obtener álbum
                        album_id = data.get('album')
                        if album_id:
                            try:
                                album = Album.objects.get(id=album_id)
                                album_name = album.name
                            except:
                                pass
                        
                        log_info("DB COMMIT", f"Usuario '{username}' eliminó canción '{title_str}' del álbum '{album_name}'", 
                                colorize_full=True, logger=self.logger, color="gold")
                    
                    elif model_name == 'album':
                        name = data.get('name', 'Sin nombre')
                        name_str = name if name else "Sin nombre"
                        
                        # Intentar obtener autor
                        author_id = data.get('author')
                        if author_id:
                            try:
                                author = Artist.objects.get(id=author_id)
                                author_name = author.name
                            except:
                                pass
                        
                        log_info("DB COMMIT", f"Usuario '{username}' eliminó álbum '{name_str}' del autor '{author_name}'", 
                                colorize_full=True, logger=self.logger, color="gold")
                    
                    elif model_name == 'artist':
                        name = data.get('name', 'Sin nombre')
                        name_str = name if name else "Sin nombre"
                        log_info("DB COMMIT", f"Usuario '{username}' eliminó artista '{name_str}'", 
                                colorize_full=True, logger=self.logger, color="gold")
                    
                    elif model_name == 'user':
                        username_data = data.get('username', 'Sin username')
                        username_str = username_data if username_data else "Sin username"
                        log_info("DB COMMIT", f"Eliminó usuario '{username_str}'", 
                                colorize_full=True, logger=self.logger, color="gold")
                
                # Commit de la transacción
                if "savepoint" in pending_op:
                    transaction.savepoint_commit(pending_op["savepoint"])
                    log_info("DB COMMIT", f"Savepoint commit para task_id {task_id}", 
                            colorize_full=True, logger=self.logger, color="gold")
                    
                    connection.commit()
                    log_info("DB COMMIT", f"Transaction committed para task_id {task_id}", 
                            colorize_full=True, logger=self.logger, color="gold")
                
                # Marcar como completed en JSON
                self.json_manager.mark_completed(task_id)
                log_info("DB COMMIT", f"Task {task_id} marcada como completada en JSON", 
                        colorize_full=True, logger=self.logger, color="gold")
                
                # Actualizar versiones DB
                self.json_manager.update_db_version_on_commit()
                new_version = self.json_manager.read().get("db_version", 0)
                log_info("DB COMMIT", f"Versión DB actualizada a: {new_version}", 
                        colorize_full=True, logger=self.logger, color="gold")
                
                # Limpiar pending
                del self.pending_operations[task_id]
                
                # ✅ FIX: Usar las variables ya inicializadas (ahora TODAS existen)
                if operation_type == 'create':
                    action = "creó"
                elif operation_type == 'update':
                    action = "actualizó"
                else:  # delete
                    action = "eliminó"
                
                if model_name == 'track':
                    log_success("DB COMMIT", f"Commit exitoso: Usuario '{username}' {action} canción '{title_str}' con artista(s) '{artist_str}' y álbum '{album_name}' (task_id: {task_id})", 
                            colorize_full=True, logger=self.logger, color="gold")
                elif model_name == 'album':
                    log_success("DB COMMIT", f"Commit exitoso: Usuario '{username}' {action} álbum '{name_str}' del autor '{author_name}' (task_id: {task_id})", 
                            colorize_full=True, logger=self.logger, color="gold")
                elif model_name == 'artist':
                    log_success("DB COMMIT", f"Commit exitoso: Usuario '{username}' {action} artista '{name_str}' (task_id: {task_id})", 
                            colorize_full=True, logger=self.logger, color="gold")
                elif model_name == 'user':
                    log_success("DB COMMIT", f"Commit exitoso: Se {action} al usuario '{username_str}' (task_id: {task_id})", 
                            colorize_full=True, logger=self.logger, color="gold")
                
                return {"success": True, "committed": True}
            
            except Exception as e:
                log_error("DB COMMIT", f"Error al hacer commit a la operación con task_id {task_id}: {e}", 
                        colorize_full=True, logger=self.logger, color="red")
                return {"success": False, "error": str(e)}

    def rollback_operation(self, task_id: str):
        """
        Rollback de una operación preparada.
        """
        with self.pending_lock:
            if task_id not in self.pending_operations:
                return {"success": False, "error": "Task not found"}
            
            try:
                pending_op = self.pending_operations[task_id]
                
                # Rollback de la transacción
                if "savepoint" in pending_op:
                    transaction.savepoint_rollback(pending_op["savepoint"])
                
                # Limpiar pending
                del self.pending_operations[task_id]
                
                return {"success": True, "rolled_back": True}
            
            except Exception as e:
                return {"success": False, "error": str(e)}

    def get_pending_operations(self):
        """Retorna todas las operaciones pendientes"""
        with self.pending_lock:
            return list(self.pending_operations.keys())
        
    # JSON
    def get_json_dump(self) -> dict:
        """Exporta el JSON completo del nodo"""
        return self.json_manager.read()

    def restore_json_from_dump(self, json_data: dict):
        """Restaura el JSON desde otro nodo"""
        from raft.log_utils import log_info, log_error
        try:
            self.json_manager.copy_from_remote(json_data)
            log_info("DB MANAGER - RESTORE JSON", f"Se ha restaurado el json con éxito")
            return {"success": True}
        except Exception as e:
            log_error("DB MANAGER - RESTORE JSON", f"Ha ocurrido un error al restaurar el json: {e}")
            return {"success": False}
    
    def delete_json(self):
        self.json_manager.delete_json()
        return {"success": True}

#DB MANAGER
    def execute_pending_operations_from_json(self):
        """Ejecuta operaciones pending del JSON"""
        import logging
        import json
        from app.models import Artist, Album, Track, User
        
        logger = logging.getLogger("DBManager")
        
        pending_ops = self.json_manager.get_pending_operations()
        
        log_info("DB RECOVER", f"Iniciando recuperación de {len(pending_ops)} operaciones pendientes", 
                colorize_full=True, logger=self.logger, color="blue")
        
        for op in pending_ops:
            try:
                task_id = op.get("task_id")
                sql_operation = op.get("sql_operation", "{}")
                
                # Parsear la operación
                operation_info = json.loads(sql_operation)
                
                function_name = operation_info.get("function")
                model_name = operation_info.get("model")
                operation_type = operation_info.get("operation", "unknown")
                params = operation_info.get("params", {})
                
                # Log inicial de la operación
                log_info("DB RECOVER", f"Procesando operación {operation_type.upper()} ({function_name}) para modelo {model_name} (task_id: {task_id})", 
                        colorize_full=True, logger=self.logger, color="blue")
                
                # Reconstruir metadata
                metadata_dict = params.get("metadata_dict", {})
                
                # Usar _normalize_to_object para convertir a instancia
                metadata_obj = self._normalize_to_object(
                    data_dict=metadata_dict,
                    model_name=model_name.lower()
                )
                
                if not metadata_obj:
                    log_warning("DB RECOVER", f"No se pudo reconstruir objeto para {model_name} (task_id: {task_id})", 
                            colorize_full=True, logger=self.logger, color="yellow")
                    self.json_manager.mark_completed(task_id)
                    continue
                
                # Obtener información descriptiva del objeto
                if model_name.lower() == 'track':
                    title = metadata_dict.get('title', 'Sin título')
                    user_id = metadata_dict.get('user')
                    username = "Usuario desconocido"
                    if user_id:
                        try:
                            user = User.objects.get(id=user_id)
                            username = user.username
                        except:
                            username = f"ID:{user_id}"
                    
                    log_info("DB RECOVER", f"Recuperando {operation_type} de canción '{title}' del usuario '{username}'", 
                            colorize_full=True, logger=self.logger, color="blue")
                
                elif model_name.lower() == 'album':
                    name = metadata_dict.get('name', 'Sin nombre')
                    user_id = metadata_dict.get('user')
                    username = "Usuario desconocido"
                    if user_id:
                        try:
                            user = User.objects.get(id=user_id)
                            username = user.username
                        except:
                            username = f"ID:{user_id}"
                    
                    log_info("DB RECOVER", f"Recuperando {operation_type} de álbum '{name}' del usuario '{username}'", 
                            colorize_full=True, logger=self.logger, color="blue")
                
                elif model_name.lower() == 'artist':
                    name = metadata_dict.get('name', 'Sin nombre')
                    user_id = metadata_dict.get('user')
                    username = "Usuario desconocido"
                    if user_id:
                        try:
                            user = User.objects.get(id=user_id)
                            username = user.username
                        except:
                            username = f"ID:{user_id}"
                    
                    log_info("DB RECOVER", f"Recuperando {operation_type} de artista '{name}' del usuario '{username}'", 
                            colorize_full=True, logger=self.logger, color="blue")
                
                elif model_name.lower() == 'user':
                    username = metadata_dict.get('username', 'Sin username')
                    log_info("DB RECOVER", f"Recuperando {operation_type} de usuario '{username}'", 
                            colorize_full=True, logger=self.logger, color="blue")
                
                # Obtener la función a ejecutar
                func = getattr(self, function_name, None)
                
                if not func:
                    log_warning("DB RECOVER", f"Función no encontrada: {function_name} (task_id: {task_id})", 
                            colorize_full=True, logger=self.logger, color="yellow")
                    self.json_manager.mark_completed(task_id)
                    continue
                
                # Ejecutar la función
                try:
                    result = func(metadata_obj)
                    
                    if isinstance(result, bool):
                        success = result
                    elif isinstance(result, dict):
                        success = not result.get("error")
                    else:
                        success = True

                    if success:
                        # Log de éxito según tipo de operación y modelo
                        if operation_type == 'create':
                            action = "creó"
                        elif operation_type == 'update':
                            action = "actualizó"
                        elif operation_type == 'delete':
                            action = "eliminó"
                        else:
                            action = "procesó"
                        
                        if model_name.lower() == 'track':
                            title = metadata_dict.get('title', 'Sin título')
                            log_info("DB RECOVER", f"Operación exitosa: Se {action} canción '{title}' (task_id: {task_id})", 
                                    colorize_full=True, logger=self.logger, color="blue")
                        
                        elif model_name.lower() == 'album':
                            name = metadata_dict.get('name', 'Sin nombre')
                            log_info("DB RECOVER", f"Operación exitosa: Se {action} álbum '{name}' (task_id: {task_id})", 
                                    colorize_full=True, logger=self.logger, color="blue")
                        
                        elif model_name.lower() == 'artist':
                            name = metadata_dict.get('name', 'Sin nombre')
                            log_info("DB RECOVER", f"Operación exitosa: Se {action} artista '{name}' (task_id: {task_id})", 
                                    colorize_full=True, logger=self.logger, color="blue")
                        
                        elif model_name.lower() == 'user':
                            username = metadata_dict.get('username', 'Sin username')
                            log_info("DB RECOVER", f"Operación exitosa: Se {action} usuario '{username}' (task_id: {task_id})", 
                                    colorize_full=True, logger=self.logger, color="blue")
                    else:
                        error_msg = result.get('error', 'Ha ocurrido un error en la operación') if isinstance(result, dict) else "Operación falló"
                        log_warning("DB RECOVER", f"Operación {task_id} completada con advertencia: {error_msg}", 
                                colorize_full=True, logger=self.logger, color="yellow")
                    
                except Exception as e:
                    error_msg = str(e).lower()
                    
                    # Ignorar errores esperados
                    if any(keyword in error_msg for keyword in [
                        "already exists", "does not exist", "duplicate", 
                        "unique constraint", "not found"
                    ]):
                        log_warning("DB RECOVER", f"Error esperado en recuperación de {task_id} para {model_name}: {e}", 
                                colorize_full=True, logger=self.logger, color="yellow")
                    else:
                        log_error("DB RECOVER", f"Error inesperado en {task_id} para {model_name}: {e}", 
                                colorize_full=True, logger=self.logger, color="red")
                
                # Marcar como completed
                self.json_manager.mark_completed(task_id)
                
            except Exception as e:
                log_error("DB RECOVER", f"Error procesando operación {op.get('task_id')}: {e}", 
                        colorize_full=True, logger=self.logger, color="red")
        
        log_info("DB RECOVER", f"Recuperación completada de {len(pending_ops)} operaciones", 
                colorize_full=True, logger=self.logger, color="blue")
    
    def execute_single_operation(self, operation_dict: dict):
        """
        Ejecuta una sola operación y la marca como completed.
        Usado para sincronizar nodos.
        """
        import logging
        import json
        from app.models import Artist, Album, Track, User
        
        logger = logging.getLogger("DBManager")
        
        try:
            task_id = operation_dict.get("task_id")
            sql_operation = operation_dict.get("sql_operation", "{}")
            term = operation_dict.get("term", 0)
            
            # Parsear
            operation_info = json.loads(sql_operation)
            
            function_name = operation_info.get("function")
            model_name = operation_info.get("model")
            operation_type = operation_info.get("operation", "unknown")
            params = operation_info.get("params", {})
            
            # Log inicial
            log_info("DB SYNC", f"Sincronizando operación {operation_type.upper()} ({function_name}) para modelo {model_name} (task_id: {task_id})", 
                    colorize_full=True, logger=self.logger, color="blue")
            
            # Reconstruir metadata
            metadata_dict = params.get("metadata_dict", {})
            
            # Convertir a objeto
            metadata_obj = self._normalize_to_object(
                data_dict=metadata_dict,
                model_name=model_name.lower()
            )
            
            if not metadata_obj:
                log_error("DB SYNC", f"No se pudo reconstruir objeto para {model_name} (task_id: {task_id})", 
                        colorize_full=True, logger=self.logger, color="red")
                return {"success": False, "error": f"No se pudo reconstruir objeto para {model_name}"}
            
            # Obtener información descriptiva para el log
            if model_name.lower() == 'track':
                title = metadata_dict.get('title', 'Sin título')
                user_id = metadata_dict.get('user')
                username = "Usuario desconocido"
                if user_id:
                    try:
                        user = User.objects.get(id=user_id)
                        username = user.username
                    except:
                        username = f"ID:{user_id}"
                
                log_info("DB SYNC", f"Sincronizando {operation_type} de canción '{title}' del usuario '{username}'", 
                        colorize_full=True, logger=self.logger, color="blue")
            
            elif model_name.lower() == 'album':
                name = metadata_dict.get('name', 'Sin nombre')
                user_id = metadata_dict.get('user')
                username = "Usuario desconocido"
                if user_id:
                    try:
                        user = User.objects.get(id=user_id)
                        username = user.username
                    except:
                        username = f"ID:{user_id}"
                
                log_info("DB SYNC", f"Sincronizando {operation_type} de álbum '{name}' del usuario '{username}'", 
                        colorize_full=True, logger=self.logger, color="blue")
            
            elif model_name.lower() == 'artist':
                name = metadata_dict.get('name', 'Sin nombre')
                user_id = metadata_dict.get('user')
                username = "Usuario desconocido"
                if user_id:
                    try:
                        user = User.objects.get(id=user_id)
                        username = user.username
                    except:
                        username = f"ID:{user_id}"
                
                log_info("DB SYNC", f"Sincronizando {operation_type} de artista '{name}' del usuario '{username}'", 
                        colorize_full=True, logger=self.logger, color="blue")
            
            elif model_name.lower() == 'user':
                username = metadata_dict.get('username', 'Sin username')
                log_info("DB SYNC", f"Sincronizando {operation_type} de usuario '{username}'", 
                        colorize_full=True, logger=self.logger, color="blue")
            
            # Obtener función
            func = getattr(self, function_name, None)
            
            if not func:
                log_error("DB SYNC", f"Función no encontrada: {function_name} (task_id: {task_id})", 
                        colorize_full=True, logger=self.logger, color="red")
                return {"success": False, "error": f"Función no encontrada: {function_name}"}
            
            # Ejecutar
            try:
                result = func(metadata_obj)
                
                # Agregar al JSON como completed
                self.json_manager.add_operation(term, task_id, sql_operation)
                self.json_manager.mark_completed(task_id)
                
                # Log de éxito
                if operation_type == 'create':
                    action = "creó"
                elif operation_type == 'update':
                    action = "actualizó"
                elif operation_type == 'delete':
                    action = "eliminó"
                else:
                    action = "procesó"
                
                if model_name.lower() == 'track':
                    title = metadata_dict.get('title', 'Sin título')
                    log_info("DB SYNC", f"Sincronización exitosa: Se {action} canción '{title}' (task_id: {task_id})", 
                            colorize_full=True, logger=self.logger, color="blue")
                
                elif model_name.lower() == 'album':
                    name = metadata_dict.get('name', 'Sin nombre')
                    log_info("DB SYNC", f"Sincronización exitosa: Se {action} álbum '{name}' (task_id: {task_id})", 
                            colorize_full=True, logger=self.logger, color="blue")
                
                elif model_name.lower() == 'artist':
                    name = metadata_dict.get('name', 'Sin nombre')
                    log_info("DB SYNC", f"Sincronización exitosa: Se {action} artista '{name}' (task_id: {task_id})", 
                            colorize_full=True, logger=self.logger, color="blue")
                
                elif model_name.lower() == 'user':
                    username = metadata_dict.get('username', 'Sin username')
                    log_info("DB SYNC", f"Sincronización exitosa: Se {action} usuario '{username}' (task_id: {task_id})", 
                            colorize_full=True, logger=self.logger, color="blue")
                
                return {"success": True, "result": result}
            
            except Exception as e:
                error_msg = str(e).lower()
                
                if any(keyword in error_msg for keyword in [
                    "already exists", "does not exist", "duplicate"
                ]):
                    # Marcar como completed de todos modos
                    self.json_manager.add_operation(term, task_id, sql_operation)
                    self.json_manager.mark_completed(task_id)
                    
                    log_warning("DB SYNC", f"Error esperado en sincronización de {model_name}: {e} (task_id: {task_id})", 
                            colorize_full=True, logger=self.logger, color="yellow")
                    return {"success": True, "warning": str(e)}
                else:
                    log_error("DB SYNC", f"Error inesperado en sincronización de {model_name}: {e} (task_id: {task_id})", 
                            colorize_full=True, logger=self.logger, color="red")
                    raise
        
        except Exception as e:
            log_error("DB SYNC", f"Error ejecutando operación: {e} (task_id: {operation_dict.get('task_id')})", 
                    colorize_full=True, logger=self.logger, color="red")
            return {"success": False, "error": str(e)}
        
    def update_db_version(self, max_db_version: int):
        self.json_manager.update_db_version(max_db_version)
        log_info("DB_MANAGER", "Se actualizo la db version correctamente", logger=self.logger, color="gold")
    
    def update_term(self, term: int):        
        self.json_manager.update_term(term)
        log_info("DB_MANAGER", "Se actualizo el term correctamente", logger=self.logger, color="gold")

