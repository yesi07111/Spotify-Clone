#db_manager.py
import datetime
import json
import logging
import threading
import Pyro5.api as rpc
from typing import TYPE_CHECKING
from django.db import transaction
from django.core.exceptions import ObjectDoesNotExist

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

    # GET
    def get_data(self, query_data: dict):
        """
        query_data:
            {
                "model": "artist" | "album" | "track",
                "filters": { field: value }
            }
        """
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

    def _serialize(self, obj):
        """
        Serializa un Artist, Album o Track a un diccionario simple.
        """
        from app.models import Artist, Album, Track

        if isinstance(obj, Artist):
            return {
                "id": obj.id,
                "name": obj.name,
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
                "author": obj.author.id if obj.author else None,
            }

        if isinstance(obj, Track):
            # PRIMERO intentar obtener de _artist_ids (atributo temporal)
            artist_ids = []
            if hasattr(obj, '_artist_ids'):
                artist_ids = obj._artist_ids
            # Si no, usar la relación ManyToMany
            elif hasattr(obj, 'artist'):
                artist_ids = list(obj.artist.values_list("id", flat=True)) if obj.artist else []
            
            return {
                "id": obj.id,
                "title": obj.title,
                "album": obj.album.id if obj.album else None,
                "artist": artist_ids,
                "duration_seconds": obj.duration_seconds,
                "bitrate": obj.bitrate,
                "extension": obj.extension,
            }

        return None

    def _deserialize_to_object(self, data_dict: dict, model_name: str):
        """
        Convierte un diccionario serializado de vuelta a una instancia del modelo.
        """
        from app.models import Artist, Album, Track
        
        if model_name == "artist":
            artist = Artist()
            artist.id = data_dict.get("id")
            artist.name = data_dict.get("name")
            return artist
            
        elif model_name == "album":
            album = Album()
            album.id = data_dict.get("id")
            album.name = data_dict.get("name")
            
            # Manejar la fecha
            date_val = data_dict.get("date")
            if date_val:
                if isinstance(date_val, str):
                    album.date = datetime.date.fromisoformat(date_val)
                elif isinstance(date_val, datetime.date):
                    album.date = date_val
            
            # Manejar el autor (ForeignKey)
            author_id = data_dict.get("author")
            if author_id:
                try:
                    album.author = Artist.objects.get(id=author_id)
                except Artist.DoesNotExist:
                    album.author = None
            else:
                album.author = None
                
            return album
            
        elif model_name == "track":
            track = Track()
            track.id = data_dict.get("id")
            track.title = data_dict.get("title")
            track.duration_seconds = data_dict.get("duration_seconds")
            track.bitrate = data_dict.get("bitrate")
            track.extension = data_dict.get("extension")
            
            # Manejar album (ForeignKey)
            album_id = data_dict.get("album")
            if album_id:
                try:
                    track.album = Album.objects.get(id=album_id)
                except Album.DoesNotExist:
                    track.album = None
            else:
                track.album = None
            
            # Guardar artist_ids para usar después en create_data
            # El campo 'artist' en data_dict es una lista de IDs
            artist_ids = data_dict.get("artist", [])
            if artist_ids is None:
                artist_ids = []
            track._artist_ids = artist_ids
            
            return track
            
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
        from app.models import Artist, Album, Track

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
        from app.models import Artist, Track
        # import logging

        # Normalizar entrada a objeto
        obj_to_process = self._normalize_to_object(
            metadata_obj=metadata_obj, 
            data_dict=data, 
            model_name=model_name
        )
        
        if obj_to_process is None:
            return {"success": False, "error": "No metadata nor data provided"}

        model_cls = obj_to_process.__class__
        model_name_lower = model_cls.__name__.lower()
        
        # Extraer artist_ids si es un Track
        artist_ids = []
        if isinstance(obj_to_process, Track):
            if hasattr(obj_to_process, '_artist_ids'):
                artist_ids = obj_to_process._artist_ids
        
        # Crear operation_info para JSON (incluye artist_ids si es Track)
        serialized_data = self._serialize(obj_to_process)
        
        operation_info = {
            "operation": "create_or_update",
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

        try:
            # Extraer campos excluyendo ManyToMany
            data_dict = self._extract_fields(obj_to_process)
            
            # Si es Track, quitar 'artist' de data_dict para evitar error
            if isinstance(obj_to_process, Track) and 'artist' in data_dict:
                del data_dict['artist']
            
            # Iniciar transacción sin commit
            with transaction.atomic():
                # Crear o actualizar el objeto
                if data_dict.get("id") and model_cls.objects.filter(id=data_dict["id"]).exists():
                    obj = model_cls.objects.get(id=data_dict["id"])
                    for k, v in data_dict.items():
                        if k != "id":  # No actualizar el ID
                            setattr(obj, k, v)
                    obj.save()
                else:
                    # Asegurarse de no incluir campo artist en la creación
                    if 'artist' in data_dict:
                        del data_dict['artist']
                    obj = model_cls.objects.create(**data_dict)

                # Manejar relaciones ManyToMany para Track (después de crear el objeto)
                if isinstance(obj, Track) and artist_ids:
                    obj.artist.set(Artist.objects.filter(id__in=artist_ids))
                    obj.save()

                # Guardar en pending (antes del commit)
                with self.pending_lock:
                    self.pending_operations[task_id] = {
                        "operation": "create",
                        "model": model_name_lower,
                        "object_id": obj.id,
                        "data": self._serialize(obj),
                        "savepoint": transaction.savepoint()
                    }
                    logging.info("[DB_MANAGER] Prepare Create/Update - OK")


                return {"success": True, "prepared": True, "data": self._serialize(obj)}
        
        except Exception as e:
            logging.error(f"Error en prepare_create: {e}")
            import traceback
            return {"success": False, "error": str(e), "traceback": traceback.format_exc()}

    def prepare_delete(self, task_id: str, metadata_obj=None, data: dict = None, model_name: str = None, term: int = 0):
        """
        Fase 1: Prepara una eliminación sin hacer commit.
        """
        # from app.models import Artist, Album, Track
        import logging

        # Normalizar entrada a objeto
        obj_to_process = self._normalize_to_object(
            metadata_obj=metadata_obj,
            data_dict=data,
            model_name=model_name
        )
        
        if obj_to_process is None:
            return {"success": False, "error": "No metadata provided"}

        model_cls = obj_to_process.__class__
        serialized = self._serialize(obj_to_process)

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

        try:
            obj_id = obj_to_process.id

            if not obj_id:
                return {"success": False, "error": "ID missing in metadata"}

            # Fase 1: PREPARE
            with transaction.atomic():

                # Verificar que el objeto existe
                if not model_cls.objects.filter(id=obj_id).exists():
                    return {"success": False, "error": "Object not found"}

                obj = model_cls.objects.get(id=obj_id)

                # Crear savepoint y agregar a pending_operations
                with self.pending_lock:
                    self.pending_operations[task_id] = {
                        "operation": "delete",
                        "model": model_cls.__name__.lower(),
                        "object_id": obj.id,
                        "backup_data": self._serialize(obj),
                        "savepoint": transaction.savepoint()    
                    } 
                logging.info("[DB_MANAGER] Prepare Delete - OK")
                # Realizar la eliminación (aún no confirmada)
                obj.delete()

                return {"success": True, "prepared": True}

        except Exception as e:
            return {"success": False, "error": str(e)}

    def commit_operation(self, task_id: str, node_id: str = None):
        """
        Fase 2: Hace commit de una operación preparada.
        """
        import logging
        with self.pending_lock:
            if task_id not in self.pending_operations:
                return {"success": False, "error": "Task not found"}
            
            try:
                pending_op = self.pending_operations[task_id]
                
                # Commit de la transacción
                if "savepoint" in pending_op:
                    transaction.savepoint_commit(pending_op["savepoint"])
                
                # Marcar como completed en JSON
                self.json_manager.mark_completed(task_id)
                # Actualizar versiones DB
                self.json_manager.update_db_version_on_commit()
                
                # Limpiar pending
                del self.pending_operations[task_id]
                logging.info("[DB_Manager] Resultado de commit - OK")
                
                return {"success": True, "committed": True}
            
            except Exception as e:
                logging.error(f"Error al hacer commit a la operación con task_id {task_id}: {e}")
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
        self.json_manager.copy_from_remote(json_data)
        return {"success": True}

    def execute_pending_operations_from_json(self):
        """Ejecuta operaciones pending del JSON"""
        import logging
        import json
        # from app.models import Artist, Album, Track
        
        logger = logging.getLogger("DBManager")
        
        pending_ops = self.json_manager.get_all_operations()
        
        for op in pending_ops:
            try:
                task_id = op.get("task_id")
                sql_operation = op.get("sql_operation", "{}")
                
                # Parsear la operación
                operation_info = json.loads(sql_operation)
                
                function_name = operation_info.get("function")
                model_name = operation_info.get("model")
                params = operation_info.get("params", {})
                
                logger.info(f"Ejecutando operación recuperada: {function_name} en {model_name}")
                
                # Reconstruir metadata
                metadata_dict = params.get("metadata_dict", {})
                
                # Usar _normalize_to_object para convertir a instancia
                metadata_obj = self._normalize_to_object(
                    data_dict=metadata_dict,
                    model_name=model_name.lower()
                )
                
                if not metadata_obj:
                    logger.warning(f"No se pudo reconstruir objeto para {model_name}")
                    self.json_manager.mark_completed(task_id)
                    continue
                
                # Obtener la función a ejecutar
                func = getattr(self, function_name, None)
                
                if not func:
                    logger.warning(f"Función no encontrada: {function_name}")
                    self.json_manager.mark_completed(task_id)
                    continue
                
                # Ejecutar la función
                try:
                    result = func(metadata_obj)
                    
                    if isinstance(result, dict) and not result.get("error"):
                        logger.info(f"Operación {task_id} ejecutada exitosamente")
                    else:
                        logger.warning(f"Operación {task_id} completada con advertencia: {result}")
                    
                except Exception as e:
                    error_msg = str(e).lower()
                    
                    # Ignorar errores esperados
                    if any(keyword in error_msg for keyword in [
                        "already exists", "does not exist", "duplicate", 
                        "unique constraint", "not found"
                    ]):
                        logger.warning(f"Error esperado en recuperación de {task_id}: {e}")
                    else:
                        logger.error(f"Error inesperado en {task_id}: {e}")
                
                # Marcar como completed
                self.json_manager.mark_completed(task_id)
                
            except Exception as e:
                logger.error(f"Error procesando operación {op.get('task_id')}: {e}")
                # Marcar como completed de todos modos para no bloquearse
                self.json_manager.mark_completed(op.get('task_id'))

    def execute_single_operation(self, operation_dict: dict):
        """
        Ejecuta una sola operación y la marca como completed.
        Usado para sincronizar nodos.
        """
        import logging
        import json
        from app.models import Artist, Album, Track
        
        logger = logging.getLogger("DBManager")
        
        try:
            task_id = operation_dict.get("task_id")
            sql_operation = operation_dict.get("sql_operation", "{}")
            term = operation_dict.get("term", 0)
            
            # Parsear
            operation_info = json.loads(sql_operation)
            
            function_name = operation_info.get("function")
            model_name = operation_info.get("model")
            params = operation_info.get("params", {})
            
            # Reconstruir metadata
            metadata_dict = params.get("metadata_dict", {})
            
            # Convertir a objeto
            metadata_obj = self._normalize_to_object(
                data_dict=metadata_dict,
                model_name=model_name.lower()
            )
            
            if not metadata_obj:
                return {"success": False, "error": f"No se pudo reconstruir objeto para {model_name}"}
            
            # Obtener función
            func = getattr(self, function_name, None)
            
            if not func:
                return {"success": False, "error": f"Función no encontrada: {function_name}"}
            
            # Ejecutar
            try:
                result = func(metadata_obj)
                
                # Agregar al JSON como completed
                self.json_manager.add_operation(term, task_id, sql_operation)
                self.json_manager.mark_completed(task_id)
                
                return {"success": True, "result": result}
            
            except Exception as e:
                error_msg = str(e).lower()
                
                if any(keyword in error_msg for keyword in [
                    "already exists", "does not exist", "duplicate"
                ]):
                    # Marcar como completed de todos modos
                    self.json_manager.add_operation(term, task_id, sql_operation)
                    self.json_manager.mark_completed(task_id)
                    return {"success": True, "warning": str(e)}
                else:
                    raise
        
        except Exception as e:
            logger.error(f"Error ejecutando operación: {e}")
            return {"success": False, "error": str(e)}
        

