from django.apps import AppConfig
import logging
import os

logger = logging.getLogger(__name__)


class RaftConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "raft"

    def ready(self):
        """
        Esta función se ejecuta dos veces cuando usas runserver:
        - Una en el proceso del autoreloader
        - Otra en el proceso real que atiende peticiones

        Para evitar inicializar Raft dos veces, solo ejecutamos la
        inicialización cuando RUN_MAIN == "true".
        """
        # Solo correr en el proceso real
        if os.environ.get("RUN_MAIN") != "true":
            return

        import raft.global_state as gs
        from . import initialize_node
        from .utils import get_raft_instance

        # Si ya se inicializó este proceso, no repetir
        if gs.initialized:
            return

        # Si ya existe una instancia previa (p.ej. si otro módulo la creó)
        if get_raft_instance() is not None:
            gs.initialized = True
            return

        # Marcar como inicializado para este proceso
        gs.initialized = True

        try:
            logger.info("AppConfig.ready(): inicializando nodo Raft...")
            initialize_node()
        except OSError as e:
            if e.errno == 98 or "address already in use" in str(e).lower():
                logger.warning("Otro proceso ya abrió el puerto Raft.")
            else:
                logger.error(f"Error inicializando Raft en AppConfig.ready(): {e}")
        except Exception as e:
            logger.error(f"Error inicializando Raft en AppConfig.ready(): {e}")
