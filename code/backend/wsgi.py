"""
WSGI config for backend project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/5.2/howto/deployment/wsgi/
"""

import os

from django.core.wsgi import get_wsgi_application


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "backend.settings")

application = get_wsgi_application()

try:
    from raft import initialize_node
    import threading

    if not threading.current_thread().name.startswith("Dummy-"):
        raft, leader_manager = initialize_node()
except Exception:
    import logging, traceback
    logging.basicConfig(level=logging.INFO)
    logging.error("Error inicializando cluster Raft:")
    traceback.print_exc()
