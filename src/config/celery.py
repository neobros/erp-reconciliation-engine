import os

from celery import Celery
from celery.signals import worker_init

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

app = Celery("config")
# Reads every CELERY_* setting from config/settings.py.
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()

WORKER_METRICS_PORT = int(os.getenv("WORKER_METRICS_PORT", "9101"))


@worker_init.connect
def _start_metrics_server(**_):
    # The threads pool is a single process, so one plain registry and HTTP server is enough.
    # (worker_process_init would never fire: it is only sent to prefork child processes.)
    # Backlog gauges are exposed by the API's /metrics only, so there is one source for them.
    from prometheus_client import start_http_server

    start_http_server(WORKER_METRICS_PORT)
