"""Django settings. Every tunable comes from an env var (PLANNING.md section 3)."""
import os
from pathlib import Path

import dj_database_url

BASE_DIR = Path(__file__).resolve().parent.parent


def env_int(name: str, default: int) -> int:
    return int(os.getenv(name, default))


def env_float(name: str, default: float) -> float:
    return float(os.getenv(name, default))


SECRET_KEY = os.getenv("DJANGO_SECRET_KEY", "dev-only-not-secret")
DEBUG = os.getenv("DJANGO_DEBUG", "0") == "1"
ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",  # DRF imports it; no login is built (see DESIGN.md section 9)
    "rest_framework",
    "invoices",
]

MIDDLEWARE = [
    "django.middleware.common.CommonMiddleware",
]

ROOT_URLCONF = "config.urls"
WSGI_APPLICATION = "config.wsgi.application"

DATABASES = {
    "default": dj_database_url.parse(
        os.getenv("DATABASE_URL", "postgres://app:app@postgres:5432/app"),
        conn_max_age=60,
    )
}
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

USE_TZ = True
TIME_ZONE = "UTC"

REST_FRAMEWORK = {
    "DEFAULT_AUTHENTICATION_CLASSES": [],
    "DEFAULT_PERMISSION_CLASSES": [],
    "UNAUTHENTICATED_USER": None,
    "DEFAULT_RENDERER_CLASSES": ["rest_framework.renderers.JSONRenderer"],
}

# --- Pipeline settings (PLANNING.md section 3) ---
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
ERP_BASE_URL = os.getenv("ERP_BASE_URL", "http://mock-erp:9100")
ERP_RPS = env_int("ERP_RPS", 8)
ERP_CONNECT_TIMEOUT = env_float("ERP_CONNECT_TIMEOUT", 1.0)
ERP_READ_TIMEOUT = env_float("ERP_READ_TIMEOUT", 2.5)
MAX_ATTEMPTS = env_int("MAX_ATTEMPTS", 6)
MAX_CRASHES = env_int("MAX_CRASHES", 3)
MAX_RATE_LIMITED = env_int("MAX_RATE_LIMITED", 20)
LEASE_SECONDS = env_int("LEASE_SECONDS", 60)
QUEUE_LEASE_SECONDS = env_int("QUEUE_LEASE_SECONDS", 300)
LIMITER_MAX_WAIT = env_float("LIMITER_MAX_WAIT", 20)
DISPATCH_BATCH = env_int("DISPATCH_BATCH", 200)
QUEUE_HIGH_WATER = env_int("QUEUE_HIGH_WATER", 1000)
PENDING_HARD_LIMIT = env_int("PENDING_HARD_LIMIT", 200_000)
PO_CACHE_TTL = env_int("PO_CACHE_TTL", 900)
PO_NEGATIVE_TTL = env_int("PO_NEGATIVE_TTL", 120)
STUCK_AFTER_SECONDS = env_int("STUCK_AFTER_SECONDS", 600)
ALLOWED_CURRENCIES = frozenset(
    c.strip() for c in os.getenv("ALLOWED_CURRENCIES", "USD,EUR,GBP").split(",") if c.strip()
)

# --- Celery (PLANNING.md 3.8) ---
CELERY_BROKER_URL = REDIS_URL
CELERY_TASK_ACKS_LATE = True
CELERY_TASK_REJECT_ON_WORKER_LOST = True
# 2, not 1: with 1, a thread gets its next message only after acking the last one, and in
# Docker one worker reached ~5.9 ERP calls/s instead of 8. With 2 it reaches 8 (the limiter).
# Safe: unstarted prefetched messages of a dead worker are re-dispatched via the queue lease.
CELERY_WORKER_PREFETCH_MULTIPLIER = 2
CELERY_BROKER_TRANSPORT_OPTIONS = {"visibility_timeout": QUEUE_LEASE_SECONDS}
CELERY_TASK_IGNORE_RESULT = True
# Let structlog write JSON straight to stdout; otherwise Celery wraps every line in
# its own WARNING log record.
CELERY_WORKER_REDIRECT_STDOUTS = False
CELERY_WORKER_HIJACK_ROOT_LOGGER = False
CELERY_TASK_ROUTES = {
    "invoices.tasks.reconcile": {"queue": "reconcile"},
    # Own queue: behind up to 1,000 reconcile tasks it would wait minutes.
    "invoices.tasks.dispatch": {"queue": "dispatch"},
}
CELERY_BEAT_SCHEDULE = {
    "dispatch": {
        "task": "invoices.tasks.dispatch",
        "schedule": 2.0,
        # If workers are down, stale dispatch messages are dropped instead of piling up.
        "options": {"expires": 10},
    },
}

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {"console": {"class": "logging.StreamHandler"}},
    "root": {"handlers": ["console"], "level": "INFO"},
}
