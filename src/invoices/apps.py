from django.apps import AppConfig


class InvoicesConfig(AppConfig):
    name = "invoices"
    default_auto_field = "django.db.models.BigAutoField"

    def ready(self):
        from invoices.observability import configure_logging

        configure_logging()  # api, worker and beat all log JSON
