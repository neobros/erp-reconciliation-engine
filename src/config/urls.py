from django.urls import path

from invoices.api import views

urlpatterns = [
    path("healthz", views.healthz),
    path("readyz", views.readyz),
    path("invoices", views.ingest),
    path("reports/summary", views.report_summary),
    path("metrics", views.metrics),
]
