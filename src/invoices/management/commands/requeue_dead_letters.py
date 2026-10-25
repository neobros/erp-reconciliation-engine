"""Send dead-lettered invoices back through the pipeline once the cause is fixed.

    python manage.py requeue_dead_letters --reason max_attempts --since 2h
    python manage.py requeue_dead_letters --invoice INV-000000123 --invoice INV-000000124
    python manage.py requeue_dead_letters --reason poison --dry-run
"""
import re

from django.core.management.base import BaseCommand, CommandError

from invoices.services import claims

_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_duration(text: str) -> int:
    match = re.fullmatch(r"(\d+)([smhd])", text.strip())
    if not match:
        raise CommandError(f"--since must look like 30m, 2h or 1d, not {text!r}")
    return int(match.group(1)) * _UNITS[match.group(2)]


class Command(BaseCommand):
    help = "Move DEAD_LETTERED invoices back to PENDING (attempt history is kept)."

    def add_arguments(self, parser):
        parser.add_argument("--invoice", action="append", dest="invoices",
                            help="invoice_id to requeue (repeatable)")
        parser.add_argument("--reason", help="e.g. max_attempts, poison, permanent_error")
        parser.add_argument("--since", help="only rows dead-lettered within this time, e.g. 2h")
        parser.add_argument("--dry-run", action="store_true", help="only count the rows")

    def handle(self, *args, invoices=None, reason=None, since=None, dry_run=False, **_):
        since_seconds = parse_duration(since) if since else None
        try:
            n = claims.requeue_dead_letters(invoices, reason, since_seconds, dry_run=dry_run)
        except ValueError as exc:
            raise CommandError(str(exc)) from exc
        verb = "would requeue" if dry_run else "requeued"
        self.stdout.write(f"{verb} {n} invoice(s)")
