import time

from django.core.management.base import BaseCommand
from django.db import connection


class Command(BaseCommand):
    """One-off backfill of StopTime.timing_point from the old timing_status
    char field, run manually against production in batches (not as part of
    a migration) to avoid one long-running transaction/lock on a huge table.
    """

    def add_arguments(self, parser):
        parser.add_argument("--batch-size", type=int, default=100_000)
        parser.add_argument("--sleep", type=float, default=0.5)

    def handle(self, batch_size, sleep, **options):
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT min(id), max(id) FROM bustimes_stoptime WHERE timing_point IS NULL"
            )
            start_id, max_id = cursor.fetchone()

        if start_id is None:
            self.stdout.write("Nothing to do")
            return

        while start_id <= max_id:
            end_id = start_id + batch_size - 1
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE bustimes_stoptime
                    SET timing_point = (timing_status != 'OTH')
                    WHERE id BETWEEN %s AND %s AND timing_point IS NULL
                    """,
                    [start_id, end_id],
                )
                self.stdout.write(f"{start_id}-{end_id}: {cursor.rowcount} rows")
            start_id = end_id + 1
            time.sleep(sleep)
