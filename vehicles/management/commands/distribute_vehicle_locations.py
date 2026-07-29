import asyncio
import logging

from channels.layers import get_channel_layer
from django.core.management.base import BaseCommand
from redis.exceptions import ConnectionError

from ...utils import VEHICLE_POSITIONS_CHANNEL, VEHICLE_WATCHERS_KEY

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    async def run(self):
        channel_layer = get_channel_layer()
        group_send = channel_layer.group_send
        watchers_connection = channel_layer.connection(
            channel_layer.consistent_hash(VEHICLE_WATCHERS_KEY)
        )

        while True:
            try:
                message = await channel_layer.receive(VEHICLE_POSITIONS_CHANNEL)
                items = message["items"]

                await group_send(
                    "vehicle_locations", {"type": "move_vehicles", "items": items}
                )

                ids = [str(item["id"]) for item in items]
                scores = await watchers_connection.zmscore(VEHICLE_WATCHERS_KEY, ids)

                for item, score in zip(items, scores):
                    if score and score > 0:
                        await group_send(
                            f"vehicle_location{item['id']}",
                            {"type": "move_vehicles", "items": [item]},
                        )
            except ConnectionError as e:
                logger.exception(e)

    def handle(self, *args, **options):
        asyncio.run(self.run())
