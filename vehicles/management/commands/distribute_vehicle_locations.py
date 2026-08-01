import asyncio
import logging

from channels.layers import get_channel_layer
from django.core.management.base import BaseCommand
from redis.exceptions import ConnectionError

from ...utils import (
    VEHICLE_POSITIONS_CHANNEL,
    VEHICLE_WATCHERS_KEY,
    async_redis_client,
)

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    async def run(self):
        channel_layer = get_channel_layer()
        group_send = channel_layer.group_send

        while True:
            try:
                message = await channel_layer.receive(VEHICLE_POSITIONS_CHANNEL)
                items = message["items"]

                await group_send(
                    "vehicle_locations", {"type": "move_vehicles", "items": items}
                )

                ids = [str(item["id"]) for item in items]
                scores = await async_redis_client.zmscore(VEHICLE_WATCHERS_KEY, ids)

                for item, score in zip(items, scores):
                    if score and score > 0:
                        await group_send(
                            f"vehicle_location{item['id']}",
                            {"type": "move_vehicles", "items": [item]},
                        )
            except ConnectionError:
                logger.exception("error distributing vehicle locations")

    def handle(self, *args, **options):
        asyncio.run(self.run())
