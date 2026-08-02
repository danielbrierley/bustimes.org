import asyncio
import functools
import logging

from channels.layers import get_channel_layer
from django.core.management.base import BaseCommand
from redis.exceptions import ConnectionError

from ...time_aware_polyline import (
    decode_time_aware_polyline,
    extend_time_aware_polyline,
)
from ...utils import (
    VEHICLE_POSITIONS_CHANNEL,
    # VEHICLE_WATCHERS_KEY,
    # redis_client,
    async_redis_client,
)

logger = logging.getLogger(__name__)


class PolylineWrapper:
    def __init__(self):
        self.polyline = ""
        self.last_lat = 0
        self.last_lng = 0
        self.last_time = 0

    def extend(self, lat, lng, time):
        self.polyline = extend_time_aware_polyline(
            self.polyline,
            ((lat, lng, time),),
            (self.last_lat, self.last_lng, self.last_time),
        )
        self.last_lat = lat
        self.last_lng = lng
        self.last_time = time

    def set_polyline(self, polyline):
        if isinstance(polyline, bytes):
            polyline = polyline.decode()
        self.polyline = polyline
        if decoded := decode_time_aware_polyline(polyline):
            self.last_lat, self.last_lng, self.last_time = decoded[-1]


class Command(BaseCommand):
    async def run(self):
        # max_id = VehicleJourney.objects.order_by("-id").first()

        @functools.lru_cache(maxsize=50_000)
        def get_polyline(uuid):
            return PolylineWrapper()

        # cache = {}

        channel_layer = get_channel_layer()

        while True:
            try:
                message = await channel_layer.receive(VEHICLE_POSITIONS_CHANNEL)

                items = message["items"]
                print(items)

                polylines = {uuid: get_polyline(uuid) for (uuid, _, _, _) in items}

                pipeline = async_redis_client.pipeline()

                unknowns = [
                    (uuid, polyline)
                    for uuid, polyline in polylines.items()
                    if not polyline.polyline
                ]

                for uuid, polyline in unknowns:
                    pipeline.type(uuid)

                types = await pipeline.execute()
                print(types)

                list_uuids = []
                string_uuids = []

                list_pipe = async_redis_client.pipeline()

                for pair, type in zip(unknowns, types):
                    if type == b"list":
                        list_uuids.append(pair)
                        list_pipe.lrange(pair[0], 0, -1)
                    elif type == b"string":
                        string_uuids.append(pair[0])

                # lists = await list_pipe.execute()
                strings = await async_redis_client.mget(string_uuids)

                for uuid, string in zip(string_uuids, strings):
                    polylines[uuid].set_polyline(string)

                for uuid, time, x, y in items:
                    polylines[uuid].extend(x, y, time)

                await async_redis_client.mset(
                    {uuid: polyline.polyline for uuid, polyline in polylines.items()}
                )

            except ConnectionError:
                logger.exception("error distributing vehicle locations")

    def handle(self, *args, **options):
        asyncio.run(self.run())
