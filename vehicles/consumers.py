from channels.generic.websocket import AsyncJsonWebsocketConsumer

from .utils import VEHICLE_WATCHERS_KEY


# firehose
class VehicleLocationConsumer(AsyncJsonWebsocketConsumer):
    @property
    def vehicle_id(self):
        return self.scope["url_route"]["kwargs"].get("vehicle_id")

    @property
    def group(self):
        if self.vehicle_id:
            return f"vehicle_location{self.vehicle_id}"
        return "vehicle_locations"

    def watchers_connection(self):
        return self.channel_layer.connection(
            self.channel_layer.consistent_hash(VEHICLE_WATCHERS_KEY)
        )

    async def connect(self):
        await self.channel_layer.group_add(self.group, self.channel_name)
        await self.accept()

        if self.vehicle_id:
            key = f"vehicle{self.vehicle_id}"
            connection = self.channel_layer.connection(
                self.channel_layer.consistent_hash(key)
            )
            item = await connection.get(key)
            if item:
                await self.send(text_data=f'{{"items": [{item.decode()}]}}')

            await self.watchers_connection().zincrby(
                VEHICLE_WATCHERS_KEY, 1, self.vehicle_id
            )

    async def disconnect(self, close_code):
        print(close_code)
        await self.channel_layer.group_discard(self.group, self.channel_name)

        if self.vehicle_id:
            await self.watchers_connection().zincrby(
                VEHICLE_WATCHERS_KEY, -1, self.vehicle_id
            )

    async def move_vehicles(self, event):
        await self.send_json({"items": event["items"]})
